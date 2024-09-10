


//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/external_sst_file_ingestion_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "table/merging_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

Status ExternalSstFileJob::PrepareInternal(
    const std::vector<std::string>& external_files_paths,
    const std::vector<std::string>& files_checksums,
    const std::vector<std::string>& files_checksum_func_names,
    const Temperature& file_temperature,
    SuperVersion* sv) {
  Status status;

  // Read the information of files we are ingesting
  for (const std::string& file_path : external_files_paths) {
    IngestedFileInfo file_to_ingest;
    // For temperature, first assume it matches provided hint
    file_to_ingest.file_temperature = file_temperature;
    status =
        GetIngestedFileInfo(file_path, start_file_number_++, &file_to_ingest, sv);
    if (!status.ok()) {
      return status;
    }

    // Files generated in another DB or CF may have a different column family
    // ID, so we let it pass here.
    if (file_to_ingest.cf_id !=
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily &&
        file_to_ingest.cf_id != cfd_->GetID() &&
        !prepare_options_.allow_db_generated_files) {
      return Status::InvalidArgument(
          "External file column family id don't match");
    }

    if (file_to_ingest.num_entries == 0 &&
        file_to_ingest.num_range_deletions == 0) {
      return Status::InvalidArgument("File contain no entries");
    }

    if (!file_to_ingest.smallest_internal_key.Valid() ||
        !file_to_ingest.largest_internal_key.Valid()) {
      return Status::Corruption("Generated table have corrupted keys");
    }

    files_to_ingest_.emplace_back(std::move(file_to_ingest));
  }

  const Comparator* ucmp = cfd_->internal_comparator().user_comparator();
  auto num_files = files_to_ingest_.size();
  if (num_files == 0) {
    return Status::InvalidArgument("The list of files is empty");
  } else if (num_files > 1) {
    // Verify that passed files don't have overlapping ranges
    autovector<const IngestedFileInfo*> sorted_files;
    for (size_t i = 0; i < num_files; i++) {
      sorted_files.push_back(&files_to_ingest_[i]);
    }

    std::sort(
        sorted_files.begin(), sorted_files.end(),
        [&ucmp](const IngestedFileInfo* info1, const IngestedFileInfo* info2) {
          return sstableKeyCompare(ucmp, info1->smallest_internal_key,
                                   info2->smallest_internal_key) < 0;
        });

    for (size_t i = 0; i + 1 < num_files; i++) {
      if (sstableKeyCompare(ucmp, sorted_files[i]->largest_internal_key,
                            sorted_files[i + 1]->smallest_internal_key) >= 0) {
        files_overlap_ = true;
        break;
      }
    }
  }

  if (prepare_options_.ingest_behind && files_overlap_) {
    // TODO(yuzhangyu): this error message need to be more generic
    return Status::NotSupported(
        "Files with overlapping ranges cannot be ingested with ingestion "
        "behind mode.");
  }

  if (ucmp->timestamp_size() > 0 && files_overlap_) {
    return Status::NotSupported(
        "Files with overlapping ranges cannot be ingested to column "
        "family with user-defined timestamp enabled.");
  }

  // Copy/Move external files into DB
  std::unordered_set<size_t> ingestion_path_ids;
  for (IngestedFileInfo& f : files_to_ingest_) {
    f.copy_file = false;
    const std::string path_outside_db = f.external_file_path;
    const std::string path_inside_db = TableFileName(
        cfd_->ioptions()->cf_paths, f.fd.GetNumber(), f.fd.GetPathId());
    if (prepare_options_.move_files || prepare_options_.link_files) {
      status =
          fs_->LinkFile(path_outside_db, path_inside_db, IOOptions(), nullptr);
      if (status.ok()) {
        // It is unsafe to assume application had sync the file and file
        // directory before ingest the file. For integrity of RocksDB we need
        // to sync the file.
        std::unique_ptr<FSWritableFile> file_to_sync;
        Status s = fs_->ReopenWritableFile(path_inside_db, env_options_,
                                           &file_to_sync, nullptr);
        TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Prepare:Reopen",
                                 &s);
        // Some file systems (especially remote/distributed) don't support
        // reopening a file for writing and don't require reopening and
        // syncing the file. Ignore the NotSupported error in that case.
        if (!s.IsNotSupported()) {
          status = s;
          if (status.ok()) {
            TEST_SYNC_POINT(
                "ExternalSstFileIngestionJob::BeforeSyncIngestedFile");
            status = SyncIngestedFile(file_to_sync.get());
            TEST_SYNC_POINT(
                "ExternalSstFileIngestionJob::AfterSyncIngestedFile");
            if (!status.ok()) {
              ROCKS_LOG_WARN(db_options_.info_log,
                             "Failed to sync ingested file %s: %s",
                             path_inside_db.c_str(), status.ToString().c_str());
            }
          }
        }
      } else if (status.IsNotSupported() &&
                 prepare_options_.failed_move_fall_back_to_copy) {
        // Original file is on a different FS, use copy instead of hard linking.
        f.copy_file = true;
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Tried to link file %s but it's not supported : %s",
                       path_outside_db.c_str(), status.ToString().c_str());
      }
    } else {
      f.copy_file = true;
    }

    if (f.copy_file) {
      TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Prepare:CopyFile",
                               nullptr);
      // Always determining the destination temperature from the ingested-to
      // level would be difficult because in general we only find out the level
      // ingested to later, during Run().
      // However, we can guarantee "last level" temperature for when the user
      // requires ingestion to the last level.
      Temperature dst_temp =
          (prepare_options_.ingest_behind ||
           prepare_options_.fail_if_not_bottommost_level)
              ? sv->mutable_cf_options.last_level_temperature
              : sv->mutable_cf_options.default_write_temperature;
      // Note: CopyFile also syncs the new file.
      status = CopyFile(fs_.get(), path_outside_db, f.file_temperature,
                        path_inside_db, dst_temp, 0, db_options_.use_fsync,
                        io_tracer_);
      // The destination of the copy will be ingested
      f.file_temperature = dst_temp;
    } else {
      // Note: we currently assume that linking files does not cross
      // temperatures, so no need to change f.file_temperature
    }
    TEST_SYNC_POINT("ExternalSstFileIngestionJob::Prepare:FileAdded");
    if (!status.ok()) {
      break;
    }
    f.internal_file_path = path_inside_db;
    // Initialize the checksum information of ingested files.
    f.file_checksum = kUnknownFileChecksum;
    f.file_checksum_func_name = kUnknownFileChecksumFuncName;
    ingestion_path_ids.insert(f.fd.GetPathId());
  }

  TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncDir");
  if (status.ok()) {
    for (auto path_id : ingestion_path_ids) {
      status = directories_->GetDataDir(path_id)->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Failed to sync directory %" ROCKSDB_PRIszt
                       " while ingest file: %s",
                       path_id, status.ToString().c_str());
        break;
      }
    }
  }
  TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncDir");

  // Generate and check the sst file checksum. Note that, if
  // IngestExternalFileOptions::write_global_seqno is true, we will not update
  // the checksum information in the files_to_ingests_ here, since the file is
  // updated with the new global_seqno. After global_seqno is updated, DB will
  // generate the new checksum and store it in the Manifest. In all other cases
  // if ingestion_options_.write_global_seqno == true and
  // verify_file_checksum is false, we only check the checksum function name.
  if (status.ok() && db_options_.file_checksum_gen_factory != nullptr) {
    if (prepare_options_.verify_file_checksum == false &&
        files_checksums.size() == files_to_ingest_.size() &&
        files_checksum_func_names.size() == files_to_ingest_.size()) {
      // Only when verify_file_checksum == false and the checksum for ingested
      // files are provided, DB will use the provided checksum and does not
      // generate the checksum for ingested files.
      need_generate_file_checksum_ = false;
    } else {
      need_generate_file_checksum_ = true;
    }
    FileChecksumGenContext gen_context;
    std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
        db_options_.file_checksum_gen_factory->CreateFileChecksumGenerator(
            gen_context);
    std::vector<std::string> generated_checksums;
    std::vector<std::string> generated_checksum_func_names;
    // Step 1: generate the checksum for ingested sst file.
    if (need_generate_file_checksum_) {
      for (size_t i = 0; i < files_to_ingest_.size(); i++) {
        std::string generated_checksum;
        std::string generated_checksum_func_name;
        std::string requested_checksum_func_name;
        // TODO: rate limit file reads for checksum calculation during file
        // ingestion.
        // TODO: plumb Env::IOActivity
        ReadOptions ro;
        IOStatus io_s = GenerateOneFileChecksum(
            fs_.get(), files_to_ingest_[i].internal_file_path,
            db_options_.file_checksum_gen_factory.get(),
            requested_checksum_func_name, &generated_checksum,
            &generated_checksum_func_name,
            prepare_options_.verify_checksums_readahead_size,
            db_options_.allow_mmap_reads, io_tracer_,
            db_options_.rate_limiter.get(), ro, db_options_.stats,
            db_options_.clock);
        if (!io_s.ok()) {
          status = io_s;
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Sst file checksum generation of file: %s failed: %s",
                         files_to_ingest_[i].internal_file_path.c_str(),
                         status.ToString().c_str());
          break;
        }
        if (prepare_options_.write_global_seqno == false) {
          files_to_ingest_[i].file_checksum = generated_checksum;
          files_to_ingest_[i].file_checksum_func_name =
              generated_checksum_func_name;
        }
        generated_checksums.push_back(generated_checksum);
        generated_checksum_func_names.push_back(generated_checksum_func_name);
      }
    }

    // Step 2: based on the verify_file_checksum and ingested checksum
    // information, do the verification.
    if (status.ok()) {
      if (files_checksums.size() == files_to_ingest_.size() &&
          files_checksum_func_names.size() == files_to_ingest_.size()) {
        // Verify the checksum and checksum function name.
        if (prepare_options_.verify_file_checksum) {
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] !=
                generated_checksum_func_names[i]) {
              status = Status::InvalidArgument(
                  "Checksum function name does not match with the checksum "
                  "function name of this DB");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  external_files_paths[i].c_str(), status.ToString().c_str());
              break;
            }
            if (files_checksums[i] != generated_checksums[i]) {
              status = Status::Corruption(
                  "Ingested checksum does not match with the generated "
                  "checksum");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  files_to_ingest_[i].internal_file_path.c_str(),
                  status.ToString().c_str());
              break;
            }
          }
        } else {
          // If verify_file_checksum is not enabled, we only verify the
          // checksum function name. If it does not match, fail the ingestion.
          // If matches, we trust the ingested checksum information and store
          // in the Manifest.
          for (size_t i = 0; i < files_to_ingest_.size(); i++) {
            if (files_checksum_func_names[i] != file_checksum_gen->Name()) {
              status = Status::InvalidArgument(
                  "Checksum function name does not match with the checksum "
                  "function name of this DB");
              ROCKS_LOG_WARN(
                  db_options_.info_log,
                  "Sst file checksum verification of file: %s failed: %s",
                  external_files_paths[i].c_str(), status.ToString().c_str());
              break;
            }
            files_to_ingest_[i].file_checksum = files_checksums[i];
            files_to_ingest_[i].file_checksum_func_name =
                files_checksum_func_names[i];
          }
        }
      } else if (files_checksums.size() != files_checksum_func_names.size() ||
                 files_checksums.size() != 0) {
        // The checksum or checksum function name vector are not both empty
        // and they are incomplete.
        status = Status::InvalidArgument(
            "The checksum information of ingested sst files are nonempty and "
            "the size of checksums or the size of the checksum function "
            "names "
            "does not match with the number of ingested sst files");
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "The ingested sst files checksum information is incomplete: %s",
            status.ToString().c_str());
      }
    }
  }

  return status;
}

void ExternalSstFileJob::Cleanup(const Status& status) {
  IOOptions io_opts;
  if (!status.ok()) {
    // We failed to add the files to the database
    // remove all the files we copied
    DeleteInternalFiles();
    files_overlap_ = false;
  } else if (status.ok() && prepare_options_.move_files) {
    // The files were moved and added successfully, remove original file links
    for (IngestedFileInfo& f : files_to_ingest_) {
      Status s = fs_->DeleteFile(f.external_file_path, io_opts, nullptr);
      if (!s.ok()) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "%s was added to DB successfully but failed to remove original "
            "file link : %s",
            f.external_file_path.c_str(), s.ToString().c_str());
      }
    }
  }
}

void ExternalSstFileJob::DeleteInternalFiles() {
  IOOptions io_opts;
  for (IngestedFileInfo& f : files_to_ingest_) {
    if (f.internal_file_path.empty()) {
      continue;
    }
    Status s = fs_->DeleteFile(f.internal_file_path, io_opts, nullptr);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "AddFile() clean up for file %s failed : %s",
                     f.internal_file_path.c_str(), s.ToString().c_str());
    }
  }
}

Status ExternalSstFileJob::ResetTableReader(
    const std::string& external_file, uint64_t new_file_number,
    bool user_defined_timestamps_persisted, SuperVersion* sv,
    IngestedFileInfo* file_to_ingest,
    std::unique_ptr<TableReader>* table_reader) {
  std::unique_ptr<FSRandomAccessFile> sst_file;
  FileOptions fo{env_options_};
  fo.temperature = file_to_ingest->file_temperature;
  Status status =
      fs_->NewRandomAccessFile(external_file, fo, &sst_file, nullptr);
  if (!status.ok()) {
    return status;
  }
  Temperature updated_temp = sst_file->GetTemperature();
  if (updated_temp != Temperature::kUnknown &&
      updated_temp != file_to_ingest->file_temperature) {
    // The hint was missing or wrong. Track temperature reported by storage.
    file_to_ingest->file_temperature = updated_temp;
  }
  std::unique_ptr<RandomAccessFileReader> sst_file_reader(
      new RandomAccessFileReader(std::move(sst_file), external_file,
                                 nullptr /*Env*/, io_tracer_));
  table_reader->reset();
  status = cfd_->ioptions()->table_factory->NewTableReader(
      TableReaderOptions(
          *cfd_->ioptions(), sv->mutable_cf_options.prefix_extractor,
          env_options_, cfd_->internal_comparator(),
          sv->mutable_cf_options.block_protection_bytes_per_key,
          /*skip_filters*/ false, /*immortal*/ false,
          /*force_direct_prefetch*/ false, /*level*/ -1,
          /*block_cache_tracer*/ nullptr,
          /*max_file_size_for_l0_meta_pin*/ 0, versions_->DbSessionId(),
          /*cur_file_num*/ new_file_number,
          /* unique_id */ {}, /* largest_seqno */ 0,
          /* tail_size */ 0, user_defined_timestamps_persisted),
      std::move(sst_file_reader), file_to_ingest->file_size, table_reader);
  return status;
}

Status ExternalSstFileJob::SanityCheckTableProperties(
    const std::string& external_file, uint64_t new_file_number,
    SuperVersion* sv, IngestedFileInfo* file_to_ingest,
    std::unique_ptr<TableReader>* table_reader) {
  // Get the external file properties
  auto props = table_reader->get()->GetTableProperties();
  assert(props.get());
  const auto& uprops = props->user_collected_properties;

  // Get table version
  auto version_iter = uprops.find(ExternalSstFilePropertyNames::kVersion);
  if (version_iter == uprops.end()) {
    if (!prepare_options_.allow_db_generated_files) {
      return Status::Corruption("External file version not found");
    } else {
      // 0 is special version for when a file from live DB does not have the
      // version table property
      file_to_ingest->version = 0;
    }
  } else {
    file_to_ingest->version = DecodeFixed32(version_iter->second.c_str());
  }

  auto seqno_iter = uprops.find(ExternalSstFilePropertyNames::kGlobalSeqno);
  if (file_to_ingest->version == 2) {
    // version 2 imply that we have global sequence number
    if (seqno_iter == uprops.end()) {
      return Status::Corruption(
          "External file global sequence number not found");
    }

    // Set the global sequence number
    file_to_ingest->original_seqno = DecodeFixed64(seqno_iter->second.c_str());
    if (props->external_sst_file_global_seqno_offset == 0) {
      file_to_ingest->global_seqno_offset = 0;
      return Status::Corruption("Was not able to find file global seqno field");
    }
    file_to_ingest->global_seqno_offset =
        static_cast<size_t>(props->external_sst_file_global_seqno_offset);
  } else if (file_to_ingest->version == 1) {
    // SST file V1 should not have global seqno field
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
    // TODO(yuzhangyu): what does it mean for allow_blocking_flush to be here
    if (prepare_options_.allow_blocking_flush ||
        prepare_options_.allow_global_seqno) {
      return Status::InvalidArgument(
          "External SST file V1 does not support global seqno");
    }
  } else if (file_to_ingest->version == 0) {
    // allow_db_generated_files is true
    assert(seqno_iter == uprops.end());
    file_to_ingest->original_seqno = 0;
    file_to_ingest->global_seqno_offset = 0;
  } else {
    return Status::InvalidArgument("External file version " +
                                   std::to_string(file_to_ingest->version) +
                                   " is not supported");
  }

  file_to_ingest->cf_id = static_cast<uint32_t>(props->column_family_id);
  // This assignment works fine even though `table_reader` may later be reset,
  // since that will not affect how table properties are parsed, and this
  // assignment is making a copy.
  file_to_ingest->table_properties = *props;

  // Get number of entries in table
  file_to_ingest->num_entries = props->num_entries;
  file_to_ingest->num_range_deletions = props->num_range_deletions;

  // Validate table properties related to comparator name and user defined
  // timestamps persisted flag.
  file_to_ingest->user_defined_timestamps_persisted =
      static_cast<bool>(props->user_defined_timestamps_persisted);
  bool mark_sst_file_has_no_udt = false;
  Status s = ValidateUserDefinedTimestampsOptions(
      cfd_->user_comparator(), props->comparator_name,
      cfd_->ioptions()->persist_user_defined_timestamps,
      file_to_ingest->user_defined_timestamps_persisted,
      &mark_sst_file_has_no_udt);
  if (s.ok() && mark_sst_file_has_no_udt) {
    // A column family that enables user-defined timestamps in Memtable only
    // feature can also ingest external files created by a setting that disables
    // user-defined timestamps. In that case, we need to re-mark the
    // user_defined_timestamps_persisted flag for the file.
    file_to_ingest->user_defined_timestamps_persisted = false;
  } else if (!s.ok()) {
    return s;
  }

  // `TableReader` is initialized with `user_defined_timestamps_persisted` flag
  // to be true. If its value changed to false after this sanity check, we
  // need to reset the `TableReader`.
  auto ucmp = cfd_->user_comparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0 &&
      !file_to_ingest->user_defined_timestamps_persisted) {
    s = ResetTableReader(external_file, new_file_number,
                         file_to_ingest->user_defined_timestamps_persisted, sv,
                         file_to_ingest, table_reader);
  }
  return s;
}

Status ExternalSstFileJob::GetIngestedFileInfo(
    const std::string& external_file, uint64_t new_file_number,
    IngestedFileInfo* file_to_ingest, SuperVersion* sv) {
  file_to_ingest->external_file_path = external_file;

  // Get external file size
  Status status = fs_->GetFileSize(external_file, IOOptions(),
                                   &file_to_ingest->file_size, nullptr);
  if (!status.ok()) {
    return status;
  }

  // Assign FD with number
  file_to_ingest->fd =
      FileDescriptor(new_file_number, 0, file_to_ingest->file_size);

  // Create TableReader for external file
  std::unique_ptr<TableReader> table_reader;
  // Initially create the `TableReader` with flag
  // `user_defined_timestamps_persisted` to be true since that's the most common
  // case
  status = ResetTableReader(external_file, new_file_number,
                            /*user_defined_timestamps_persisted=*/true, sv,
                            file_to_ingest, &table_reader);
  if (!status.ok()) {
    return status;
  }

  status = SanityCheckTableProperties(external_file, new_file_number, sv,
                                      file_to_ingest, &table_reader);
  if (!status.ok()) {
    return status;
  }

  if (prepare_options_.verify_checksums_before_ingest) {
    // If customized readahead size is needed, we can pass a user option
    // all the way to here. Right now we just rely on the default readahead
    // to keep things simple.
    // TODO: plumb Env::IOActivity, Env::IOPriority
    ReadOptions ro;
    ro.readahead_size = prepare_options_.verify_checksums_readahead_size;
    status = table_reader->VerifyChecksum(
        ro, TableReaderCaller::kExternalSSTIngestion);
    if (!status.ok()) {
      return status;
    }
  }

  ParsedInternalKey key;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, sv->mutable_cf_options.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kExternalSSTIngestion));

  // Get first (smallest) and last (largest) key from file.
  file_to_ingest->smallest_internal_key =
      InternalKey("", 0, ValueType::kTypeValue);
  file_to_ingest->largest_internal_key =
      InternalKey("", 0, ValueType::kTypeValue);
  bool bounds_set = false;
  bool allow_data_in_errors = db_options_.allow_data_in_errors;
  iter->SeekToFirst();
  if (iter->Valid()) {
    Status pik_status =
        ParseInternalKey(iter->key(), &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->smallest_internal_key.SetFrom(key);

    Slice largest;
    if (strcmp(cfd_->ioptions()->table_factory->Name(), "PlainTable") == 0) {
      // PlainTable iterator does not support SeekToLast().
      largest = iter->key();
      for (; iter->Valid(); iter->Next()) {
        if (cfd_->internal_comparator().Compare(iter->key(), largest) > 0) {
          largest = iter->key();
        }
      }
      if (!iter->status().ok()) {
        return iter->status();
      }
    } else {
      iter->SeekToLast();
      if (!iter->Valid()) {
        if (iter->status().ok()) {
          // The file contains at least 1 key since iter is valid after
          // SeekToFirst().
          return Status::Corruption("Can not find largest key in sst file");
        } else {
          return iter->status();
        }
      }
      largest = iter->key();
    }

    pik_status = ParseInternalKey(largest, &key, allow_data_in_errors);
    if (!pik_status.ok()) {
      return Status::Corruption("Corrupted key in external file. ",
                                pik_status.getState());
    }
    if (key.sequence != 0) {
      return Status::Corruption("External file has non zero sequence number");
    }
    file_to_ingest->largest_internal_key.SetFrom(key);

    bounds_set = true;
  } else if (!iter->status().ok()) {
    return iter->status();
  }
  SequenceNumber largest_seqno =
      table_reader.get()->GetTableProperties()->key_largest_seqno;
  // UINT64_MAX means unknown and the file is generated before table property
  // `key_largest_seqno` is introduced.
  if (largest_seqno != UINT64_MAX && largest_seqno > 0) {
    return Status::Corruption(
        "External file has non zero largest sequence number " +
        std::to_string(largest_seqno));
  }
  if (prepare_options_.allow_db_generated_files &&
      largest_seqno == UINT64_MAX) {
    // Need to verify that all keys have seqno zero.
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Status pik_status =
          ParseInternalKey(iter->key(), &key, allow_data_in_errors);
      if (!pik_status.ok()) {
        return Status::Corruption("Corrupted key in external file. ",
                                  pik_status.getState());
      }
      if (key.sequence != 0) {
        return Status::NotSupported(
            "External file has a key with non zero sequence number.");
      }
    }
    if (!iter->status().ok()) {
      return iter->status();
    }
  }

  std::unique_ptr<InternalIterator> range_del_iter(
      table_reader->NewRangeTombstoneIterator(ro));
  // We may need to adjust these key bounds, depending on whether any range
  // deletion tombstones extend past them.
  const Comparator* ucmp = cfd_->user_comparator();
  if (range_del_iter != nullptr) {
    for (range_del_iter->SeekToFirst(); range_del_iter->Valid();
         range_del_iter->Next()) {
      Status pik_status =
          ParseInternalKey(range_del_iter->key(), &key, allow_data_in_errors);
      if (!pik_status.ok()) {
        return Status::Corruption("Corrupted key in external file. ",
                                  pik_status.getState());
      }
      if (key.sequence != 0) {
        return Status::Corruption(
            "External file has a range deletion with non zero sequence "
            "number.");
      }
      RangeTombstone tombstone(key, range_del_iter->value());

      InternalKey start_key = tombstone.SerializeKey();
      if (!bounds_set ||
          sstableKeyCompare(ucmp, start_key,
                            file_to_ingest->smallest_internal_key) < 0) {
        file_to_ingest->smallest_internal_key = start_key;
      }
      InternalKey end_key = tombstone.SerializeEndKey();
      if (!bounds_set ||
          sstableKeyCompare(ucmp, end_key,
                            file_to_ingest->largest_internal_key) > 0) {
        file_to_ingest->largest_internal_key = end_key;
      }
      bounds_set = true;
    }
  }

  const size_t ts_sz = ucmp->timestamp_size();
  Slice smallest = file_to_ingest->smallest_internal_key.user_key();
  Slice largest = file_to_ingest->largest_internal_key.user_key();
  if (ts_sz > 0) {
    AppendUserKeyWithMaxTimestamp(&file_to_ingest->start_ukey, smallest, ts_sz);
    AppendUserKeyWithMinTimestamp(&file_to_ingest->limit_ukey, largest, ts_sz);
  } else {
    file_to_ingest->start_ukey.assign(smallest.data(), smallest.size());
    file_to_ingest->limit_ukey.assign(largest.data(), largest.size());
  }

  auto s =
      GetSstInternalUniqueId(file_to_ingest->table_properties.db_id,
                             file_to_ingest->table_properties.db_session_id,
                             file_to_ingest->table_properties.orig_file_number,
                             &(file_to_ingest->unique_id));
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get SST unique id for file %s",
                   file_to_ingest->internal_file_path.c_str());
    file_to_ingest->unique_id = kNullUniqueId64x2;
  }

  return status;
}

IOStatus ExternalSstFileJob::GenerateChecksumForIngestedFile(
    IngestedFileInfo* file_to_ingest) {
  if (db_options_.file_checksum_gen_factory == nullptr ||
      need_generate_file_checksum_ == false ||
      ingestion_options_.write_global_seqno == false) {
    // If file_checksum_gen_factory is not set, we are not able to generate
    // the checksum. if write_global_seqno is false, it means we will use
    // file checksum generated during Prepare(). This step will be skipped.
    return IOStatus::OK();
  }
  std::string file_checksum;
  std::string file_checksum_func_name;
  std::string requested_checksum_func_name;
  // TODO: rate limit file reads for checksum calculation during file ingestion.
  // TODO: plumb Env::IOActivity
  ReadOptions ro;
  IOStatus io_s = GenerateOneFileChecksum(
      fs_.get(), file_to_ingest->internal_file_path,
      db_options_.file_checksum_gen_factory.get(), requested_checksum_func_name,
      &file_checksum, &file_checksum_func_name,
      ingestion_options_.verify_checksums_readahead_size,
      db_options_.allow_mmap_reads, io_tracer_, db_options_.rate_limiter.get(),
      ro, db_options_.stats, db_options_.clock);
  if (!io_s.ok()) {
    return io_s;
  }
  file_to_ingest->file_checksum = std::move(file_checksum);
  file_to_ingest->file_checksum_func_name = std::move(file_checksum_func_name);
  return IOStatus::OK();
}

template <typename TWritableFile>
Status ExternalSstFileJob::SyncIngestedFile(TWritableFile* file) {
  assert(file != nullptr);
  if (db_options_.use_fsync) {
    return file->Fsync(IOOptions(), nullptr);
  } else {
    return file->Sync(IOOptions(), nullptr);
  }
}

}  // namespace ROCKSDB_NAMESPACE