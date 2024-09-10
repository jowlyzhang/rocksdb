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

Status ExternalSstFileIngestionJob::Prepare(SuperVersion* super_version) {
  return ExternalSstFileJob::PrepareInternal(
      arg.external_files, arg.files_checksums,
      arg.files_checksum_func_names, arg.file_temperature, start_file_number);
}

Status ExternalSstFileIngestionJob::NeedsFlush(bool* flush_needed,
                                               SuperVersion* super_version) {
  size_t n = files_to_ingest_.size();
  autovector<UserKeyRange> ranges;
  ranges.reserve(n);
  for (const IngestedFileInfo& file_to_ingest : files_to_ingest_) {
    ranges.emplace_back(file_to_ingest.start_ukey, file_to_ingest.limit_ukey);
  }
  Status status = cfd_->RangesOverlapWithMemtables(
      ranges, super_version, db_options_.allow_data_in_errors, flush_needed);
  if (status.ok() && *flush_needed) {
    if (!ingestion_options_.allow_blocking_flush) {
      status = Status::InvalidArgument("External file requires flush");
    }
    auto ucmp = cfd_->user_comparator();
    assert(ucmp);
    if (ucmp->timestamp_size() > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make "
          "sure the key range (without timestamp) of external file does not "
          "overlap with key range in the memtables.");
    }
  }
  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ExternalSstFileIngestionJob::Run() {
  Status status;
  SuperVersion* super_version = cfd_->GetSuperVersion();
#ifndef NDEBUG
  // We should never run the job with a memtable that is overlapping
  // with the files we are ingesting
  bool need_flush = false;
  status = NeedsFlush(&need_flush, super_version);
  if (!status.ok()) {
    return status;
  }
  if (need_flush) {
    return Status::TryAgain("need_flush");
  }
  assert(status.ok() && need_flush == false);
#endif

  bool force_global_seqno = false;

  if (ingestion_options_.snapshot_consistency && !db_snapshots_->empty()) {
    // We need to assign a global sequence number to all the files even
    // if the don't overlap with any ranges since we have snapshots
    force_global_seqno = true;
  }
  // It is safe to use this instead of LastAllocatedSequence since we are
  // the only active writer, and hence they are equal
  SequenceNumber last_seqno = versions_->LastSequence();
  edit_.SetColumnFamily(cfd_->GetID());
  // The levels that the files will be ingested into

  for (IngestedFileInfo& f : files_to_ingest_) {
    SequenceNumber assigned_seqno = 0;
    if (ingestion_options_.ingest_behind) {
      status = CheckLevelForIngestedBehindFile(&f);
    } else {
      status = AssignLevelAndSeqnoForIngestedFile(
          super_version, force_global_seqno, cfd_->ioptions()->compaction_style,
          last_seqno, &f, &assigned_seqno);
    }

    // Modify the smallest/largest internal key to include the sequence number
    // that we just learned. Only overwrite sequence number zero. There could
    // be a nonzero sequence number already to indicate a range tombstone's
    // exclusive endpoint.
    ParsedInternalKey smallest_parsed, largest_parsed;
    if (status.ok()) {
      status = ParseInternalKey(*f.smallest_internal_key.rep(),
                                &smallest_parsed, false /* log_err_key */);
    }
    if (status.ok()) {
      status = ParseInternalKey(*f.largest_internal_key.rep(), &largest_parsed,
                                false /* log_err_key */);
    }
    if (!status.ok()) {
      return status;
    }
    if (smallest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(f.smallest_internal_key.rep(), assigned_seqno,
                        smallest_parsed.type);
    }
    if (largest_parsed.sequence == 0 && assigned_seqno != 0) {
      UpdateInternalKey(f.largest_internal_key.rep(), assigned_seqno,
                        largest_parsed.type);
    }

    status = AssignGlobalSeqnoForIngestedFile(&f, assigned_seqno);
    if (!status.ok()) {
      return status;
    }
    TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::Run",
                             &assigned_seqno);
    assert(assigned_seqno == 0 || assigned_seqno == last_seqno + 1);
    if (assigned_seqno > last_seqno) {
      last_seqno = assigned_seqno;
      ++consumed_seqno_count_;
    }

    status = GenerateChecksumForIngestedFile(&f);
    if (!status.ok()) {
      return status;
    }

    // We use the import time as the ancester time. This is the time the data
    // is written to the database.
    int64_t temp_current_time = 0;
    uint64_t current_time = kUnknownFileCreationTime;
    uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
    if (clock_->GetCurrentTime(&temp_current_time).ok()) {
      current_time = oldest_ancester_time =
          static_cast<uint64_t>(temp_current_time);
    }
    uint64_t tail_size = 0;
    bool contain_no_data_blocks = f.table_properties.num_entries > 0 &&
                                  (f.table_properties.num_entries ==
                                   f.table_properties.num_range_deletions);
    if (f.table_properties.tail_start_offset > 0 || contain_no_data_blocks) {
      uint64_t file_size = f.fd.GetFileSize();
      assert(f.table_properties.tail_start_offset <= file_size);
      tail_size = file_size - f.table_properties.tail_start_offset;
    }

    FileMetaData f_metadata(
        f.fd.GetNumber(), f.fd.GetPathId(), f.fd.GetFileSize(),
        f.smallest_internal_key, f.largest_internal_key, f.assigned_seqno,
        f.assigned_seqno, false, f.file_temperature, kInvalidBlobFileNumber,
        oldest_ancester_time, current_time,
        ingestion_options_.ingest_behind
            ? kReservedEpochNumberForFileIngestedBehind
            : cfd_->NewEpochNumber(),
        f.file_checksum, f.file_checksum_func_name, f.unique_id, 0, tail_size,
        f.user_defined_timestamps_persisted);
    f_metadata.temperature = f.file_temperature;
    edit_.AddFile(f.picked_level, f_metadata);
  }

  CreateEquivalentFileIngestingCompactions();
  return status;
}

void ExternalSstFileIngestionJob::CreateEquivalentFileIngestingCompactions() {
  // A map from output level to input of compactions equivalent to this
  // ingestion job.
  // TODO: simplify below logic to creating compaction per ingested file
  // instead of per output level, once we figure out how to treat ingested files
  // with adjacent range deletion tombstones to same output level in the same
  // job as non-overlapping compactions.
  std::map<int, CompactionInputFiles>
      output_level_to_file_ingesting_compaction_input;

  for (const auto& pair : edit_.GetNewFiles()) {
    int output_level = pair.first;
    const FileMetaData& f_metadata = pair.second;

    CompactionInputFiles& input =
        output_level_to_file_ingesting_compaction_input[output_level];
    if (input.files.empty()) {
      // Treat the source level of ingested files to be level 0
      input.level = 0;
    }

    compaction_input_metdatas_.push_back(new FileMetaData(f_metadata));
    input.files.push_back(compaction_input_metdatas_.back());
  }

  for (const auto& pair : output_level_to_file_ingesting_compaction_input) {
    int output_level = pair.first;
    const CompactionInputFiles& input = pair.second;

    const auto& mutable_cf_options = *(cfd_->GetLatestMutableCFOptions());
    file_ingesting_compactions_.push_back(new Compaction(
        cfd_->current()->storage_info(), *cfd_->ioptions(), mutable_cf_options,
        mutable_db_options_, {input}, output_level,
        MaxFileSizeForLevel(
            mutable_cf_options, output_level,
            cfd_->ioptions()->compaction_style) /* output file size
            limit,
                                                 * not applicable
                                                 */
        ,
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts,
        mutable_cf_options.default_write_temperature,
        0 /* max_subcompaction, not applicable */,
        {} /* grandparents, not applicable */, false /* is manual */,
        "" /* trim_ts */, -1 /* score, not applicable */,
        false /* is deletion compaction, not applicable */,
        files_overlap_ /* l0_files_might_overlap, not applicable */,
        CompactionReason::kExternalSstIngestion));
  }
}

void ExternalSstFileIngestionJob::RegisterRange() {
  for (const auto& c : file_ingesting_compactions_) {
    cfd_->compaction_picker()->RegisterCompaction(c);
  }
}

void ExternalSstFileIngestionJob::UnregisterRange() {
  for (const auto& c : file_ingesting_compactions_) {
    cfd_->compaction_picker()->UnregisterCompaction(c);
    delete c;
  }
  file_ingesting_compactions_.clear();

  for (const auto& f : compaction_input_metdatas_) {
    delete f;
  }
  compaction_input_metdatas_.clear();
}

void ExternalSstFileIngestionJob::UpdateStats() {
  // Update internal stats for new ingested files
  uint64_t total_keys = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_time = clock_->NowMicros() - job_start_time_;

  EventLoggerStream stream = event_logger_->Log();
  stream << "event"
         << "ingest_finished";
  stream << "files_ingested";
  stream.StartArray();

  for (IngestedFileInfo& f : files_to_ingest_) {
    InternalStats::CompactionStats stats(
        CompactionReason::kExternalSstIngestion, 1);
    stats.micros = total_time;
    // If actual copy occurred for this file, then we need to count the file
    // size as the actual bytes written. If the file was linked, then we ignore
    // the bytes written for file metadata.
    // TODO (yanqin) maybe account for file metadata bytes for exact accuracy?
    if (f.copy_file) {
      stats.bytes_written = f.fd.GetFileSize();
    } else {
      stats.bytes_moved = f.fd.GetFileSize();
    }
    stats.num_output_files = 1;
    cfd_->internal_stats()->AddCompactionStats(f.picked_level,
                                               Env::Priority::USER, stats);
    cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_INGESTED_ADD_FILE,
                                       f.fd.GetFileSize());
    total_keys += f.num_entries;
    if (f.picked_level == 0) {
      total_l0_files += 1;
    }
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "[AddFile] External SST file %s was ingested in L%d with path %s "
        "(global_seqno=%" PRIu64 ")\n",
        f.external_file_path.c_str(), f.picked_level,
        f.internal_file_path.c_str(), f.assigned_seqno);
    stream << "file" << f.internal_file_path << "level" << f.picked_level;
  }
  stream.EndArray();

  stream << "lsm_state";
  stream.StartArray();
  auto vstorage = cfd_->current()->storage_info();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_KEYS_TOTAL,
                                     total_keys);
  cfd_->internal_stats()->AddCFStats(InternalStats::INGESTED_NUM_FILES_TOTAL,
                                     files_to_ingest_.size());
  cfd_->internal_stats()->AddCFStats(
      InternalStats::INGESTED_LEVEL0_NUM_FILES_TOTAL, total_l0_files);
}

Status ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile(
    SuperVersion* sv, bool force_global_seqno, CompactionStyle compaction_style,
    SequenceNumber last_seqno, IngestedFileInfo* file_to_ingest,
    SequenceNumber* assigned_seqno) {
  Status status;
  *assigned_seqno = 0;
  auto ucmp = cfd_->user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  if (force_global_seqno || files_overlap_ ||
      compaction_style == kCompactionStyleFIFO) {
    *assigned_seqno = last_seqno + 1;
    // If files overlap, we have to ingest them at level 0.
    if (files_overlap_ || compaction_style == kCompactionStyleFIFO) {
      assert(ts_sz == 0);
      file_to_ingest->picked_level = 0;
      if (ingestion_options_.fail_if_not_bottommost_level &&
          cfd_->NumberLevels() > 1) {
        status = Status::TryAgain(
            "Files cannot be ingested to Lmax. Please make sure key range of "
            "Lmax does not overlap with files to ingest.");
      }
      return status;
    }
  }

  bool overlap_with_db = false;
  Arena arena;
  // TODO: plumb Env::IOActivity, Env::IOPriority
  ReadOptions ro;
  ro.total_order_seek = true;
  int target_level = 0;
  auto* vstorage = cfd_->current()->storage_info();

  for (int lvl = 0; lvl < cfd_->NumberLevels(); lvl++) {
    if (lvl > 0 && lvl < vstorage->base_level()) {
      continue;
    }
    if (cfd_->RangeOverlapWithCompaction(file_to_ingest->start_ukey,
                                         file_to_ingest->limit_ukey, lvl)) {
      // We must use L0 or any level higher than `lvl` to be able to overwrite
      // the compaction output keys that we overlap with in this level, We also
      // need to assign this file a seqno to overwrite the compaction output
      // keys in level `lvl`
      overlap_with_db = true;
      break;
    } else if (vstorage->NumLevelFiles(lvl) > 0) {
      bool overlap_with_level = false;
      status = sv->current->OverlapWithLevelIterator(
          ro, env_options_, file_to_ingest->start_ukey,
          file_to_ingest->limit_ukey, lvl, &overlap_with_level);
      if (!status.ok()) {
        return status;
      }
      if (overlap_with_level) {
        // We must use L0 or any level higher than `lvl` to be able to overwrite
        // the keys that we overlap with in this level, We also need to assign
        // this file a seqno to overwrite the existing keys in level `lvl`
        overlap_with_db = true;
        break;
      }
    } else if (compaction_style == kCompactionStyleUniversal) {
      continue;
    }

    // We don't overlap with any keys in this level, but we still need to check
    // if our file can fit in it
    if (IngestedFileFitInLevel(file_to_ingest, lvl)) {
      target_level = lvl;
    }
  }

  if (ingestion_options_.fail_if_not_bottommost_level &&
      target_level < cfd_->NumberLevels() - 1) {
    status = Status::TryAgain(
        "Files cannot be ingested to Lmax. Please make sure key range of Lmax "
        "and ongoing compaction's output to Lmax"
        "does not overlap with files to ingest.");
    return status;
  }

  TEST_SYNC_POINT_CALLBACK(
      "ExternalSstFileIngestionJob::AssignLevelAndSeqnoForIngestedFile",
      &overlap_with_db);
  file_to_ingest->picked_level = target_level;
  if (overlap_with_db) {
    if (ts_sz > 0) {
      status = Status::InvalidArgument(
          "Column family enables user-defined timestamps, please make sure the "
          "key range (without timestamp) of external file does not overlap "
          "with key range (without timestamp) in the db");
      return status;
    }
    if (*assigned_seqno == 0) {
      *assigned_seqno = last_seqno + 1;
    }
  }

  if (ingestion_options_.allow_db_generated_files && *assigned_seqno != 0) {
    return Status::InvalidArgument(
        "An ingested file is assigned to a non-zero sequence number, which is "
        "incompatible with ingestion option allow_db_generated_files.");
  }
  return status;
}

Status ExternalSstFileIngestionJob::CheckLevelForIngestedBehindFile(
    IngestedFileInfo* file_to_ingest) {
  auto* vstorage = cfd_->current()->storage_info();
  // First, check if new files fit in the last level
  int last_lvl = cfd_->NumberLevels() - 1;
  if (!IngestedFileFitInLevel(file_to_ingest, last_lvl)) {
    return Status::InvalidArgument(
        "Can't ingest_behind file as it doesn't fit "
        "at the last level!");
  }

  // Second, check if despite allow_ingest_behind=true we still have 0 seqnums
  // at some upper level
  for (int lvl = 0; lvl < cfd_->NumberLevels() - 1; lvl++) {
    for (auto file : vstorage->LevelFiles(lvl)) {
      if (file->fd.smallest_seqno == 0) {
        return Status::InvalidArgument(
            "Can't ingest_behind file as despite allow_ingest_behind=true "
            "there are files with 0 seqno in database at upper levels!");
      }
    }
  }

  file_to_ingest->picked_level = last_lvl;
  return Status::OK();
}

Status ExternalSstFileIngestionJob::AssignGlobalSeqnoForIngestedFile(
    IngestedFileInfo* file_to_ingest, SequenceNumber seqno) {
  if (file_to_ingest->original_seqno == seqno) {
    // This file already have the correct global seqno
    return Status::OK();
  } else if (!ingestion_options_.allow_global_seqno) {
    return Status::InvalidArgument("Global seqno is required, but disabled");
  } else if (ingestion_options_.write_global_seqno &&
             file_to_ingest->global_seqno_offset == 0) {
    return Status::InvalidArgument(
        "Trying to set global seqno for a file that don't have a global seqno "
        "field");
  }

  if (ingestion_options_.write_global_seqno) {
    // Determine if we can write global_seqno to a given offset of file.
    // If the file system does not support random write, then we should not.
    // Otherwise we should.
    std::unique_ptr<FSRandomRWFile> rwfile;
    Status status = fs_->NewRandomRWFile(file_to_ingest->internal_file_path,
                                         env_options_, &rwfile, nullptr);
    TEST_SYNC_POINT_CALLBACK("ExternalSstFileIngestionJob::NewRandomRWFile",
                             &status);
    if (status.ok()) {
      FSRandomRWFilePtr fsptr(std::move(rwfile), io_tracer_,
                              file_to_ingest->internal_file_path);
      std::string seqno_val;
      PutFixed64(&seqno_val, seqno);
      status = fsptr->Write(file_to_ingest->global_seqno_offset, seqno_val,
                            IOOptions(), nullptr);
      if (status.ok()) {
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::BeforeSyncGlobalSeqno");
        status = SyncIngestedFile(fsptr.get());
        TEST_SYNC_POINT("ExternalSstFileIngestionJob::AfterSyncGlobalSeqno");
        if (!status.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Failed to sync ingested file %s after writing global "
                         "sequence number: %s",
                         file_to_ingest->internal_file_path.c_str(),
                         status.ToString().c_str());
        }
      }
      if (!status.ok()) {
        return status;
      }
    } else if (!status.IsNotSupported()) {
      return status;
    }
  }

  file_to_ingest->assigned_seqno = seqno;
  return Status::OK();
}

// Cleanup after successful/failed job
void ExternalSstFileIngestionJob::Cleanup(const Status& status) {
  consumed_seqno_count_ = 0;
  ExternalSstFileJob::Cleanup(status);
}

bool ExternalSstFileIngestionJob::IngestedFileFitInLevel(
    const IngestedFileInfo* file_to_ingest, int level) {
  if (level == 0) {
    // Files can always fit in L0
    return true;
  }

  auto* vstorage = cfd_->current()->storage_info();
  Slice file_smallest_user_key(file_to_ingest->start_ukey);
  Slice file_largest_user_key(file_to_ingest->limit_ukey);

  if (vstorage->OverlapInLevel(level, &file_smallest_user_key,
                               &file_largest_user_key)) {
    // File overlap with another files in this level, we cannot
    // add it to this level
    return false;
  }

  // File did not overlap with level files, nor compaction output
  return true;
}

}  // namespace ROCKSDB_NAMESPACE