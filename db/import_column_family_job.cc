//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_builder.h"

#include "db/import_column_family_job.h"

#include <algorithm>
#include <cinttypes>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "table/merging_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status ImportColumnFamilyJob::Prepare(
    SuperVersion* sv) {
  std::vector<std::string> external_files_paths;
  // TODO(yuzhangyu): support file checksum verification.
  std::vector<std::string> files_checksums;
  std::vector<std::string> files_checksum_func_names;

  std::vector<ColumnFamilyIngestFileInfo> cf_ingest_infos;
  size_t accumulated_size = 0;
  for (size_t i = 0; i < metadatas_.size(); i++) {
    const auto metadata_per_cf = metadatas_[i];
    metadata_accumulated_sizes_[i] = accumulated_size;
    accumulated_size += metadata_per_cf.size();
    for (size_t j = 0; j < metadata_per_cf.size(); j++ ) {
      auto file_metadata = *metadata_per_cf[j];
      const auto file_path = file_metadata.db_path + "/" + file_metadata.name;
      external_files_paths.push_back(std::move(file_path));
    }
  }

  // TODO(yuzhangyu): support file temperature.
  Status status = ExternalSstFileJob::PrepareInternal(external_files_paths,
                                                      files_checksums,
                                                      files_checksum_func_names,
                                                      Temperature::kUnknown,
                                                      sv);
  if (status.ok()) {
    assert(files_to_ingest_.size() == accumulated_size);
  }

  return status;
}

// REQUIRES: we have become the only writer by entering both write_thread_ and
// nonmem_write_thread_
Status ImportColumnFamilyJob::Run() {
  // We use the import time as the ancester time. This is the time the data
  // is written to the database.
  int64_t temp_current_time = 0;
  uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;
  uint64_t current_time = kUnknownOldestAncesterTime;
  if (clock_->GetCurrentTime(&temp_current_time).ok()) {
    current_time = oldest_ancester_time =
        static_cast<uint64_t>(temp_current_time);
  }

  Status s;
  // When importing multiple CFs, we should not reuse epoch number from ingested
  // files. Since these epoch numbers were assigned by different CFs, there may
  // be different files from different CFs with the same epoch number. With a
  // subsequent intra-L0 compaction we may end up with files with overlapping
  // key range but the same epoch number. Here we will create a dummy
  // VersionStorageInfo per CF being imported. Each CF's files will be assigned
  // increasing epoch numbers to avoid duplicated epoch number. This is done by
  // only resetting epoch number of the new CF in the first call to
  // RecoverEpochNumbers() below.
  for (size_t i = 0; s.ok() && i < metadatas_.size(); ++i) {
    VersionBuilder dummy_version_builder(
        cfd_->current()->version_set()->file_options(), cfd_->ioptions(),
        cfd_->table_cache(), cfd_->current()->storage_info(),
        cfd_->current()->version_set(),
        cfd_->GetFileMetadataCacheReservationManager());
    VersionStorageInfo dummy_vstorage(
        &cfd_->internal_comparator(), cfd_->user_comparator(),
        cfd_->NumberLevels(), cfd_->ioptions()->compaction_style,
        nullptr /* src_vstorage */, cfd_->ioptions()->force_consistency_checks,
        EpochNumberRequirement::kMightMissing, cfd_->ioptions()->clock,
        cfd_->GetLatestMutableCFOptions()->bottommost_file_compaction_delay,
        cfd_->current()->version_set()->offpeak_time_option());
    size_t accumulated_size = metadata_accumulated_sizes_[i]
        for (size_t j = 0; s.ok() && j < metadatas_[i].size(); ++j) {
      const auto& f = files_to_ingest_[accumulated_size + j];
      const auto& file_metadata = *metadatas_[i][j];

      uint64_t tail_size = 0;
      bool contain_no_data_blocks = f.table_properties.num_entries > 0 &&
                                    (f.table_properties.num_entries ==
                                     f.table_properties.num_range_deletions);
      if (f.table_properties.tail_start_offset > 0 || contain_no_data_blocks) {
        uint64_t file_size = f.fd.GetFileSize();
        assert(f.table_properties.tail_start_offset <= file_size);
        tail_size = file_size - f.table_properties.tail_start_offset;
      }

      VersionEdit dummy_version_edit;
      dummy_version_edit.AddFile(
          file_metadata.level, f.fd.GetNumber(), f.fd.GetPathId(),
          f.fd.GetFileSize(), f.smallest_internal_key, f.largest_internal_key,
          file_metadata.smallest_seqno, file_metadata.largest_seqno, false,
          file_metadata.temperature, kInvalidBlobFileNumber,
          oldest_ancester_time, current_time, file_metadata.epoch_number,
          kUnknownFileChecksum, kUnknownFileChecksumFuncName, f.unique_id, 0,
          tail_size,
          static_cast<bool>(
              f.table_properties.user_defined_timestamps_persisted));
      s = dummy_version_builder.Apply(&dummy_version_edit);
    }
    if (s.ok()) {
      s = dummy_version_builder.SaveTo(&dummy_vstorage);
    }
    if (s.ok()) {
      // force resetting epoch number for each file
      dummy_vstorage.RecoverEpochNumbers(cfd_, /*restart_epoch=*/i == 0,
                                         /*force=*/true);
      edit_.SetColumnFamily(cfd_->GetID());

      for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
        for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
          edit_.AddFile(level, *file_meta);
          // If incoming sequence number is higher, update local sequence
          // number.
          if (file_meta->fd.largest_seqno > versions_->LastSequence()) {
            versions_->SetLastAllocatedSequence(file_meta->fd.largest_seqno);
            versions_->SetLastPublishedSequence(file_meta->fd.largest_seqno);
            versions_->SetLastSequence(file_meta->fd.largest_seqno);
          }
        }
      }
    }
    // Release resources occupied by the dummy VersionStorageInfo
    for (int level = 0; level < dummy_vstorage.num_levels(); level++) {
      for (FileMetaData* file_meta : dummy_vstorage.LevelFiles(level)) {
        file_meta->refs--;
        if (file_meta->refs <= 0) {
          delete file_meta;
        }
      }
    }
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE