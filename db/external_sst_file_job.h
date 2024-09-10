

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/internal_stats.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "env/file_system_tracer.h"
#include "logging/event_logger.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Directories;
class SystemClock;

struct IngestedFileInfo {
  // External file path
  std::string external_file_path;
  // Smallest internal key in external file
  InternalKey smallest_internal_key;
  // Largest internal key in external file
  InternalKey largest_internal_key;
  // NOTE: use below two fields for all `*Overlap*` types of checks instead of
  // smallest_internal_key.user_key() and largest_internal_key.user_key().
  // The smallest / largest user key contained in the file for key range checks.
  // These could be different from smallest_internal_key.user_key(), and
  // largest_internal_key.user_key() when user-defined timestamps are enabled,
  // because the check is about making sure the user key without timestamps part
  // does not overlap. To achieve that, the smallest user key will be updated
  // with the maximum timestamp while the largest user key will be updated with
  // the min timestamp. It's otherwise the same.
  std::string start_ukey;
  std::string limit_ukey;
  // Sequence number for keys in external file
  SequenceNumber original_seqno;
  // Offset of the global sequence number field in the file, will
  // be zero if version is 1 (global seqno is not supported)
  size_t global_seqno_offset;
  // External file size
  uint64_t file_size;
  // total number of keys in external file
  uint64_t num_entries;
  // total number of range deletions in external file
  uint64_t num_range_deletions;
  // Id of column family this file should be ingested into
  uint32_t cf_id;
  // TableProperties read from external file
  TableProperties table_properties;
  // Version of external file
  int version;

  // FileDescriptor for the file inside the DB
  FileDescriptor fd;
  // file path that we picked for file inside the DB
  std::string internal_file_path;
  // Global sequence number that we picked for the file inside the DB
  SequenceNumber assigned_seqno = 0;
  // Level inside the DB we picked for the external file.
  int picked_level = 0;
  // Whether to copy or link the external sst file. copy_file will be set to
  // false if ingestion_options.move_files is true and underlying FS
  // supports link operation. Need to provide a default value to make the
  // undefined-behavior sanity check of llvm happy. Since
  // ingestion_options.move_files is false by default, thus copy_file is true
  // by default.
  bool copy_file = true;
  // The checksum of ingested file
  std::string file_checksum;
  // The name of checksum function that generate the checksum
  std::string file_checksum_func_name;
  // The temperature of the file to be ingested
  Temperature file_temperature = Temperature::kUnknown;
  // Unique id of the file to be ingested
  UniqueId64x2 unique_id{};
  // Whether the external file should be treated as if it has user-defined
  // timestamps or not. If this flag is false, and the column family enables
  // UDT feature, the file will have min-timestamp artificially padded to its
  // user keys when it's read. Since it will affect how `TableReader` reads a
  // table file, it's defaulted to optimize for the majority of the case where
  // the user key's format in the external file matches the column family's
  // setting.
  bool user_defined_timestamps_persisted = true;
};

// TODO(yuzhangyu): implement this.
struct ExternalSstFilePrepareOptions {
  ExternalSstFilePrepareOptions(const IngestExternalFileOptions& ingest_options) {}

  ExternalSstFilePrepareOptions(const ImportColumnFamilyOptions& import_options) {}
};

class ExternalSstFileJob {
 public:
  ExternalSstFileJob(
      VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const EnvOptions& env_options,
      const ExternalSstFilePrepareOptions prepare_options,
      Directories* directories, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer,
      uint64_t start_file_number)
      : clock_(db_options.clock),
        job_start_time_(clock_->NowMicros()),
        fs_(db_options.fs, io_tracer),
        versions_(versions),
        cfd_(cfd),
        db_options_(db_options),
        env_options_(env_options),
        prepare_options_(prepare_options),
        directories_(directories),
        event_logger_(event_logger),
        io_tracer_(io_tracer),
        start_file_number_(start_file_number) {
    assert(directories != nullptr);
  }

  ~ExternalSstFileJob() {  }

  // Prepare the external Sst files to get ready for Run.
  virtual Status Prepare(SuperVersion* sv);

  // Execute the job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  virtual Status Run() = 0;

  // Cleanup after successful/failed job
  virtual void Cleanup(const Status& status);

  VersionEdit* edit() { return &edit_; }

  const autovector<IngestedFileInfo>& files_to_ingest() const {
    return files_to_ingest_;
  }

 private:
  Status PrepareInternal(const std::vector<std::string>& external_files_paths,
                         const std::vector<std::string>& files_checksums,
                         const std::vector<std::string>& files_checksum_func_names,
                         const Temperature& file_temperature,
                         SuperVersion* sv);

  Status ResetTableReader(const std::string& external_file,
                          uint64_t new_file_number,
                          bool user_defined_timestamps_persisted,
                          SuperVersion* sv, IngestedFileInfo* file_to_ingest,
                          std::unique_ptr<TableReader>* table_reader);

  // Read the external file's table properties to do various sanity checks and
  // populates certain fields in `IngestedFileInfo` according to some table
  // properties.
  // In some cases when sanity check passes, `table_reader` could be reset with
  // different options. For example: when external file does not contain
  // timestamps while column family enables UDT in Memtables only feature.
  Status SanityCheckTableProperties(const std::string& external_file,
                                    uint64_t new_file_number, SuperVersion* sv,
                                    IngestedFileInfo* file_to_ingest,
                                    std::unique_ptr<TableReader>* table_reader);

  // Open the external file and populate `file_to_ingest` with all the
  // external information we need to ingest this file.
  Status GetIngestedFileInfo(const std::string& external_file,
                             uint64_t new_file_number,
                             IngestedFileInfo* file_to_ingest,
                             SuperVersion* sv);

  // Generate the file checksum and store in the IngestedFileInfo
  IOStatus GenerateChecksumForIngestedFile(IngestedFileInfo* file_to_ingest);

  // Helper method to sync given file.
  template <typename TWritableFile>
  Status SyncIngestedFile(TWritableFile* file);

  // Remove all the internal files created, called when ingestion job fails.
  void DeleteInternalFiles();

  SystemClock* clock_;
  uint64_t job_start_time_;
  FileSystemPtr fs_;
  VersionSet* versions_;
  ColumnFamilyData* cfd_;
  const ImmutableDBOptions& db_options_;
  const EnvOptions& env_options_;
  const ExternalSstFilePrepareOptions prepare_options_;
  autovector<IngestedFileInfo> files_to_ingest_;
  Directories* directories_;
  EventLogger* event_logger_;
  VersionEdit edit_;
  // Set in ExternalSstFileJob::PrepareInternal(), if true all files are
  // ingested in L0
  bool files_overlap_{false};
  // Set in ExternalSstFileJob::PrepareInternal(), if true and DB
  // file_checksum_gen_factory is set, DB will generate checksum each file.
  bool need_generate_file_checksum_{true};
  std::shared_ptr<IOTracer> io_tracer_;
  uint64_t start_file_number_;
};

}  // namespace ROCKSDB_NAMESPACE
