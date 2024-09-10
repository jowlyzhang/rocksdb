//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/external_sst_file_job.h"
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

class ExternalSstFileIngestionJob : public ExternalSstFileJob {
 public:
  ExternalSstFileIngestionJob(
      VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options,
      const MutableDBOptions& mutable_db_options, const EnvOptions& env_options,
      SnapshotList* db_snapshots,
      const IngestExternalFileOptions& ingestion_options,
      Directories* directories, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer, uint64_t start_file_number, const IngestExternalFileArg& ingestion_arg)
      : ExternalSstFileJob(versions, cfd, db_options, env_options, ExternalSstFilePrepareOptions(ingestion_options),
                           directories, event_logger, io_tracer, start_file_number),
        mutable_db_options_(mutable_db_options),
        db_snapshots_(db_snapshots),
        ingestion_options_(ingestion_options),
        consumed_seqno_count_(0),
        ingestion_arg_(ingestion_arg){
  }

  ~ExternalSstFileIngestionJob() { UnregisterRange(); }

  Status Prepare(SuperVersion* super_version) override;

  // Check if we need to flush the memtable before running the ingestion job
  // This will be true if the files we are ingesting are overlapping with any
  // key range in the memtable.
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status NeedsFlush(bool* flush_needed, SuperVersion* super_version);

  // Will execute the ingestion job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run() override;

  // Register key range involved in this ingestion job
  // to prevent key range conflict with other ongoing compaction/file ingestion
  // REQUIRES: Mutex held
  void RegisterRange();

  // Unregister key range registered for this ingestion job
  // REQUIRES: Mutex held
  void UnregisterRange();

  // Update column family stats.
  // REQUIRES: Mutex held
  void UpdateStats();

  // How many sequence numbers did we consume as part of the ingestion job?
  virtual int ConsumedSequenceNumbersCount() const { return consumed_seqno_count_; }

  void Cleanup(const Status& status) override;

 private:

  // Assign `file_to_ingest` the appropriate sequence number and the lowest
  // possible level that it can be ingested to according to compaction_style.
  // REQUIRES: Mutex held
  Status AssignLevelAndSeqnoForIngestedFile(SuperVersion* sv,
                                            bool force_global_seqno,
                                            CompactionStyle compaction_style,
                                            SequenceNumber last_seqno,
                                            IngestedFileInfo* file_to_ingest,
                                            SequenceNumber* assigned_seqno);

  // File that we want to ingest behind always goes to the lowest level;
  // we just check that it fits in the level, that DB allows ingest_behind,
  // and that we don't have 0 seqnums at the upper levels.
  // REQUIRES: Mutex held
  Status CheckLevelForIngestedBehindFile(IngestedFileInfo* file_to_ingest);

  // Set the file global sequence number to `seqno`
  Status AssignGlobalSeqnoForIngestedFile(IngestedFileInfo* file_to_ingest,
                                          SequenceNumber seqno);

  // Check if `file_to_ingest` can fit in level `level`
  // REQUIRES: Mutex held
  bool IngestedFileFitInLevel(const IngestedFileInfo* file_to_ingest,
                              int level);

  // Create equivalent `Compaction` objects to this file ingestion job
  // , which will be used to check range conflict with other ongoing
  // compactions.
  void CreateEquivalentFileIngestingCompactions();

  const MutableDBOptions& mutable_db_options_;
  SnapshotList* db_snapshots_;
  const IngestExternalFileOptions& ingestion_options_;
  const IngestExternalFileArg& ingestion_arg_;
  int consumed_seqno_count_;

  // Below are variables used in (un)registering range for this ingestion job
  //
  // FileMetaData used in inputs of compactions equivalent to this ingestion
  // job
  std::vector<FileMetaData*> compaction_input_metdatas_;
  // Compactions equivalent to this ingestion job
  std::vector<Compaction*> file_ingesting_compactions_;
};

}  // namespace ROCKSDB_NAMESPACE