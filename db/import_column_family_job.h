//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <unordered_set>
#include <vector>

#include "db/column_family.h"
#include "db/external_sst_file_job.h"
#include "db/snapshot_impl.h"
#include "options/db_options.h"
#include "rocksdb/db.h"
#include "rocksdb/metadata.h"
#include "rocksdb/sst_file_writer.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {
struct EnvOptions;
class SystemClock;

// Imports a set of sst files as is into a new column family. Logic is similar
// to ExternalSstFileIngestionJob.
class ImportColumnFamilyJob : public ExternalSstFileJob {
 public:
  ImportColumnFamilyJob(
      VersionSet* versions, ColumnFamilyData* cfd,
      const ImmutableDBOptions& db_options, const EnvOptions& env_options,
      const ImportColumnFamilyOptions& import_options,
      Directories* directories, EventLogger* event_logger,
      const std::shared_ptr<IOTracer>& io_tracer,
      uint64_t start_file_number,
      const std::vector<std::vector<LiveFileMetaData*>>& metadatas)
      : ExternalSstFileJob(
            versions, cfd,
            db_options,
            env_options,
            ExternalSstFilePrepareOptions(import_options),
            directories,
            event_logger,
            io_tracer,
            start_file_number),
        metadatas_(metadatas),
        metadata_accumulated_sizes_(metadatas_.size(), 0) {
  }

  Status Prepare(SuperVersion* sv) override;

  // Will execute the import job and prepare edit() to be applied.
  // REQUIRES: Mutex held
  Status Run() override;

 private:
  // TODO(yuzhangyu): better documentation
  // say you have a vector of vectors in metadatas_ like this:
  // [[*, *, *], [*, *], [*], [*, *, *, *]]
  // metadata_accumulated_sizes will be
  // [0, 3, 5, 6]
  // To enumerate the corresponding IngestedFileInfo from files_to_import_ for
  // the jth element of the ith vector from metadatas_. Need to do something
  // like this:
  // for (size_t i = 0; i < metadatas_.size(); i++) {
  //   size_t accumulated_size = metadata_accumulated_sizes_[i];
  //   for (size_t j = 0; j < metadatas_[i].size(); j++) {
  //     IngestedFileInfo file_info = files_to_import_[accumulated_size + j];
  //   }
  // }
  const std::vector<std::vector<LiveFileMetaData*>>& metadatas_;
  std::vector<size_t> metadata_accumulated_sizes_;
};

}  // namespace ROCKSDB_NAMESPACE