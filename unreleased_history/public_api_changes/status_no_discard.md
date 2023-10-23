Mark the `Status`  class as nodiscard. RocksDB returns `Status` to indicate any error, so any returned `Status`
shouldn't be discarded.