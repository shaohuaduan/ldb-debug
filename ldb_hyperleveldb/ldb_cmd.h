//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

//#define DEBUG

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include "hyperleveldb/db.h"
#include "hyperleveldb/write_batch.h"
/*
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
*/
#pragma once

#include "ldb_tool.h"
using namespace leveldb;

class LDBCommand {
 public:
  // Command-line arguments
  static const std::string ARG_ENV_URI;
  static const std::string ARG_DB;
  static const std::string ARG_PATH;
  static const std::string ARG_SECONDARY_PATH;
  static const std::string ARG_HEX;
  static const std::string ARG_KEY_HEX;
  static const std::string ARG_VALUE_HEX;
  static const std::string ARG_CF_NAME;
  static const std::string ARG_TTL;
  static const std::string ARG_TTL_START;
  static const std::string ARG_TTL_END;
  static const std::string ARG_TIMESTAMP;
  static const std::string ARG_TRY_LOAD_OPTIONS;
  static const std::string ARG_IGNORE_UNKNOWN_OPTIONS;
  static const std::string ARG_FROM;
  static const std::string ARG_TO;
  static const std::string ARG_MAX_KEYS;
  static const std::string ARG_BLOOM_BITS;
  static const std::string ARG_FIX_PREFIX_LEN;
  static const std::string ARG_COMPRESSION_TYPE;
  static const std::string ARG_COMPRESSION_MAX_DICT_BYTES;
  static const std::string ARG_AUTO_COMPACTION;
  static const std::string ARG_WRITE_BUFFER_SIZE;
  static const std::string ARG_FILE_SIZE;
  static const std::string ARG_NO_VALUE;
  static const std::string ARG_DISABLE_CONSISTENCY_CHECKS;

  // -------------------------------------- DB options --------------------------------------

  //DBOPTIONS* OPTIMIZEFORSMALLDB(STD::SHARED_PTR<CACHE>* CACHE = NULLPTR);
  //DBOPTIONS* INCREASEPARALLELISM(INT TOTAL_THREADS = 16);
  static const std::string ARG_CREATE_IF_MISSING;
  static const std::string ARG_CREATE_MISSING_COLUMN_FAMILIES;
  static const std::string ARG_ERROR_IF_EXISTS;
  static const std::string ARG_PARANOID_CHECKS;
  //ENV* ENV = ENV::DEFAULT();
  //STD::SHARED_PTR<FILESYSTEM> FILE_SYSTEM = NULLPTR;
  //STD::SHARED_PTR<RATELIMITER> RATE_LIMITER = NULLPTR;
  //STD::SHARED_PTR<SSTFILEMANAGER> SST_FILE_MANAGER = NULLPTR;
  //STD::SHARED_PTR<LOGGER> INFO_LOG = NULLPTR;
  //INFOLOGLEVEL INFO_LOG_LEVEL = INFO_LEVEL;
  //INFOLOGLEVEL INFO_LOG_LEVEL = DEBUG_LEVEL;
  static const std::string ARG_MAX_OPEN_FILES;//int  = -1;
  static const std::string ARG_MAX_FILE_OPENING_THREADS;//int  = 16;
  static const std::string ARG_MAX_TOTAL_WAL_SIZE;//uint64_t  = 0;
  //STD::SHARED_PTR<STATISTICS> STATISTICS = NULLPTR;
  static const std::string ARG_USE_FSYNC;
  //STD::VECTOR<DBPATH> DB_PATHS;
  //STD::STRING DB_LOG_DIR = "";
  //STD::STRING WAL_DIR = "";
  static const std::string ARG_DELETE_OBSOLETE_FILES_PERIOD_MICROS;//uint64_t  = 6ULL * 60 * 60 * 1000000;
  static const std::string ARG_MAX_BACKGROUND_JOBS;//int  = 2;
  static const std::string ARG_BASE_BACKGROUND_COMPACTIONS;//int  = -1;
  static const std::string ARG_MAX_BACKGROUND_COMPACTIONS;//int  = -1;
  static const std::string ARG_MAX_SUBCOMPACTIONS;//uint32_t  = 1;
  static const std::string ARG_MAX_BACKGROUND_FLUSHES;//int  = -1;
  static const std::string ARG_MAX_LOG_FILE_SIZE;//size_t  = 0;
  static const std::string ARG_LOG_FILE_TIME_TO_ROLL;//size_t  = 0;
  static const std::string ARG_KEEP_LOG_FILE_NUM;//size_t  = 1000;
  static const std::string ARG_RECYCLE_LOG_FILE_NUM;//size_t  = 0;
  static const std::string ARG_MAX_MANIFEST_FILE_SIZE;//uint64_t  = 1024 * 1024 * 1024;
  static const std::string ARG_TABLE_CACHE_NUMSHARDBITS;//int  = 6;
  static const std::string ARG_WAL_TTL_SECONDS;//uint64_t  = 0;
  static const std::string ARG_WAL_SIZE_LIMIT_MB;//uint64_t  = 0;
  static const std::string ARG_MANIFEST_PREALLOCATION_SIZE;//size_t  = 4 * 1024 * 1024;
  static const std::string ARG_ALLOW_MMAP_READS;
  static const std::string ARG_ALLOW_MMAP_WRITES;
  static const std::string ARG_USE_DIRECT_READS;
  static const std::string ARG_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION;
  static const std::string ARG_ALLOW_FALLOCATE;
  static const std::string ARG_IS_FD_CLOSE_ON_EXEC;
  static const std::string ARG_SKIP_LOG_ERROR_ON_RECOVERY;
  static const std::string ARG_STATS_DUMP_PERIOD_SEC;//unsigned int  = 600;
  static const std::string ARG_STATS_PERSIST_PERIOD_SEC;//unsigned int  = 600;
  static const std::string ARG_PERSIST_STATS_TO_DISK;
  static const std::string ARG_STATS_HISTORY_BUFFER_SIZE;//size_t  = 1024 * 1024;
  static const std::string ARG_ADVISE_RANDOM_ON_OPEN;
  static const std::string ARG_DB_WRITE_BUFFER_SIZE;//size_t  = 0;
  //STD::SHARED_PTR<WRITEBUFFERMANAGER> WRITE_BUFFER_MANAGER = NULLPTR;
  //ENUM ACCESSHINT { NONE, NORMAL, SEQUENTIAL, WILLNEED };
  //ACCESSHINT ACCESS_HINT_ON_COMPACTION_START = NORMAL;
  static const std::string ARG_NEW_TABLE_READER_FOR_COMPACTION_INPUTS;
  static const std::string ARG_COMPACTION_READAHEAD_SIZE;//size_t  = 0;
  static const std::string ARG_RANDOM_ACCESS_MAX_BUFFER_SIZE;//size_t  = 1024 * 1024;
  static const std::string ARG_WRITABLE_FILE_MAX_BUFFER_SIZE;//size_t  = 1024 * 1024;
  static const std::string ARG_USE_ADAPTIVE_MUTEX;
  //DBOPTIONS();
  //EXPLICIT DBOPTIONS(CONST OPTIONS& OPTIONS);
  //VOID DUMP(LOGGER* LOG) CONST;
  static const std::string ARG_BYTES_PER_SYNC;//uint64_t  = 0;
  static const std::string ARG_WAL_BYTES_PER_SYNC;//uint64_t  = 0;
  static const std::string ARG_STRICT_BYTES_PER_SYNC;
  //STD::VECTOR<STD::SHARED_PTR<EVENTLISTENER>> LISTENERS;
  static const std::string ARG_ENABLE_THREAD_TRACKING;
  static const std::string ARG_DELAYED_WRITE_RATE;//uint64_t  = 0;
  static const std::string ARG_ENABLE_PIPELINED_WRITE;
  static const std::string ARG_UNORDERED_WRITE;
  static const std::string ARG_ALLOW_CONCURRENT_MEMTABLE_WRITE;
  static const std::string ARG_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD;
  static const std::string ARG_MAX_WRITE_BATCH_GROUP_SIZE_BYTES;//uint64_t  = 1 << 20;
  static const std::string ARG_WRITE_THREAD_MAX_YIELD_USEC;//uint64_t  = 100;
  static const std::string ARG_WRITE_THREAD_SLOW_YIELD_USEC;//uint64_t  = 3;
  static const std::string ARG_SKIP_STATS_UPDATE_ON_DB_OPEN;
  static const std::string ARG_SKIP_CHECKING_SST_FILE_SIZES_ON_DB_OPEN;
  //WALRECOVERYMODE WAL_RECOVERY_MODE = WALRECOVERYMODE::KPOINTINTIMERECOVERY;
  static const std::string ARG_ALLOW_2PC;
  //STD::SHARED_PTR<CACHE> ROW_CACHE = NULLPTR;
  //WALFILTER* WAL_FILTER = NULLPTR;
  static const std::string ARG_FAIL_IF_OPTIONS_FILE_ERROR;
  static const std::string ARG_DUMP_MALLOC_STATS;
  static const std::string ARG_AVOID_FLUSH_DURING_RECOVERY;
  static const std::string ARG_AVOID_FLUSH_DURING_SHUTDOWN;
  static const std::string ARG_ALLOW_INGEST_BEHIND;
  static const std::string ARG_PRESERVE_DELETES;
  static const std::string ARG_TWO_WRITE_QUEUES;
  static const std::string ARG_MANUAL_WAL_FLUSH;
  static const std::string ARG_ATOMIC_FLUSH;
  static const std::string ARG_AVOID_UNNECESSARY_BLOCKING_IO;
  static const std::string ARG_WRITE_DBID_TO_MANIFEST;
  static const std::string ARG_LOG_READAHEAD_SIZE;//size_t  = 0;
  //STD::SHARED_PTR<FILECHECKSUMFUNC> SST_FILE_CHECKSUM_FUNC = NULLPTR;
  
  static const std::string ARG_K_SNAPPY_COMPRESSION;  // CompressionType kSnappyCompression; leveldb/hyperleveldb ONLY
  static const std::string ARG_K_NO_COMPRESSION;  // CompressionType kSnappyCompression; leveldb/hyperleveldb ONLY
  static const std::string ARG_COMPRESSION;  // CompressionType = kSnappyCompression; leveldb/hyperleveldb ONLY
  static const std::string ARG_BLOCK_SIZE;  // leveldb/hyperleveldb ONLY
  static const std::string ARG_BLOCK_RESTART_INTERVAL;//int  = 16; leveldb/hyperleveldb ONLY
  static const std::string ARG_MAX_FILE_SIZE;//size_t  = 2 * 1024 * 1024; leveldb ONLY
  static const std::string ARG_REUSE_LOGS;//bool  = false; leveldb ONLY

  static const std::string ARG_MANUAL_GARBAGE_COLLECTION;//bool  = false; hyperleveldb ONLY

  // -------------------------------------- read options --------------------------------------

  static const std::string ARG_VERIFY_CHECKSUMS;// bool verify_checksums = false; leveldb/hyperleveldb ONLY
  static const std::string ARG_FILL_CACHE;// bool fill_cache = true; leveldb/hyperleveldb ONLY
  static const std::string ARG_SNAPSHOT;// const Snapshot* snapshot = nullptr; leveldb/hyperleveldb ONLY

  // -------------------------------------- write options --------------------------------------

  static const std::string ARG_SYNC;// bool sync = false; leveldb/hyperleveldb ONLY

  struct ParsedParams {
    std::string cmd;
    std::vector<std::string> cmd_params;
    std::map<std::string, std::string> option_map;
    std::vector<std::string> flags;
  };

  static LDBCommand* SelectCommand(const ParsedParams& parsed_parms);

  static LDBCommand* InitCommand(const std::string command, DB* db, std::vector<const Snapshot*> snapshot, std::vector<Iterator*> iterator, std::vector<ReplayIterator*> replay_iterator);

  static LDBCommand* InitCommand(
      const std::vector<std::string>& cmdSplits,
      DB* db,
      std::vector<const Snapshot*> snapshot,
      std::vector<Iterator*> iterator,
	  std::vector<ReplayIterator*> replay_iterator,
      const std::function<LDBCommand*(const ParsedParams&)>& selector);


  /* Run the command, and return the execute result. */
  void Run();

  virtual void DoCommand() = 0;

  LDBCommandExecuteResult GetExecuteState() { return exec_state_; }
  void SetDB( DB* db) { db_ = db; }
  void SetSnapshot(std::vector<const Snapshot*> snapshot){ snapshot_ = snapshot; }
  void SetIterator(std::vector<Iterator*> iterator){ iterator_ = iterator; }
  void SetReplayIterator(std::vector< ReplayIterator*> replay_iterator){ replay_iterator_ = replay_iterator; }  

 protected:
  LDBCommandExecuteResult exec_state_;
  DB* db_;
  Options options_;
  ReadOptions read_options_;
  WriteOptions write_options_;
  std::vector<const Snapshot*> snapshot_;
  std::vector<Iterator*> iterator_;
  int snapshot_index_;
  std::vector< ReplayIterator*> replay_iterator_; //hyperleveldb ONLY
  bool invalid_command_ = false;

  //std::vector<ColumnFamilyDescriptor> column_families_; // rocksdb ONLY
  //DBWithTTL* db_ttl_; // rocksdb ONLY

  LDBCommand(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  /**
   * A helper function that returns a list of command line options
   * used by this command.  It includes the common options and the ones
   * passed in.
   */
  bool ParseOpenDBOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  bool ParseReadOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  /**
   * Returns the value of the specified option as a boolean.
   * default_val is used if the option is not found in options.
   * Throws an exception if the value of the option is not
   * "true" or "false" (case insensitive).
   */
  bool ParseWriteOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  bool StringToBool(std::string val);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  static std::vector<std::string> StrSplit(const std::string& arg, char delim);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  char* EncodeVarint64(char* dst, uint64_t v);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  void PutVarint64(std::string* dst, uint64_t v);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
bool GetVarint64(Slice* input, uint64_t* value);
};


// ----------------------------------------------------------------------------

class OpenDBCommand : public LDBCommand {
 public:
  static std::string Name() { return "opendb"; }

  OpenDBCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string db_path_;
};

// ----------------------------------------------------------------------------

class PutCommand : public LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
    std::string key_;
    std::string value_;
};

// ----------------------------------------------------------------------------

class FlushWALCommand : public LDBCommand {
 public:
  static std::string Name() { return "flushwal"; }

  FlushWALCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:

};

// ----------------------------------------------------------------------------

class IteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "iterator"; }

  IteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
  std::string value_;
};

// ----------------------------------------------------------------------------

class WriteCommand : public LDBCommand {
 public:
  static std::string Name() { return "write"; }

  WriteCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
    std::vector<std::string> subcmd_;
};

// ----------------------------------------------------------------------------

class GetCommand : public LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class GetSnapshotCommand : public LDBCommand {
 public:
  static std::string Name() { return "getsnapshot"; }

  GetSnapshotCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:

};

// ----------------------------------------------------------------------------

class ReleaseSnapshotCommand : public LDBCommand {
 public:
  static std::string Name() { return "releasesnapshot"; }

  ReleaseSnapshotCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class DeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class SingleDeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "singledelete"; }

  SingleDeleteCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class DeleteDBCommand : public LDBCommand {
 public:
  static std::string Name() { return "deletedb"; }

  DeleteDBCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:

};

// ----------------------------------------------------------------------------

class GetPropertyCommand : public LDBCommand {
 public:
  static std::string Name() { return "getproperty"; }

  GetPropertyCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
    std::string property_;
};

// ----------------------------------------------------------------------------

class GetApproximateSizesCommand : public LDBCommand {
 public:
  static std::string Name() { return "getapproximatesizes"; }

  GetApproximateSizesCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
    std::vector<std::string> key_;
};

// ----------------------------------------------------------------------------

class CompactRangeCommand : public LDBCommand {
 public:
  static std::string Name() { return "compactrange"; }

  CompactRangeCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
    std::string begin_key_;
    std::string end_key_;
};

// ----------------------------------------------------------------------------

class DestroyDBCommand : public LDBCommand {
 public:
  static std::string Name() { return "destroydb"; }

  DestroyDBCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string db_path_;
};

// ----------------------------------------------------------------------------

class RepairDBCommand : public LDBCommand {
 public:
  static std::string Name() { return "repairdb"; }

  RepairDBCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string db_path_;
};

// ----------------------------------------------------------------------------
class IteratorSeekCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratorseek"; }

  IteratorSeekCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
  std::string value_;
};

// ----------------------------------------------------------------------------

class IteratorSeekToFirstCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratorseektofirst"; }

  IteratorSeekToFirstCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class IteratorSeekToLastCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratorseektolast"; }

  IteratorSeekToLastCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class IteratorNextCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratornext"; }

  IteratorNextCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class IteratorPrevCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratorprev"; }

  IteratorPrevCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class IteratorValidCommand : public LDBCommand {
 public:
  static std::string Name() { return "iteratorvalid"; }

  IteratorValidCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class NewIteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "newiterator"; }

  NewIteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:

};

// ----------------------------------------------------------------------------

class DeleteIteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "deleteiterator"; }

  DeleteIteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class LiveBackupCommand : public LDBCommand {
 public:
  static std::string Name() { return "livebackup"; }

  LiveBackupCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string db_path_;
};

// ----------------------------------------------------------------------------

class GetReplayTimestampCommand : public LDBCommand {
 public:
  static std::string Name() { return "getreplaytimestamp"; }

  GetReplayTimestampCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:

};

// ----------------------------------------------------------------------------

class AllowGarbageCollectBeforeTimestampCommand : public LDBCommand {
 public:
  static std::string Name() { return "allowgarbagecollectbeforetimestamp"; }

  AllowGarbageCollectBeforeTimestampCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string timestamp_;
};

// ----------------------------------------------------------------------------

class ValidateTimestampCommand : public LDBCommand {
 public:
  static std::string Name() { return "validatetimestamp"; }

  ValidateTimestampCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string timestamp_;
};

// ----------------------------------------------------------------------------

class CompareTimestampsCommand : public LDBCommand {
 public:
  static std::string Name() { return "comparetimestamps"; }

  CompareTimestampsCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string timestamp1_;
  std::string timestamp2_;
};

// ----------------------------------------------------------------------------

class GetReplayIteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "getreplayiterator"; }

  GetReplayIteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string timestamp_;
};

// ----------------------------------------------------------------------------

class ReleaseReplayIteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "releasereplayiterator"; }

  ReleaseReplayIteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  std::string key_;
};

// ----------------------------------------------------------------------------

class ReplayIteratorCommand : public LDBCommand {
 public:
  static std::string Name() { return "replayiterator"; }

  ReplayIteratorCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

 private:
  int replay_iterator_index_;
  std::string key_;
};








