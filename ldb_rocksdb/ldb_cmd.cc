//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "ldb_cmd.h"

#include <cinttypes>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

using namespace ROCKSDB_NAMESPACE;

const std::string LDBCommand::ARG_ENV_URI = "env_uri";
const std::string LDBCommand::ARG_DB = "db";
const std::string LDBCommand::ARG_PATH = "path";
const std::string LDBCommand::ARG_SECONDARY_PATH = "secondary_path";
const std::string LDBCommand::ARG_HEX = "hex";
const std::string LDBCommand::ARG_KEY_HEX = "key_hex";
const std::string LDBCommand::ARG_VALUE_HEX = "value_hex";
const std::string LDBCommand::ARG_CF_NAME = "column_family";
const std::string LDBCommand::ARG_TTL = "ttl";
const std::string LDBCommand::ARG_TTL_START = "start_time";
const std::string LDBCommand::ARG_TTL_END = "end_time";
const std::string LDBCommand::ARG_TIMESTAMP = "timestamp";// const Slice* timestamp = nullptr;
const std::string LDBCommand::ARG_TRY_LOAD_OPTIONS = "try_load_options";
const std::string LDBCommand::ARG_DISABLE_CONSISTENCY_CHECKS =
    "disable_consistency_checks";
const std::string LDBCommand::ARG_IGNORE_UNKNOWN_OPTIONS =
    "ignore_unknown_options";
const std::string LDBCommand::ARG_FROM = "from";
const std::string LDBCommand::ARG_TO = "to";
const std::string LDBCommand::ARG_MAX_KEYS = "max_keys";
const std::string LDBCommand::ARG_BLOOM_BITS = "bloom_bits";
const std::string LDBCommand::ARG_FIX_PREFIX_LEN = "fix_prefix_len";
const std::string LDBCommand::ARG_COMPRESSION_TYPE = "compression_type";
const std::string LDBCommand::ARG_COMPRESSION_MAX_DICT_BYTES =
    "compression_max_dict_bytes";
const std::string LDBCommand::ARG_AUTO_COMPACTION = "auto_compaction";
const std::string LDBCommand::ARG_WRITE_BUFFER_SIZE = "write_buffer_size";
const std::string LDBCommand::ARG_FILE_SIZE = "file_size";
const std::string LDBCommand::ARG_NO_VALUE = "no_value";

// -------------------------------------- open DB options --------------------------------------

//DBOptions* OptimizeForSmallDb(std::shared_ptr<Cache>* cache = nullptr);
//DBOptions* IncreaseParallelism(int total_threads = 16);
const std::string LDBCommand::ARG_CREATE_IF_MISSING = "create_if_missing";
const std::string LDBCommand::ARG_ERROR_IF_EXISTS = "error_if_exists";
const std::string LDBCommand:: ARG_CREATE_MISSING_COLUMN_FAMILIES = "create_missing_column_families";
const std::string LDBCommand:: ARG_PARANOID_CHECKS = "paranoid_checks";
//Env* env = Env::Default();
//std::shared_ptr<FileSystem> file_system = nullptr;
//std::shared_ptr<RateLimiter> rate_limiter = nullptr;
//std::shared_ptr<SstFileManager> sst_file_manager = nullptr;
//std::shared_ptr<Logger> info_log = nullptr;
//InfoLogLevel info_log_level = INFO_LEVEL;
//InfoLogLevel info_log_level = DEBUG_LEVEL;
const std::string LDBCommand:: ARG_MAX_OPEN_FILES = "max_open_files";//int  = -1;
const std::string LDBCommand:: ARG_MAX_FILE_OPENING_THREADS = "max_file_opening_threads";//int  = 16;
const std::string LDBCommand:: ARG_MAX_TOTAL_WAL_SIZE = "max_total_wal_size";//uint64_t  = 0;
//std::shared_ptr<Statistics> statistics = nullptr;
const std::string LDBCommand:: ARG_USE_FSYNC = "use_fsync";
//std::vector<DbPath> db_paths;
//std::string db_log_dir = "";
//std::string wal_dir = "";
const std::string LDBCommand:: ARG_DELETE_OBSOLETE_FILES_PERIOD_MICROS = "delete_obsolete_files_period_micros";//uint64_t  = 6ULL * 60 * 60 * 1000000;
const std::string LDBCommand:: ARG_MAX_BACKGROUND_JOBS = "max_background_jobs";//int  = 2;
const std::string LDBCommand:: ARG_BASE_BACKGROUND_COMPACTIONS = "base_background_compactions";//int  = -1;
const std::string LDBCommand:: ARG_MAX_BACKGROUND_COMPACTIONS = "max_background_compactions";//int  = -1;
const std::string LDBCommand:: ARG_MAX_SUBCOMPACTIONS = "max_subcompactions";//uint32_t  = 1;
const std::string LDBCommand:: ARG_MAX_BACKGROUND_FLUSHES = "max_background_flushes";//int  = -1;
const std::string LDBCommand:: ARG_MAX_LOG_FILE_SIZE = "max_log_file_size";//size_t  = 0;
const std::string LDBCommand:: ARG_LOG_FILE_TIME_TO_ROLL = "log_file_time_to_roll";//size_t  = 0;
const std::string LDBCommand:: ARG_KEEP_LOG_FILE_NUM = "keep_log_file_num";//size_t  = 1000;
const std::string LDBCommand:: ARG_RECYCLE_LOG_FILE_NUM = "recycle_log_file_num";//size_t  = 0;
const std::string LDBCommand:: ARG_MAX_MANIFEST_FILE_SIZE = "max_manifest_file_size";//uint64_t  = 1024 * 1024 * 1024;
const std::string LDBCommand:: ARG_TABLE_CACHE_NUMSHARDBITS = "table_cache_numshardbits";//int  = 6;
const std::string LDBCommand:: ARG_WAL_TTL_SECONDS = "WAL_ttl_seconds";//uint64_t  = 0;
const std::string LDBCommand:: ARG_WAL_SIZE_LIMIT_MB = "WAL_size_limit_MB";//uint64_t  = 0;
const std::string LDBCommand:: ARG_MANIFEST_PREALLOCATION_SIZE = "manifest_preallocation_size";//size_t  = 4 * 1024 * 1024;
const std::string LDBCommand:: ARG_ALLOW_MMAP_READS = "allow_mmap_reads";
const std::string LDBCommand:: ARG_ALLOW_MMAP_WRITES = "allow_mmap_writes";
const std::string LDBCommand:: ARG_USE_DIRECT_READS = "use_direct_reads";
const std::string LDBCommand:: ARG_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION = "use_direct_io_for_flush_and_compaction";
const std::string LDBCommand:: ARG_ALLOW_FALLOCATE = "allow_fallocate";
const std::string LDBCommand:: ARG_IS_FD_CLOSE_ON_EXEC = "is_fd_close_on_exec";
const std::string LDBCommand:: ARG_SKIP_LOG_ERROR_ON_RECOVERY = "skip_log_error_on_recovery";
const std::string LDBCommand:: ARG_STATS_DUMP_PERIOD_SEC = "stats_dump_period_sec";//unsigned int  = 600;
const std::string LDBCommand:: ARG_STATS_PERSIST_PERIOD_SEC = "stats_persist_period_sec";//unsigned int  = 600;
const std::string LDBCommand:: ARG_PERSIST_STATS_TO_DISK = "persist_stats_to_disk";
const std::string LDBCommand:: ARG_STATS_HISTORY_BUFFER_SIZE = "stats_history_buffer_size";//size_t  = 1024 * 1024;
const std::string LDBCommand:: ARG_ADVISE_RANDOM_ON_OPEN = "advise_random_on_open";
const std::string LDBCommand:: ARG_DB_WRITE_BUFFER_SIZE = "db_write_buffer_size";//size_t  = 0;
//std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;
//enum AccessHint { NONE, NORMAL, SEQUENTIAL, WILLNEED };
//AccessHint access_hint_on_compaction_start = NORMAL;
const std::string LDBCommand:: ARG_NEW_TABLE_READER_FOR_COMPACTION_INPUTS = "new_table_reader_for_compaction_inputs";
const std::string LDBCommand:: ARG_COMPACTION_READAHEAD_SIZE = "compaction_readahead_size";//size_t  = 0;
const std::string LDBCommand:: ARG_RANDOM_ACCESS_MAX_BUFFER_SIZE = "random_access_max_buffer_size";//size_t  = 1024 * 1024;
const std::string LDBCommand:: ARG_WRITABLE_FILE_MAX_BUFFER_SIZE = "writable_file_max_buffer_size";//size_t  = 1024 * 1024;
const std::string LDBCommand:: ARG_USE_ADAPTIVE_MUTEX = "use_adaptive_mutex";
//DBOptions();
//explicit DBOptions(const Options& options);
//void Dump(Logger* log) const;
const std::string LDBCommand:: ARG_BYTES_PER_SYNC = "bytes_per_sync";//uint64_t  = 0;
const std::string LDBCommand:: ARG_WAL_BYTES_PER_SYNC = "wal_bytes_per_sync";//uint64_t  = 0;
const std::string LDBCommand:: ARG_STRICT_BYTES_PER_SYNC = "strict_bytes_per_sync";
//std::vector<std::shared_ptr<EventListener>> listeners;
const std::string LDBCommand:: ARG_ENABLE_THREAD_TRACKING = "enable_thread_tracking";
const std::string LDBCommand:: ARG_DELAYED_WRITE_RATE = "delayed_write_rate";//uint64_t  = 0;
const std::string LDBCommand:: ARG_ENABLE_PIPELINED_WRITE = "enable_pipelined_write";
const std::string LDBCommand:: ARG_UNORDERED_WRITE = "unordered_write";
const std::string LDBCommand:: ARG_ALLOW_CONCURRENT_MEMTABLE_WRITE = "allow_concurrent_memtable_write";
const std::string LDBCommand:: ARG_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD = "enable_write_thread_adaptive_yield";
const std::string LDBCommand:: ARG_MAX_WRITE_BATCH_GROUP_SIZE_BYTES = "max_write_batch_group_size_bytes";//uint64_t  = 1 << 20;
const std::string LDBCommand:: ARG_WRITE_THREAD_MAX_YIELD_USEC = "write_thread_max_yield_usec";//uint64_t  = 100;
const std::string LDBCommand:: ARG_WRITE_THREAD_SLOW_YIELD_USEC = "write_thread_slow_yield_usec";//uint64_t  = 3;
const std::string LDBCommand:: ARG_SKIP_STATS_UPDATE_ON_DB_OPEN = "skip_stats_update_on_db_open";
const std::string LDBCommand:: ARG_SKIP_CHECKING_SST_FILE_SIZES_ON_DB_OPEN = "skip_checking_sst_file_sizes_on_db_open";
//WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
const std::string LDBCommand:: ARG_ALLOW_2PC = "allow_2pc";
const std::string LDBCommand:: ARG_ROW_CACHE = "row_cache";//std::shared_ptr<Cache> row_cache = nullptr;
//WalFilter* wal_filter = nullptr;
const std::string LDBCommand:: ARG_FAIL_IF_OPTIONS_FILE_ERROR = "fail_if_options_file_error";
const std::string LDBCommand:: ARG_DUMP_MALLOC_STATS = "dump_malloc_stats";
const std::string LDBCommand:: ARG_AVOID_FLUSH_DURING_RECOVERY = "avoid_flush_during_recovery";
const std::string LDBCommand:: ARG_AVOID_FLUSH_DURING_SHUTDOWN = "avoid_flush_during_shutdown";
const std::string LDBCommand:: ARG_ALLOW_INGEST_BEHIND = "allow_ingest_behind";
const std::string LDBCommand:: ARG_PRESERVE_DELETES = "preserve_deletes";
const std::string LDBCommand:: ARG_TWO_WRITE_QUEUES = "two_write_queues";
const std::string LDBCommand:: ARG_MANUAL_WAL_FLUSH = "manual_wal_flush";
const std::string LDBCommand:: ARG_ATOMIC_FLUSH = "atomic_flush";
const std::string LDBCommand:: ARG_AVOID_UNNECESSARY_BLOCKING_IO = "avoid_unnecessary_blocking_io";
const std::string LDBCommand:: ARG_WRITE_DBID_TO_MANIFEST = "write_dbid_to_manifest";
const std::string LDBCommand:: ARG_LOG_READAHEAD_SIZE = "log_readahead_size";//size_t  = 0;
//std::shared_ptr<FileChecksumFunc> sst_file_checksum_func = nullptr;

const std::string LDBCommand:: ARG_K_SNAPPY_COMPRESSION = "kSnappyCompression"; // CompressionType kSnappyCompression; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_K_NO_COMPRESSION = "kNoCompression"; // CompressionType kNoCompression; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_COMPRESSION = "compression"; // CompressionType = kSnappyCompression; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_BLOCK_SIZE = "block_size"; // leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_BLOCK_RESTART_INTERVAL = "block_restart_interval";// int  = 16; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_MAX_FILE_SIZE = "max_file_size";// size_t  = 2 * 1024 * 1024; leveldb ONLY
const std::string LDBCommand:: ARG_REUSE_LOGS = "reuse_logs";//bool  = false; leveldb ONLY

const std::string LDBCommand:: ARG_MANUAL_GARBAGE_COLLECTION = "manual_garbage_collection";// bool  = false; hyperleveldb ONLY

// -------------------------------------- read options --------------------------------------

const std::string LDBCommand:: ARG_SNAPSHOT = "snapshot";// const Snapshot* snapshot = nullptr; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_ITERATE_LOWER_BOUND = "iterate_lower_bound";// const Slice* iterate_lower_bound = nullptr;
const std::string LDBCommand:: ARG_ITERATE_UPPER_BOUND = "iterate_upper_bound";// const Slice* iterate_upper_bound = nullptr;
  // size_t readahead_size = 0;
  // uint64_t max_skippable_internal_keys = 0;
  // ReadTier read_tier = kReadAllTier;
const std::string LDBCommand:: ARG_VERIFY_CHECKSUMS = "verify_checksums";// bool verify_checksums = false; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_FILL_CACHE = "fill_cache";// bool fill_cache = true; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_TAILING = "tailing";// bool tailing = false;
  // bool managed = removed;
const std::string LDBCommand:: ARG_TOTAL_ORDER_SEEK = "total_order_seek";// bool total_order_seek = true;
const std::string LDBCommand:: ARG_AUTO_PREFIX_MODE = "auto_prefix_mode";// bool auto_prefix_mode = unknown;
const std::string LDBCommand:: ARG_PREFIX_SAME_AS_START = "prefix_same_as_start";// bool prefix_same_as_start = false;
const std::string LDBCommand:: ARG_PIN_DATA = "pin_data";// bool pin_data = false;
const std::string LDBCommand:: ARG_BACKGROUND_PURGE_ON_ITERATOR_CLEANUP = "background_purge_on_iterator_cleanup";// bool background_purge_on_iterator_cleanup = false;
const std::string LDBCommand:: ARG_IGNORE_RANGE_DELETIONS = "ignore_range_deletions";// bool ignore_range_deletions = false;
  // std::function<bool(const TableProperties&)> table_filter;// Default: empty (every table will be scanned)
const std::string LDBCommand:: ARG_ITER_START_SEQNUM = "iter_start_seqnum";// SequenceNumber iter_start_seqnum = 0;
  // const Slice* timestamp = nullptr;

// -------------------------------------- write options --------------------------------------

const std::string LDBCommand:: ARG_SYNC = "sync";// bool sync = false; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_DISABLEWAL = "disableWAL";  // bool disableWAL = false;
const std::string LDBCommand:: ARG_IGNORE_MISSING_COLUMN_FAMILIES = "ignore_missing_column_families";  // bool ignore_missing_column_families = false;
const std::string LDBCommand:: ARG_NO_SLOWDOWN = "no_slowdown";  // bool no_slowdown = false;
const std::string LDBCommand:: ARG_LOW_PRI = "low_pri";  // bool low_pri = false;
const std::string LDBCommand:: ARG_MEMTABLE_INSERT_HINT_PER_BATCH = "memtable_insert_hint_per_batch";  // bool memtable_insert_hint_per_batch = false;
  // const Slice* timestamp = nullptr;

// -------------------------------------- Tools --------------------------------------

std::vector<std::string> LDBCommand::StrSplit(const std::string& arg, char delim) {
  std::vector<std::string> splits;
  std::stringstream ss(arg);
  std::string item;

  while (std::getline(ss, item, delim)) {
    if(item == ""){continue;}
    splits.push_back(item);
  }
  return splits;
}

bool LDBCommand::StringToBool(std::string val) {
  std::transform(val.begin(), val.end(), val.begin(),
                 [](char ch) -> char { return (char)::tolower(ch); });

  if (val == "true") {
    return true;
  } else if (val == "false") {
    return false;
  } else {
    throw "Invalid value for boolean argument";
  }
}

char* LDBCommand::EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

void LDBCommand::PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

const char* LDBCommand::GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool LDBCommand::GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

uint32_t LDBCommand::DecodeFixed32(const char* ptr) {
  if (1) {//port::kLittleEndian
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

uint64_t LDBCommand::DecodeFixed64(const char* ptr) {
  if (1) {//port::kLittleEndian
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

std::string LDBCommand::ParseKey(const Slice& internal_key) {
  std::string key_string;
  if(read_options_.iter_start_seqnum == 0){
    key_string = internal_key.ToString();
  }else{
    const size_t n = internal_key.size();
    if (n < 8) key_string = internal_key.ToString();
    uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
    size_t type = num & 0xff;
    uint64_t sequence = num >> 8;
    Slice key = Slice(internal_key.data(), n - 8);
    key_string = key.ToString() + "_"+ std::to_string(sequence)+ "_"+ std::to_string(type);
  }
  return key_string;
}

// -------------------------------------- functions --------------------------------------

LDBCommand* LDBCommand::InitCommand(const std::string cmd, DB* db, DBWithTTL* db_with_ttl, std::vector<const Snapshot*> snapshot, std::vector<Iterator*> iterator, WriteBatchWithIndex* batch) {
  
  std::vector<std::string> cmdSplits = StrSplit(cmd, ' ');
#ifdef DEBUG
  std::cout  << "(Ldb-Tool) Command: " << cmd << ", Command size: " << cmdSplits.size() <<std::endl;
#endif
  return InitCommand(cmdSplits, db, db_with_ttl, snapshot, iterator, batch, SelectCommand);
}

/**
 * Parse the command-line arguments and create the appropriate LDBCommand2
 * instance.
 * The command line arguments must be in the following format:
 * ./ldb --db=PATH_TO_DB [--commonOpt1=commonOpt1Val] ..
 *        COMMAND <PARAM1> <PARAM2> ... [-cmdSpecificOpt1=cmdSpecificOpt1Val] ..
 * This is similar to the command line format used by HBaseClientTool.
 * Command name is not included in args.
 * Returns nullptr if the command-line cannot be parsed.
 */
LDBCommand* LDBCommand::InitCommand(
    const std::vector<std::string>& cmdSplits /*column_families*/,
    DB* db,
    DBWithTTL* db_with_ttl,
    std::vector<const Snapshot*> snapshot,
    std::vector<Iterator*> iterator,
    WriteBatchWithIndex* batch,
    const std::function<LDBCommand*(const ParsedParams&)>& selector) {
  // --x=y command line arguments are added as x->y map entries in
  // parsed_params.option_map.
  //
  // Command-line arguments of the form --hex end up in this array as hex to
  // parsed_params.flags
  ParsedParams parsed_params;

  // Everything other than option_map and flags. Represents commands
  // and their parameters.  For eg: put key1 value1 go into this vector.
  std::vector<std::string> cmdTokens;
  const std::string OPTION_PREFIX = "--";

  if(cmdSplits.size()==0){
#ifdef DEBUG
    std::cout <<"(Ldb-Tool) Invalid Command: " << parsed_params.cmd << " size: " << cmdSplits.size() << std::endl;
#endif 
    return nullptr;
  }

  for (const auto& cmdSplit : cmdSplits) {
    if (cmdSplit[0] == '-' && cmdSplit[1] == '-'){
      std::vector<std::string> splits = StrSplit(cmdSplit, '=');
      // --option_name=option_value
      if (splits.size() == 2) {
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.option_map[optionKey] = splits[1];
      } else if (splits.size() == 1) {
        // --flag_name
        std::string optionKey = splits[0].substr(OPTION_PREFIX.size());
        parsed_params.flags.push_back(optionKey);
      } else {}
    } else {
      cmdTokens.push_back(cmdSplit);
    }
  }

  parsed_params.cmd = cmdTokens[0];
  parsed_params.cmd_params.assign(cmdTokens.begin() + 1, cmdTokens.end());
  LDBCommand* command = selector(parsed_params);
  
  if (!command) {
#ifdef DEBUG
    std::cout <<"(Ldb-Tool) Invalid: command == nullptr1" <<std::endl;
#endif
  }else{
    command->SetDB(db);
    command->SetDBWithTTL(db_with_ttl);
    command->SetSnapshot(snapshot);
    command->SetIterator(iterator);
    command->SetBatch(batch);
  }
  return command;
}

LDBCommand* LDBCommand::SelectCommand(const ParsedParams& parsed_params) {
  if (parsed_params.cmd == GetCommand::Name()) {
    return new GetCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetWithTTLCommand::Name()) {
    return new GetWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == MultiGetCommand::Name()) {
    return new MultiGetCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetSnapshotCommand::Name()){
    return new GetSnapshotCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == ReleaseSnapshotCommand::Name()){
    return new ReleaseSnapshotCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == OpenDBCommand::Name()){
    return new OpenDBCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == OpenDBWithTTLCommand::Name()){
    return new OpenDBWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == OpenAsSecondaryCommand::Name()){
    return new OpenAsSecondaryCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteDBCommand::Name()){
    return new DeleteDBCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteDBWithTTLCommand::Name()){
    return new DeleteDBWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == CloseCommand::Name()){
    return new CloseCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == CloseWithTTLCommand::Name()){
    return new CloseWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetPropertyCommand::Name()){
    return new GetPropertyCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetApproximateSizesCommand::Name()){
    return new GetApproximateSizesCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == CompactRangeCommand::Name()){
    return new CompactRangeCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == FlushCommand::Name()){
    return new FlushCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DestroyDBCommand::Name()){
    return new DestroyDBCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == RepairDBCommand::Name()){
    return new RepairDBCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorSeekCommand::Name()){
    return new IteratorSeekCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorSeekToFirstCommand::Name()){
    return new IteratorSeekToFirstCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorSeekToLastCommand::Name()){
    return new IteratorSeekToLastCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorNextCommand::Name()){
    return new IteratorNextCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorPrevCommand::Name()){
    return new IteratorPrevCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorValidCommand::Name()){
    return new IteratorValidCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorRefreshCommand::Name()){
    return new IteratorRefreshCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == NewIteratorCommand::Name()){
    return new NewIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteIteratorCommand::Name()){
    return new DeleteIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == PutCommand::Name()){
    return new PutCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == PutWithTTLCommand::Name()){
    return new PutWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == FlushWALCommand::Name()){
    return new FlushWALCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == IteratorCommand::Name()){
    return new IteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == WriteCommand::Name()){
    return new WriteCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == WBWIPutCommand::Name()){
    return new WBWIPutCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == WBWIDeleteCommand::Name()){
    return new WBWIDeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == WBWIIteratorCommand::Name()){
    return new WBWIIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == WBWIWriteCommand::Name()){
    return new WBWIWriteCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteCommand::Name()){
    return new DeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteRangeCommand::Name()){
    return new DeleteRangeCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteRangeWithTTLCommand::Name()){
    return new DeleteRangeWithTTLCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == SingleDeleteCommand::Name()){
    return new SingleDeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }
  std::cout <<"(Ldb-Tool) Invalid Command: " << parsed_params.cmd << std::endl;
  return nullptr;
}

/* Run the command, and return the execute result. */
void LDBCommand::Run() {
  // We'll intentionally proceed even if the DB can't be opened because users
  // can also specify a filename, not just a directory.
  if (invalid_command_ == true){
    return;
  }
  DoCommand();
}

LDBCommand::LDBCommand(const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags){

}

/**
  * A helper function that returns a list of command line options
  * used by this command.  It includes the common options and the ones
  * passed in.
  */
bool LDBCommand::ParseDBOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags){
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options_.IncreaseParallelism();
  options_.OptimizeLevelStyleCompaction();
  
  // bool create_if_missing = false
  std::vector<std::string>::const_iterator vitr;
  vitr = std::find(flags.begin(), flags.end(), ARG_CREATE_IF_MISSING);
  if (vitr != flags.end()) {
    options_.create_if_missing = true;
  }
  std::map<std::string, std::string>::const_iterator itr;
  itr = options.find(ARG_CREATE_IF_MISSING);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.create_if_missing = StringToBool(val);
  }
  // bool error_if_exists = false
  vitr = std::find(flags.begin(), flags.end(), ARG_ERROR_IF_EXISTS);
  if (vitr != flags.end()) {
    options_.error_if_exists = true;
  }
  itr = options.find(ARG_ERROR_IF_EXISTS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.error_if_exists = StringToBool(val);
  }
  //DBOptions* OptimizeForSmallDb(std::shared_ptr<Cache>* cache = nullptr);
  //DBOptions* IncreaseParallelism(int total_threads = 16);

  // bool create_missing_column_families = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_CREATE_MISSING_COLUMN_FAMILIES);
  if (vitr != flags.end()) {
    options_.create_missing_column_families = true;
  }
  itr = options.find(ARG_CREATE_MISSING_COLUMN_FAMILIES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.create_missing_column_families = StringToBool(val);
  }

  // bool paranoid_checks = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_PARANOID_CHECKS);
  if (vitr != flags.end()) {
    options_.paranoid_checks = true;
  }
  itr = options.find(ARG_PARANOID_CHECKS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.paranoid_checks = StringToBool(val);
  }
  //Env* env = Env::Default();
  //std::shared_ptr<FileSystem> file_system = nullptr;
  //std::shared_ptr<RateLimiter> rate_limiter = nullptr;
  //std::shared_ptr<SstFileManager> sst_file_manager = nullptr;
  //std::shared_ptr<Logger> info_log = nullptr;
  //InfoLogLevel info_log_level = INFO_LEVEL;
  //InfoLogLevel info_log_level = DEBUG_LEVEL;
  //int max_open_files = -1;
  itr = options.find(ARG_MAX_OPEN_FILES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_open_files = std::stoi(val);
  }
  //int max_file_opening_threads = 16;
  itr = options.find(ARG_MAX_FILE_OPENING_THREADS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_file_opening_threads = std::stoi(val);
  }
  //uint64_t max_total_wal_size = 0;
  itr = options.find(ARG_MAX_TOTAL_WAL_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_total_wal_size = std::stoull(val);
  }
  //std::shared_ptr<Statistics> statistics = nullptr;
  // bool use_fsync = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_USE_FSYNC);
  if (vitr != flags.end()) {
    options_.use_fsync = true;
  }
  itr = options.find(ARG_USE_FSYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.use_fsync = StringToBool(val);
  }
  //std::vector<DbPath> db_paths;
  //std::string db_log_dir = "";
  //std::string wal_dir = "";
  //uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;
  itr = options.find(ARG_DELETE_OBSOLETE_FILES_PERIOD_MICROS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.delete_obsolete_files_period_micros = std::stoull(val);
  }
  //int max_background_jobs = 2;
  itr = options.find(ARG_MAX_BACKGROUND_JOBS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_background_jobs = std::stoi(val);
  }
  //int base_background_compactions = -1;
  itr = options.find(ARG_BASE_BACKGROUND_COMPACTIONS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.base_background_compactions = std::stoi(val);
  }
  //int max_background_compactions = -1;
  itr = options.find(ARG_MAX_BACKGROUND_COMPACTIONS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_background_compactions = std::stoi(val);
  }
  //uint32_t max_subcompactions = 1;
  itr = options.find(ARG_MAX_SUBCOMPACTIONS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_subcompactions = std::stoul(val);
  }
  //int max_background_flushes = -1;
  itr = options.find(ARG_MAX_BACKGROUND_FLUSHES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_background_flushes = std::stoi(val);
  }
  //size_t max_log_file_size = 0;
  itr = options.find(ARG_MAX_LOG_FILE_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_log_file_size = std::stoul(val);
  }
  //size_t log_file_time_to_roll = 0;
  itr = options.find(ARG_LOG_FILE_TIME_TO_ROLL);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.log_file_time_to_roll = std::stoul(val);
  }
  //size_t keep_log_file_num = 1000;
  itr = options.find(ARG_KEEP_LOG_FILE_NUM);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.keep_log_file_num = std::stoul(val);
  }
  //size_t recycle_log_file_num = 0;
  itr = options.find(ARG_RECYCLE_LOG_FILE_NUM);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.recycle_log_file_num = std::stoul(val);
  }
  //uint64_t max_manifest_file_size = 1024 * 1024 * 1024;
  itr = options.find(ARG_MAX_MANIFEST_FILE_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_manifest_file_size = std::stoull(val);
  }
  //int table_cache_numshardbits = 6;
  itr = options.find(ARG_TABLE_CACHE_NUMSHARDBITS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.table_cache_numshardbits = std::stoi(val);
  }
  //uint64_t WAL_ttl_seconds = 0;
  itr = options.find(ARG_WAL_TTL_SECONDS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.WAL_ttl_seconds = std::stoull(val);
  }
  //uint64_t WAL_size_limit_MB = 0;
  itr = options.find(ARG_WAL_SIZE_LIMIT_MB);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.WAL_size_limit_MB = std::stoull(val);
  }
  //size_t manifest_preallocation_size = 4 * 1024 * 1024;
  itr = options.find(ARG_MANIFEST_PREALLOCATION_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.manifest_preallocation_size = std::stoul(val);
  }
  // bool allow_mmap_reads = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_MMAP_READS);
  if (vitr != flags.end()) {
    options_.allow_mmap_reads = true;
  }
  itr = options.find(ARG_ALLOW_MMAP_READS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_mmap_reads = StringToBool(val);
  }
  // bool allow_mmap_writes = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_MMAP_WRITES);
  if (vitr != flags.end()) {
    options_.allow_mmap_writes = true;
  }
  itr = options.find(ARG_ALLOW_MMAP_WRITES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_mmap_writes = StringToBool(val);
  }
  // bool use_direct_reads = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_USE_DIRECT_READS);
  if (vitr != flags.end()) {
    options_.use_direct_reads = true;
  }
  itr = options.find(ARG_USE_DIRECT_READS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.use_direct_reads = StringToBool(val);
  }
  // bool use_direct_io_for_flush_and_compaction = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION);
  if (vitr != flags.end()) {
    options_.use_direct_io_for_flush_and_compaction = true;
  }
  itr = options.find(ARG_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.use_direct_io_for_flush_and_compaction = StringToBool(val);
  }
  // bool allow_fallocate = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_FALLOCATE);
  if (vitr != flags.end()) {
    options_.allow_fallocate = true;
  }
  itr = options.find(ARG_ALLOW_FALLOCATE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_fallocate = StringToBool(val);
  }
  // bool is_fd_close_on_exec = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_IS_FD_CLOSE_ON_EXEC);
  if (vitr != flags.end()) {
    options_.is_fd_close_on_exec = true;
  }
  itr = options.find(ARG_IS_FD_CLOSE_ON_EXEC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.is_fd_close_on_exec = StringToBool(val);
  }
  // bool skip_log_error_on_recovery = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_SKIP_LOG_ERROR_ON_RECOVERY);
  if (vitr != flags.end()) {
    options_.skip_log_error_on_recovery = true;
  }
  itr = options.find(ARG_SKIP_LOG_ERROR_ON_RECOVERY);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.skip_log_error_on_recovery = StringToBool(val);
  }
  //unsigned int stats_dump_period_sec = 600;
  itr = options.find(ARG_STATS_DUMP_PERIOD_SEC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.stats_dump_period_sec = std::stoul(val);
  }
  //unsigned int stats_persist_period_sec = 600;
  itr = options.find(ARG_STATS_PERSIST_PERIOD_SEC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.stats_persist_period_sec = std::stoul(val);
  }
  //bool persist_stats_to_disk = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_PERSIST_STATS_TO_DISK);
  if (vitr != flags.end()) {
    options_.persist_stats_to_disk = true;
  }
  itr = options.find(ARG_PERSIST_STATS_TO_DISK);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.persist_stats_to_disk = StringToBool(val);
  }
  //size_t stats_history_buffer_size = 1024 * 1024;
  itr = options.find(ARG_STATS_HISTORY_BUFFER_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.stats_history_buffer_size = std::stoul(val);
  }
  // bool advise_random_on_open = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_ADVISE_RANDOM_ON_OPEN);
  if (vitr != flags.end()) {
    options_.advise_random_on_open = true;
  }
  itr = options.find(ARG_ADVISE_RANDOM_ON_OPEN);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.advise_random_on_open = StringToBool(val);
  }
  //size_t db_write_buffer_size = 0;
  itr = options.find(ARG_DB_WRITE_BUFFER_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.db_write_buffer_size = std::stoul(val);
  }
  //std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;
  //enum AccessHint { NONE, NORMAL, SEQUENTIAL, WILLNEED };
  //AccessHint access_hint_on_compaction_start = NORMAL;
  // bool new_table_reader_for_compaction_inputs = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_NEW_TABLE_READER_FOR_COMPACTION_INPUTS);
  if (vitr != flags.end()) {
    options_.new_table_reader_for_compaction_inputs = true;
  }
  itr = options.find(ARG_NEW_TABLE_READER_FOR_COMPACTION_INPUTS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.new_table_reader_for_compaction_inputs = StringToBool(val);
  }
  //size_t compaction_readahead_size = 0;
  itr = options.find(ARG_COMPACTION_READAHEAD_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.compaction_readahead_size = std::stoul(val);
  }
  //size_t random_access_max_buffer_size = 1024 * 1024;
  itr = options.find(ARG_RANDOM_ACCESS_MAX_BUFFER_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.random_access_max_buffer_size = std::stoul(val);
  }
  //size_t writable_file_max_buffer_size = 1024 * 1024;
  itr = options.find(ARG_WRITABLE_FILE_MAX_BUFFER_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.writable_file_max_buffer_size = std::stoul(val);
  }
  // bool use_adaptive_mutex = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_USE_ADAPTIVE_MUTEX);
  if (vitr != flags.end()) {
    options_.use_adaptive_mutex = true;
  }
  itr = options.find(ARG_USE_ADAPTIVE_MUTEX);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.use_adaptive_mutex = StringToBool(val);
  }
  //DBOptions();
  //explicit DBOptions(const Options& options);
  //void Dump(Logger* log) const;
  //uint64_t bytes_per_sync = 0;
  itr = options.find(ARG_BYTES_PER_SYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.bytes_per_sync = std::stoull(val);
  }
  //uint64_t wal_bytes_per_sync = 0;
  itr = options.find(ARG_WAL_BYTES_PER_SYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.wal_bytes_per_sync = std::stoull(val);
  }
  // bool strict_bytes_per_sync = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_STRICT_BYTES_PER_SYNC);
  if (vitr != flags.end()) {
    options_.strict_bytes_per_sync = true;
  }
  itr = options.find(ARG_STRICT_BYTES_PER_SYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.strict_bytes_per_sync = StringToBool(val);
  }
  //std::vector<std::shared_ptr<EventListener>> listeners;
  // bool enable_thread_tracking = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ENABLE_THREAD_TRACKING);
  if (vitr != flags.end()) {
    options_.enable_thread_tracking = true;
  }
  itr = options.find(ARG_ENABLE_THREAD_TRACKING);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.enable_thread_tracking = StringToBool(val);
  }
  //uint64_t delayed_write_rate = 0;
  itr = options.find(ARG_DELAYED_WRITE_RATE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.delayed_write_rate = std::stoull(val);
  }
  // bool enable_pipelined_write = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ENABLE_PIPELINED_WRITE);
  if (vitr != flags.end()) {
    options_.enable_pipelined_write = true;
  }
  itr = options.find(ARG_ENABLE_PIPELINED_WRITE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.enable_pipelined_write = StringToBool(val);
  }
  // bool unordered_write = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_UNORDERED_WRITE);
  if (vitr != flags.end()) {
    options_.unordered_write = true;
  }
  itr = options.find(ARG_UNORDERED_WRITE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.unordered_write = StringToBool(val);
  }
  // bool allow_concurrent_memtable_write = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_CONCURRENT_MEMTABLE_WRITE);
  if (vitr != flags.end()) {
    options_.allow_concurrent_memtable_write = true;
  }
  itr = options.find(ARG_ALLOW_CONCURRENT_MEMTABLE_WRITE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_concurrent_memtable_write = StringToBool(val);
  }
  // bool enable_write_thread_adaptive_yield = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD);
  if (vitr != flags.end()) {
    options_.enable_write_thread_adaptive_yield = true;
  }
  itr = options.find(ARG_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.enable_write_thread_adaptive_yield = StringToBool(val);
  }
  //uint64_t max_write_batch_group_size_bytes = 1 << 20;
  itr = options.find(ARG_MAX_WRITE_BATCH_GROUP_SIZE_BYTES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_write_batch_group_size_bytes = std::stoull(val);
  }
  //uint64_t write_thread_max_yield_usec = 100;
  itr = options.find(ARG_WRITE_THREAD_MAX_YIELD_USEC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.write_thread_max_yield_usec = std::stoull(val);
  }
  //uint64_t write_thread_slow_yield_usec = 3;
  itr = options.find(ARG_WRITE_THREAD_SLOW_YIELD_USEC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.write_thread_slow_yield_usec = std::stoull(val);
  }
  // bool skip_stats_update_on_db_open = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_SKIP_STATS_UPDATE_ON_DB_OPEN);
  if (vitr != flags.end()) {
    options_.skip_stats_update_on_db_open = true;
  }
  itr = options.find(ARG_SKIP_STATS_UPDATE_ON_DB_OPEN);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.skip_stats_update_on_db_open = StringToBool(val);
  }
  // bool skip_checking_sst_file_sizes_on_db_open = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_SKIP_CHECKING_SST_FILE_SIZES_ON_DB_OPEN);
  if (vitr != flags.end()) {
    options_.skip_checking_sst_file_sizes_on_db_open = true;
  }
  itr = options.find(ARG_SKIP_CHECKING_SST_FILE_SIZES_ON_DB_OPEN);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.skip_checking_sst_file_sizes_on_db_open = StringToBool(val);
  }
  //WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  // bool allow_2pc = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_2PC);
  if (vitr != flags.end()) {
    options_.allow_2pc = true;
  }
  itr = options.find(ARG_ALLOW_2PC);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_2pc = StringToBool(val);
  }
  //std::shared_ptr<Cache> row_cache = nullptr;
  itr = options.find(ARG_ROW_CACHE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.row_cache = NewLRUCache(std::stoull(val));
  }
  //WalFilter* wal_filter = nullptr;
  // bool fail_if_options_file_error = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_FAIL_IF_OPTIONS_FILE_ERROR);
  if (vitr != flags.end()) {
    options_.fail_if_options_file_error = true;
  }
  itr = options.find(ARG_FAIL_IF_OPTIONS_FILE_ERROR);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.fail_if_options_file_error = StringToBool(val);
  }
  // bool dump_malloc_stats = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_DUMP_MALLOC_STATS);
  if (vitr != flags.end()) {
    options_.dump_malloc_stats = true;
  }
  itr = options.find(ARG_DUMP_MALLOC_STATS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.dump_malloc_stats = StringToBool(val);
  }
  // bool avoid_flush_during_recovery = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_AVOID_FLUSH_DURING_RECOVERY);
  if (vitr != flags.end()) {
    options_.avoid_flush_during_recovery = true;
  }
  itr = options.find(ARG_AVOID_FLUSH_DURING_RECOVERY);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.avoid_flush_during_recovery = StringToBool(val);
  }
  // bool avoid_flush_during_shutdown = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_AVOID_FLUSH_DURING_SHUTDOWN);
  if (vitr != flags.end()) {
    options_.avoid_flush_during_shutdown = true;
  }
  itr = options.find(ARG_AVOID_FLUSH_DURING_SHUTDOWN);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.avoid_flush_during_shutdown = StringToBool(val);
  }
  // bool allow_ingest_behind = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ALLOW_INGEST_BEHIND);
  if (vitr != flags.end()) {
    options_.allow_ingest_behind = true;
  }
  itr = options.find(ARG_ALLOW_INGEST_BEHIND);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.allow_ingest_behind = StringToBool(val);
  }
  // bool preserve_deletes = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_PRESERVE_DELETES);
  if (vitr != flags.end()) {
    options_.preserve_deletes = true;
  }
  itr = options.find(ARG_PRESERVE_DELETES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.preserve_deletes = StringToBool(val);
  }
  // bool two_write_queues = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_TWO_WRITE_QUEUES);
  if (vitr != flags.end()) {
    options_.two_write_queues = true;
  }
  itr = options.find(ARG_TWO_WRITE_QUEUES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.two_write_queues = StringToBool(val);
  }
  // bool manual_wal_flush = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_MANUAL_WAL_FLUSH);
  if (vitr != flags.end()) {
    options_.manual_wal_flush = true;
  }
  itr = options.find(ARG_MANUAL_WAL_FLUSH);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.manual_wal_flush = StringToBool(val);
  }
  // bool atomic_flush = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_ATOMIC_FLUSH);
  if (vitr != flags.end()) {
    options_.atomic_flush = true;
  }
  itr = options.find(ARG_ATOMIC_FLUSH);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.atomic_flush = StringToBool(val);
  }
  // bool avoid_unnecessary_blocking_io = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_AVOID_UNNECESSARY_BLOCKING_IO);
  if (vitr != flags.end()) {
    options_.avoid_unnecessary_blocking_io = true;
  }
  itr = options.find(ARG_AVOID_UNNECESSARY_BLOCKING_IO);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.avoid_unnecessary_blocking_io = StringToBool(val);
  }
  // bool write_dbid_to_manifest = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_WRITE_DBID_TO_MANIFEST);
  if (vitr != flags.end()) {
    options_.write_dbid_to_manifest = true;
  }
  itr = options.find(ARG_WRITE_DBID_TO_MANIFEST);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.write_dbid_to_manifest = StringToBool(val);
  }
  //size_t log_readahead_size = 0;
  itr = options.find(ARG_LOG_READAHEAD_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.log_readahead_size = std::stoul(val);
  }
  //std::shared_ptr<FileChecksumFunc> sst_file_checksum_func = nullptr;
  return true;
}

bool LDBCommand::ParseReadOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags){

  std::vector<std::string>::const_iterator vitr;
  std::map<std::string, std::string>::const_iterator itr;
/*
  for(itr = options.begin(); itr != options.end(); itr++){
    std::cout << itr->first << " " << itr->second << std::endl;
  }
*/
  // const Snapshot* snapshot = nullptr; leveldb/hyperleveldb ONLY
  itr = options.find(ARG_SNAPSHOT);
  if (itr != options.end()) {
    std::string val = itr->second;
    snapshot_index_ = std::stoi(val);
  }else{
    snapshot_index_ = -1;
  }

  // const Slice* iterate_lower_bound = nullptr;
  itr = options.find(ARG_ITERATE_LOWER_BOUND);
  if (itr != options.end()) {
    iterate_lower_bound_ = itr->second;
    //std::cout <<"read_options_.iterate_lower_bound_ = " << iterate_lower_bound_ << std::endl;
  }else{
    iterate_lower_bound_ = "";
  }
  // const Slice* iterate_upper_bound = nullptr;
  itr = options.find(ARG_ITERATE_UPPER_BOUND);
  if (itr != options.end()) {
    iterate_upper_bound_ = itr->second;
    //std::cout <<"read_options_.iterate_upper_bound_ = " << iterate_upper_bound_ << std::endl;
  }else{
    iterate_upper_bound_ = "";
  }

  // size_t readahead_size = 0;

  // uint64_t max_skippable_internal_keys = 0;

  // ReadTier read_tier = kReadAllTier;

  // bool verify_checksums = true; leveldb/hyperleveldb ONLY
  vitr = std::find(flags.begin(), flags.end(), ARG_VERIFY_CHECKSUMS);
  if (vitr != flags.end()) {
    read_options_.verify_checksums = false;
  }
  itr = options.find(ARG_VERIFY_CHECKSUMS);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.verify_checksums = StringToBool(val);
  }

  // bool fill_cache = true; leveldb/hyperleveldb ONLY
  vitr = std::find(flags.begin(), flags.end(), ARG_FILL_CACHE);
  if (vitr != flags.end()) {
    read_options_.fill_cache = false;
  }
  itr = options.find(ARG_FILL_CACHE);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.fill_cache = StringToBool(val);
  }

  // bool tailing = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_TAILING);
  if (vitr != flags.end()) {
    read_options_.tailing = true;
  }
  itr = options.find(ARG_TAILING);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.tailing = StringToBool(val);
  }

  // bool managed = removed;

  // bool total_order_seek = true;
  vitr = std::find(flags.begin(), flags.end(), ARG_TOTAL_ORDER_SEEK);
  if (vitr != flags.end()) {
    read_options_.total_order_seek = false;
  }
  itr = options.find(ARG_TOTAL_ORDER_SEEK);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.total_order_seek = StringToBool(val);
  }

  // bool auto_prefix_mode = unknown;
  vitr = std::find(flags.begin(), flags.end(), ARG_AUTO_PREFIX_MODE);
  if (vitr != flags.end()) {
    read_options_.auto_prefix_mode = false;
  }
  itr = options.find(ARG_AUTO_PREFIX_MODE);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.auto_prefix_mode = StringToBool(val);
  }

  // bool prefix_same_as_start = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_PREFIX_SAME_AS_START);
  if (vitr != flags.end()) {
    read_options_.prefix_same_as_start = true;
  }
  itr = options.find(ARG_PREFIX_SAME_AS_START);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.prefix_same_as_start = StringToBool(val);
  }

  // bool pin_data = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_PIN_DATA);
  if (vitr != flags.end()) {
    read_options_.pin_data = true;
  }
  itr = options.find(ARG_PIN_DATA);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.pin_data = StringToBool(val);
  }

  // bool background_purge_on_iterator_cleanup = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_BACKGROUND_PURGE_ON_ITERATOR_CLEANUP);
  if (vitr != flags.end()) {
    read_options_.background_purge_on_iterator_cleanup = true;
  }
  itr = options.find(ARG_BACKGROUND_PURGE_ON_ITERATOR_CLEANUP);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.background_purge_on_iterator_cleanup = StringToBool(val);
  }

  // bool ignore_range_deletions = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_IGNORE_RANGE_DELETIONS);
  if (vitr != flags.end()) {
    read_options_.ignore_range_deletions = true;
  }
  itr = options.find(ARG_IGNORE_RANGE_DELETIONS);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.ignore_range_deletions = StringToBool(val);
  }

  // std::function<bool(const TableProperties&)> table_filter;// Default: empty (every table will be scanned)

  // SequenceNumber iter_start_seqnum = 0;
  itr = options.find(ARG_ITER_START_SEQNUM);
  if (itr != options.end()) {
    std::string val = itr->second;
    read_options_.iter_start_seqnum = std::stoul(val);
    //std::cout <<"read_options_.iter_start_seqnum = " << read_options_.iter_start_seqnum << std::endl;
  }

  // const Slice* timestamp = nullptr;
  itr = options.find(ARG_TIMESTAMP);
  if (itr != options.end()) {
    timestamp_ = itr->second;
    //std::cout <<"read_options_.timestamp_ = " << timestamp_ << std::endl;
  }else{
    timestamp_ = "";
  }

  return true;
}

/**
  * Returns the value of the specified option as a boolean.
  * default_val is used if the option is not found in options.
  * Throws an exception if the value of the option is not
  * "true" or "false" (case insensitive).
  */
bool LDBCommand::ParseWriteOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags){

  std::vector<std::string>::const_iterator vitr;
  std::map<std::string, std::string>::const_iterator itr;

  // bool sync = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_SYNC);
  if (vitr != flags.end()) {
    write_options_.sync = true;
  }
  itr = options.find(ARG_SYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.sync = StringToBool(val);
  }

  // bool disableWAL = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_DISABLEWAL);
  if (vitr != flags.end()) {
    write_options_.disableWAL = true;
  }
  itr = options.find(ARG_DISABLEWAL);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.disableWAL = StringToBool(val);
  }

  // bool ignore_missing_column_families = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_IGNORE_MISSING_COLUMN_FAMILIES);
  if (vitr != flags.end()) {
    write_options_.ignore_missing_column_families = true;
  }
  itr = options.find(ARG_IGNORE_MISSING_COLUMN_FAMILIES);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.ignore_missing_column_families = StringToBool(val);
  }

  // bool no_slowdown = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_NO_SLOWDOWN);
  if (vitr != flags.end()) {
    write_options_.no_slowdown = true;
  }
  itr = options.find(ARG_NO_SLOWDOWN);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.no_slowdown = StringToBool(val);
  }

  // bool low_pri = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_LOW_PRI);
  if (vitr != flags.end()) {
    write_options_.low_pri = true;
  }
  itr = options.find(ARG_LOW_PRI);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.low_pri = StringToBool(val);
  }

  // bool memtable_insert_hint_per_batch = false;
  vitr = std::find(flags.begin(), flags.end(), ARG_MEMTABLE_INSERT_HINT_PER_BATCH);
  if (vitr != flags.end()) {
    write_options_.memtable_insert_hint_per_batch = true;
  }
  itr = options.find(ARG_MEMTABLE_INSERT_HINT_PER_BATCH);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.memtable_insert_hint_per_batch = StringToBool(val);
  }

  // const Slice* timestamp = nullptr;
  itr = options.find(ARG_TIMESTAMP);
  if (itr != options.end()) {
    timestamp_ = itr->second;
    //std::cout <<"write_options_.timestamp_ = " << timestamp_ << std::endl;
  }else{
    timestamp_ = "";
  }

  return true;
}

// ----------------------------------------------------------------------------

OpenDBCommand::OpenDBCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseDBOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
  }
}

void OpenDBCommand::DoCommand() {
  // open DB
  Status st;
  st = DB::Open(options_, db_path_, &db_);
  std::cout <<st.ToString() <<std::endl;
  if(st.ok()){
    exec_state_ = LDBCommandExecuteResult::ExeOpenDeleteDB(db_);
  }
  
}

// ----------------------------------------------------------------------------

OpenDBWithTTLCommand::OpenDBWithTTLCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseDBOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
  }
}

void OpenDBWithTTLCommand::DoCommand() {
  // open DB
  Status st;
  st = DBWithTTL::Open(options_, db_path_, &db_with_ttl_);
  std::cout <<st.ToString() <<std::endl;
  if(st.ok()){
    exec_state_ = LDBCommandExecuteResult::ExeOpenDeleteDBWithTTL(db_with_ttl_);
  }
  
}

// ----------------------------------------------------------------------------

OpenAsSecondaryCommand::OpenAsSecondaryCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseDBOption(options, flags);
  if(params.size() != 2){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
    secondary_path_ = params.at(1);
  }
}

void OpenAsSecondaryCommand::DoCommand() {
  // open DB
  Status st;
  st = DB::OpenAsSecondary(options_, db_path_, secondary_path_, &db_);
  std::cout <<st.ToString() <<std::endl;
  if(st.ok()){
    exec_state_ = LDBCommandExecuteResult::ExeOpenDeleteDB(db_);
  }
  
}

// ----------------------------------------------------------------------------

PutCommand::PutCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseWriteOption(options, flags);
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }
}

void PutCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_->Put(write_options_, key_, value_);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
}

// ----------------------------------------------------------------------------

PutWithTTLCommand::PutWithTTLCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseWriteOption(options, flags);
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }
}

void PutWithTTLCommand::DoCommand() {
  if (!db_with_ttl_) {
    std::cout <<"(Ldb-Tool) Invalid: db_with_ttl_ == nullptr" <<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_with_ttl_->Put(write_options_, key_, value_);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
}

// ----------------------------------------------------------------------------

FlushWALCommand::FlushWALCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void FlushWALCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  Status st = db_->FlushWAL(true);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<"(Ldb-Tool) Invalid: flushwal is not supported by leveldb" <<std::endl;
}

// ----------------------------------------------------------------------------

IteratorCommand::IteratorCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseReadOption(options, flags);  
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }
}

void IteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }
  
  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  std::string output = "";
  Iterator* it = db_->NewIterator(read_options_);
  std::vector<std::string> splits = StrSplit(key_, ':');
  if (splits.size() == 2 && splits[0] == "seek") {
    // seek:key1
    if (value_ == "next"){
      for(it->Seek(splits[1]); it->Valid(); it->Next()){
        //output += it->key().ToString() + " " + it->value().ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->Seek(splits[1]); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{ std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl; }

  }else if (splits.size() == 1 && splits[0] == "seektofirst") {
    // seektofirst
    if (value_ == "next"){
      for(it->SeekToFirst(); it->Valid(); it->Next()){
        //output += it->key().ToString() + " " + it->value().ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->SeekToFirst(); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl;}

  }else if (splits.size() == 1 && splits[0] == "seektolast") {
    //seektolast
    if (value_ == "next"){
      for(it->SeekToLast(); it->Valid(); it->Next()){
        //output += it->key().ToString() + " " + it->value().ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->SeekToLast(); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{ std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl; }
  }else{ std::cout <<"(Ldb-Tool) Invalid: iterator begin " << key_ <<std::endl; }

  delete it;
}

// ----------------------------------------------------------------------------

WriteCommand::WriteCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseWriteOption(options, flags);
  if (params.size() == 0) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() == 0" <<std::endl;
    invalid_command_ = true;
  } else {
    subcmd_.assign(params.begin(), params.end());
  }
}

void WriteCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  WriteBatch batch;
  for (const auto& subcmd : subcmd_) {
    std::vector<std::string> splits = StrSplit(subcmd, ':');
    if (splits.size() == 2 && splits[0] == DeleteCommand::Name()) {
      // delete:key1
      batch.Delete(splits[1]);

    } else if (splits.size() == 3 && splits[0] == PutCommand::Name()) {
      // put:key1:value1
      batch.Put(splits[1], splits[2]);

    } else {
      std::cout <<"(Ldb-Tool) Invalid: write subcmd " << subcmd <<std::endl;
      return;

    }
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_->Write(write_options_, &batch);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
}

// ----------------------------------------------------------------------------

WBWIPutCommand::WBWIPutCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
  }
}

void WBWIPutCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  if(!batch_){
    /*
    WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false,
      size_t max_bytes = 0)
    */
    batch_ = new WriteBatchWithIndex(BytewiseComparator(),0, true, 0);
    exec_state_ = LDBCommandExecuteResult::ExeGetReleaseBatch(batch_);
  }
  Status st = batch_->Put(key_, value_);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
}

// ----------------------------------------------------------------------------

WBWIDeleteCommand::WBWIDeleteCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
  }else{
    key_ = params.at(0);
  }
}

void WBWIDeleteCommand::DoCommand() {

  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  if(!batch_){
    /*
    WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false,
      size_t max_bytes = 0)
    */
    batch_ = new WriteBatchWithIndex(BytewiseComparator(),0, true, 0);
    exec_state_ = LDBCommandExecuteResult::ExeGetReleaseBatch(batch_);
  }

  Status st = batch_->Delete(key_);
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

WBWIIteratorCommand::WBWIIteratorCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseReadOption(options, flags);  
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    key_ = params.at(0);
    value_ = params.at(1);
    //std::cout << key_ <<" " << value_ <<std::endl;
  }
}

void WBWIIteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }

  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if(!batch_){
    /*
    WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false,
      size_t max_bytes = 0)
    */
    batch_ = new WriteBatchWithIndex(BytewiseComparator(),0, true, 0);
    exec_state_ = LDBCommandExecuteResult::ExeGetReleaseBatch(batch_);
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  std::string output = "";
  Iterator* it = batch_->NewIteratorWithBase(db_->DefaultColumnFamily(), db_->NewIterator(read_options_), &read_options_);

  std::vector<std::string> splits = StrSplit(key_, ':');
  if (splits.size() == 2 && splits[0] == "seek") {
    // seek:key1
    if (value_ == "next"){
      for(it->Seek(splits[1]); it->Valid(); it->Next()){
        //output += it->key.ToString() + " " + it->value.ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key.ToString()<<" "<< it->value.ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->Seek(splits[1]); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key.ToString()<<" "<< it->value.ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{ std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl; }

  }else if (splits.size() == 1 && splits[0] == "seektofirst") {
    // seektofirst
    if (value_ == "next"){
      for(it->SeekToFirst(); it->Valid(); it->Next()){
        //output += it->key().ToString() + " " + it->value().ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->SeekToFirst(); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key.ToString()<<" "<< it->value.ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl;}

  }else if (splits.size() == 1 && splits[0] == "seektolast") {
    //seektolast
    if (value_ == "next"){
      for(it->SeekToLast(); it->Valid(); it->Next()){
        //output += it->key.ToString() + " " + it->value.ToString() + " ";
        output += ParseKey(it->key()) + " " + it->value().ToString() + " ";
        //std::cout << it->key.ToString()<<" "<< it->value.ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else if (value_ == "prev"){
      for(it->SeekToLast(); it->Valid(); it->Prev()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key.ToString()<<" "<< it->value.ToString()<<" "<< std::endl;
      }
      Status st = it->status();
      std::cout <<st.ToString()<< " : " << output <<std::endl;

    }else{ std::cout <<"(Ldb-Tool) Invalid: iterator order " << value_ <<std::endl; }
  }else{ std::cout <<"(Ldb-Tool) Invalid: iterator begin " << key_ <<std::endl; }

  delete it;
}

// ----------------------------------------------------------------------------

WBWIWriteCommand::WBWIWriteCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseWriteOption(options, flags);
}

void WBWIWriteCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  if(!batch_){
    std::cout <<"(Ldb-Tool) Invalid: batch_ == nullptr" <<std::endl;
    return;
  }

  batch_->Clear();
  delete batch_;
  batch_ = nullptr;
  exec_state_ = LDBCommandExecuteResult::ExeGetReleaseBatch(batch_);
  std::cout <<"OK"<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
}

// ----------------------------------------------------------------------------

GetWithTTLCommand::GetWithTTLCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseReadOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void GetWithTTLCommand::DoCommand() {

  std::string value;
  if (!db_with_ttl_){
    std::cout <<"(Ldb-Tool) Invalid: db_with_ttl_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }
  
  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }
  else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    read_options_.timestamp = &val;
  }

  Status st = db_with_ttl_->Get(read_options_, key_, &value);
  //std::cout <<"key_: " <<key_<<std::endl;
  //std::cout <<st.ToString()<< " : " <<value.c_str()<<std::endl;
  std::cout <<st.ToString()<< " : " << value <<std::endl;
  //std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

GetCommand::GetCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseReadOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void GetCommand::DoCommand() {

  std::string value;
  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }
  
  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }
  else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    read_options_.timestamp = &val;
  }

  Status st = db_->Get(read_options_, key_, &value);
  //std::cout <<"key_: " <<key_<<std::endl;
  //std::cout <<st.ToString()<< " : " <<value.c_str()<<std::endl;
  std::cout <<st.ToString()<< " : " << value <<std::endl;
  //std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

MultiGetCommand::MultiGetCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseReadOption(options, flags);
  if(params.size() == 0){
    std::cout <<"(Ldb-Tool) Invalid: params.size() == 0" <<std::endl;
    invalid_command_ = true;
  }else{
    for(int i=0; i < params.size(); i++){
      keys_.push_back(params.at(i));
    }
  }
}

void MultiGetCommand::DoCommand() {

  std::vector<std::string> values;
  std::vector<Slice> keys;
  std::string output = "";
  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }
  
  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }
  else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    read_options_.timestamp = &val;
  }

  for(int i=0; i < keys_.size(); i++){
    keys.push_back(keys_.at(i));
    values.push_back("a");
  }
  //std::cout <<"keys_.at(0) " <<keys_.at(0) << std::endl;
  //std::cout <<"keys.at(0) " <<keys.at(0).ToString() << std::endl;
  std::vector<Status> sts = db_->MultiGet(read_options_, keys, &values);
  //std::cout <<"values.at(0) " <<values.at(0) << std::endl;
  //std::cout <<"key_: " <<key_<<std::endl;
  //std::cout <<st.ToString()<< " : " <<value.c_str()<<std::endl;
  //std::cout <<"values.size() " << values.size() << std::endl;
  for(int i=0; i < values.size(); i++){
    output += values.at(i) + " ";
  }
  Status st = sts.at(0);
  for(int i=0; i < sts.size(); i++){
    if(!sts.at(i).ok()){
      st = sts.at(i);
      break;
    }
  }
  std::cout <<st.ToString()<< " : " << output <<std::endl;
}

// ----------------------------------------------------------------------------

GetSnapshotCommand::GetSnapshotCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void GetSnapshotCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  const Snapshot* s1 = db_->GetSnapshot();
  std::cout <<"OK"<< " : " <<snapshot_.size()<<std::endl;
  snapshot_.push_back(s1);
  exec_state_ = LDBCommandExecuteResult::ExeGetReleaseSnapshot(snapshot_);
}

// ----------------------------------------------------------------------------

ReleaseSnapshotCommand::ReleaseSnapshotCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {


  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void ReleaseSnapshotCommand::DoCommand() {

  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  int snapshot_index = std::stoi(key_);
  if ((0 <= snapshot_index) && (snapshot_index <snapshot_.size())){
    db_->ReleaseSnapshot(snapshot_[snapshot_index]);
    std::cout <<"OK"<<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseSnapshot(snapshot_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

DeleteCommand::DeleteCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseWriteOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
  }else{
    key_ = params.at(0);
  }
}

void DeleteCommand::DoCommand() {

  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_->Delete(write_options_, key_);
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

DeleteRangeCommand::DeleteRangeCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseWriteOption(options, flags);
  if(params.size() != 2){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
  }else{
    begin_key_ = params.at(0);
    end_key_ = params.at(1);
  }
}

void DeleteRangeCommand::DoCommand() {

  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_->DeleteRange(write_options_, db_->DefaultColumnFamily(), begin_key_, end_key_);
  //Status st = db_with_ttl_->DeleteRange(write_options_, db_with_ttl_->DefaultColumnFamily(), begin_key_, end_key_);
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

DeleteRangeWithTTLCommand::DeleteRangeWithTTLCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseWriteOption(options, flags);
  if(params.size() != 2){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
  }else{
    begin_key_ = params.at(0);
    end_key_ = params.at(1);
  }
}

void DeleteRangeWithTTLCommand::DoCommand() {

  if (!db_with_ttl_){
    std::cout <<"(Ldb-Tool) Invalid: db_with_ttl_ == nullptr" <<std::endl;
    return;
  }

  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_with_ttl_->DeleteRange(write_options_, db_with_ttl_->DefaultColumnFamily(), begin_key_, end_key_);
  //Status st = db_with_ttl_->DeleteRange(write_options_, db_with_ttl_->DefaultColumnFamily(), begin_key_, end_key_);
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

SingleDeleteCommand::SingleDeleteCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseWriteOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
  }else{
    key_ = params.at(0);
  }
}

void SingleDeleteCommand::DoCommand() {

  if (!db_){
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  //std::cout <<"(Ldb-Tool) Invalid: singledelete is not supported by leveldb" <<std::endl;
  
  Slice val;
  if (timestamp_ !=""){
    val = timestamp_;
    write_options_.timestamp = &val;
  }

  Status st = db_->SingleDelete(write_options_, key_);
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

DeleteDBCommand::DeleteDBCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void DeleteDBCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  delete db_;
  std::cout <<"OK"<<std::endl;
  exec_state_ = LDBCommandExecuteResult::ExeOpenDeleteDB(nullptr);
}

// ----------------------------------------------------------------------------

DeleteDBWithTTLCommand::DeleteDBWithTTLCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void DeleteDBWithTTLCommand::DoCommand() {
  if (!db_with_ttl_) {
    std::cout <<"(Ldb-Tool) Invalid: db_with_ttl_ == nullptr" <<std::endl;
    return;
  }
  delete db_with_ttl_;
  std::cout <<"OK"<<std::endl;
  exec_state_ = LDBCommandExecuteResult::ExeOpenDeleteDBWithTTL(nullptr);
}

// ----------------------------------------------------------------------------

CloseCommand::CloseCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void CloseCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  Status st = db_->Close();
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

CloseWithTTLCommand::CloseWithTTLCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {}

void CloseWithTTLCommand::DoCommand() {
  if (!db_with_ttl_) {
    std::cout <<"(Ldb-Tool) Invalid: db_with_ttl_ == nullptr" <<std::endl;
    return;
  }
  Status st = db_with_ttl_->Close();
  std::cout <<st.ToString()<<std::endl;
}

// ----------------------------------------------------------------------------

GetPropertyCommand::GetPropertyCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  if (params.size() != 1) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  } else {
    property_ = params.at(0);
  }
}

void GetPropertyCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice property = property_;
  std::string value;
  bool status = db_->GetProperty(property, &value);
  if(status){
    std::cout << "OK" << " : " <<"property="<< property.ToString() << ", value=" << value <<std::endl;
  }else{
    std::cout << "false" << " : " <<"property="<< property.ToString() << ", value=" << value <<std::endl;
  }
}

// ----------------------------------------------------------------------------

GetApproximateSizesCommand::GetApproximateSizesCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if (params.size() == 0 || params.size()%2 != 0) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() == 0 || params.size()%2 != 0" <<std::endl;
    invalid_command_ = true;
  } else {
    key_.assign(params.begin(), params.end());
  }
}

void GetApproximateSizesCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  if(key_.size()/2 > 128){
    std::cout <<"(Ldb-Tool) Invalid: oversize ranges = " << key_.size() <<std::endl;
    return;
  }

  std::string result = "";
  Range ranges[128];
  uint64_t sizes[128];

  for (int i = 0, j =0 ; i < key_.size(); i = i+2, j++)
  {
    ranges[j] = Range(key_[i], key_[i + 1]);
    std::cout << "range " << j << ": key_[" << i <<"]=" << key_[i] <<" key_[" << i+1 << "]="<< key_[i+1] << std::endl;
  }

  db_->GetApproximateSizes(ranges, key_.size()/2, sizes);
  for (int i = 0; i < key_.size()/2; i++)
  {
    result.append( ranges[i].start.ToString() + " to " + ranges[i].limit.ToString() + " sizes=" + std::to_string(sizes[i]) + ", ");
  }
  std::cout << "OK" << " : " << result <<std::endl;
}

// ----------------------------------------------------------------------------

CompactRangeCommand::CompactRangeCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    begin_key_ = params.at(0);
    end_key_ = params.at(1);
  }
}

void CompactRangeCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice* begin = nullptr;
  Slice begin_key;
  if(begin_key_ != "nullptr"){
    begin_key = begin_key_;
    begin = &begin_key;
  }

  Slice* end = nullptr;
  Slice end_key;
 if(end_key_ != "nullptr"){
    end_key = end_key_;
    end = &end_key;
  }
  CompactRangeOptions compact_range_options_; //rocksdb ONLY
  // compact database
  db_->CompactRange(compact_range_options_, begin, end); //rocksdb ONLY
  //std::cout <<"OK"<<std::endl;
  std::cout <<"OK"<< " : " <<"begin="<< begin << ", end=" << end <<std::endl;
}

// ----------------------------------------------------------------------------

FlushCommand::FlushCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

}

void FlushCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  FlushOptions flush_options_; //rocksdb ONLY
  // Flush
  Status st;
  st = db_->Flush(flush_options_); //rocksdb ONLY
  std::cout <<st.ToString() <<std::endl;
}

// ----------------------------------------------------------------------------

DestroyDBCommand::DestroyDBCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseDBOption(options, flags); 
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
  }
}

void DestroyDBCommand::DoCommand() {
  // Destroy DB
  Status st;
  st = DestroyDB(db_path_, options_);
  std::cout <<st.ToString() <<std::endl;
  
}

// ----------------------------------------------------------------------------

RepairDBCommand::RepairDBCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseDBOption(options, flags);
  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
  }
}

void RepairDBCommand::DoCommand() {
  // Repair DB
  Status st;
  st = RepairDB(db_path_, options_);
  std::cout <<st.ToString() <<std::endl;
  
}

// ----------------------------------------------------------------------------

IteratorSeekCommand::IteratorSeekCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 2){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
    value_ = params.at(1);
  }
}

void IteratorSeekCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    iterator_[iterator_index]->Seek(Slice(value_));
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << iterator_[iterator_index]->key().ToString() << " " << iterator_[iterator_index]->value().ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorSeekToFirstCommand::IteratorSeekToFirstCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorSeekToFirstCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    iterator_[iterator_index]->SeekToFirst();
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << iterator_[iterator_index]->key().ToString() << " " << iterator_[iterator_index]->value().ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorSeekToLastCommand::IteratorSeekToLastCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorSeekToLastCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    iterator_[iterator_index]->SeekToLast();
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << iterator_[iterator_index]->key().ToString() << " " << iterator_[iterator_index]->value().ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorNextCommand::IteratorNextCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorNextCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    iterator_[iterator_index]->Next();
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << iterator_[iterator_index]->key().ToString() << " " << iterator_[iterator_index]->value().ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorPrevCommand::IteratorPrevCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorPrevCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    iterator_[iterator_index]->Prev();
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << iterator_[iterator_index]->key().ToString() << " " << iterator_[iterator_index]->value().ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorValidCommand::IteratorValidCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorValidCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    bool result = iterator_[iterator_index]->Valid();
    Status st = iterator_[iterator_index]->status();
    std::cout << st.ToString() << " : " << result <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

IteratorRefreshCommand::IteratorRefreshCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void IteratorRefreshCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    Status st = iterator_[iterator_index]->Refresh();
    std::cout << st.ToString() <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

NewIteratorCommand::NewIteratorCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  ParseReadOption(options, flags);
}

void NewIteratorCommand::DoCommand() {

  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  Slice lower;
  if (iterate_lower_bound_ !=""){
    lower = iterate_lower_bound_;
    read_options_.iterate_lower_bound = &lower;
    //std::cout <<"read_options_.iterate_lower_bound " <<read_options_.iterate_lower_bound->ToString()<< std::endl;
  }
  
  Slice upper;
  if (iterate_upper_bound_ !=""){
    upper = iterate_upper_bound_;
    read_options_.iterate_upper_bound = &upper;
    //std::cout <<"read_options_.iterate_upper_bound " <<read_options_.iterate_upper_bound->ToString()<< std::endl;
  }

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  Status st;
  Iterator* it = db_->NewIterator(read_options_);
  st = it->status();
  std::cout << st.ToString() << " : " << iterator_.size() <<std::endl;
  iterator_.push_back(it);
  exec_state_ = LDBCommandExecuteResult::ExeGetReleaseIterator(iterator_);
}


// ----------------------------------------------------------------------------

DeleteIteratorCommand::DeleteIteratorCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    key_ = params.at(0);
  }
}

void DeleteIteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseIterator DB
  int iterator_index = std::stoi(key_);
  if ((0 <= iterator_index) && (iterator_index < iterator_.size())){
    delete iterator_[iterator_index];
    std::cout <<"OK" << " : " << key_ <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: iterator_index == "<< key_ <<" iterator_.size() == "<< iterator_.size()<<std::endl;
  }
}
















