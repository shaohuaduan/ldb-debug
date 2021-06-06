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

using namespace leveldb;

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
const std::string LDBCommand::ARG_TIMESTAMP = "timestamp";
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
//std::shared_ptr<Cache> row_cache = nullptr;
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

const std::string LDBCommand:: ARG_VERIFY_CHECKSUMS = "verify_checksums";// bool verify_checksums = false; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_FILL_CACHE = "fill_cache";// bool fill_cache = true; leveldb/hyperleveldb ONLY
const std::string LDBCommand:: ARG_SNAPSHOT = "snapshot";// const Snapshot* snapshot = nullptr; leveldb/hyperleveldb ONLY

// -------------------------------------- write options --------------------------------------

const std::string LDBCommand:: ARG_SYNC = "sync";// bool sync = false; leveldb/hyperleveldb ONLY

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


LDBCommand* LDBCommand::InitCommand(const std::string cmd, DB* db, std::vector<const Snapshot*> snapshot, std::vector<Iterator*> iterator, std::vector< ReplayIterator*> replay_iterator)/*hyperleveldb ONLY*/ {
  
  std::vector<std::string> cmdSplits = StrSplit(cmd, ' ');
#ifdef DEBUG
  std::cout  << "(Ldb-Tool) Command: " << cmd << ", Command size: " << cmdSplits.size() <<std::endl;
#endif
  return InitCommand(cmdSplits, db, snapshot, iterator, replay_iterator, SelectCommand);
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
    std::vector<const Snapshot*> snapshot,
	std::vector<Iterator*> iterator,
    std::vector< ReplayIterator*> replay_iterator /*hyperleveldb ONLY*/,
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
    command->SetSnapshot(snapshot);
	command->SetIterator(iterator);
    command->SetReplayIterator(replay_iterator); //hyperleveldb ONLY
  }
  return command;
}

LDBCommand* LDBCommand::SelectCommand(const ParsedParams& parsed_params) {
  if (parsed_params.cmd == GetCommand::Name()) {
    return new GetCommand(parsed_params.cmd_params, parsed_params.option_map,
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
  }else if (parsed_params.cmd == DeleteDBCommand::Name()){
    return new DeleteDBCommand(parsed_params.cmd_params, parsed_params.option_map,
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
  }else if (parsed_params.cmd == NewIteratorCommand::Name()){
    return new NewIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == DeleteIteratorCommand::Name()){
    return new DeleteIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == LiveBackupCommand::Name()){
    return new LiveBackupCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetReplayTimestampCommand::Name()){
    return new GetReplayTimestampCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == AllowGarbageCollectBeforeTimestampCommand::Name()){
    return new AllowGarbageCollectBeforeTimestampCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == ValidateTimestampCommand::Name()){
    return new ValidateTimestampCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == CompareTimestampsCommand::Name()){
    return new CompareTimestampsCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == GetReplayIteratorCommand::Name()){
    return new GetReplayIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == ReleaseReplayIteratorCommand::Name()){
    return new ReleaseReplayIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == ReplayIteratorCommand::Name()){
    return new ReplayIteratorCommand(parsed_params.cmd_params, parsed_params.option_map,
                          parsed_params.flags);
  }else if (parsed_params.cmd == PutCommand::Name()){
    return new PutCommand(parsed_params.cmd_params, parsed_params.option_map,
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
  }else if (parsed_params.cmd == DeleteCommand::Name()){
    return new DeleteCommand(parsed_params.cmd_params, parsed_params.option_map,
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
bool LDBCommand::ParseOpenDBOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags){
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
  //size_t write_buffer_size = 4 * 1024 * 1024;
  itr = options.find(ARG_WRITE_BUFFER_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.write_buffer_size = std::stoul(val);
  }
  //int max_open_files = 1000;
  itr = options.find(ARG_MAX_OPEN_FILES);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_open_files = std::stoi(val);
  }
  //CompressionType = kSnappyCompression;
  itr = options.find(ARG_COMPRESSION);
  if (itr != options.end()) {
    std::string val = itr->second;
    if (val == ARG_K_SNAPPY_COMPRESSION) {
      options_.compression = kSnappyCompression;
    }else if (val == ARG_K_NO_COMPRESSION){
      options_.compression = kNoCompression;
    }else{
      std::cout <<"(Ldb-Tool) Invalid: CompressionType " << val << " NOT supported" << std::endl;
      invalid_command_ = true;
    }
  }
  //size_t block_size = 4 * 1024;
  itr = options.find(ARG_BLOCK_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.block_size = std::stoul(val);
  }
  //int block_restart_interval = 16;
  itr = options.find(ARG_BLOCK_RESTART_INTERVAL);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.block_restart_interval = std::stoi(val);
  }
  /*
  //size_t max_file_size = 2 * 1024 * 1024; leveldb ONLY
  itr = options.find(ARG_MAX_FILE_SIZE);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.max_file_size = std::stoul(val);
  }
  // bool reuse_logs = false; leveldb ONLY
  vitr = std::find(flags.begin(), flags.end(), ARG_REUSE_LOGS);
  if (vitr != flags.end()) {
    options_.reuse_logs = true;
  }
  itr = options.find(ARG_REUSE_LOGS);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.reuse_logs = StringToBool(val);
  }
  */
  // bool manual_garbage_collection = false;  hyperleveldb ONLY
  vitr = std::find(flags.begin(), flags.end(), ARG_MANUAL_GARBAGE_COLLECTION);
  if (vitr != flags.end()) {
    options_.manual_garbage_collection = true;
  }
  itr = options.find(ARG_MANUAL_GARBAGE_COLLECTION);
  if (itr != options.end()) {
    std::string val = itr->second;
    options_.manual_garbage_collection = StringToBool(val);
  }
  return true;
}

bool LDBCommand::ParseReadOption(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags){
  // bool verify_checksums = false; leveldb/hyperleveldb ONLY
  std::vector<std::string>::const_iterator vitr;
  vitr = std::find(flags.begin(), flags.end(), ARG_VERIFY_CHECKSUMS);
  if (vitr != flags.end()) {
    read_options_.verify_checksums = true;
  }
  std::map<std::string, std::string>::const_iterator itr;
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
  // const Snapshot* snapshot = nullptr; leveldb/hyperleveldb ONLY
  itr = options.find(ARG_SNAPSHOT);
  if (itr != options.end()) {
    std::string val = itr->second;
    snapshot_index_ = std::stoi(val);
  }else{
    snapshot_index_ = -1;
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
  // bool sync = false;
  std::vector<std::string>::const_iterator vitr;
  vitr = std::find(flags.begin(), flags.end(), ARG_SYNC);
  if (vitr != flags.end()) {
    write_options_.sync = true;
  }
  std::map<std::string, std::string>::const_iterator itr;
  itr = options.find(ARG_SYNC);
  if (itr != options.end()) {
    std::string val = itr->second;
    write_options_.sync = StringToBool(val);
  }
  return true;
}

// ----------------------------------------------------------------------------

OpenDBCommand::OpenDBCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseOpenDBOption(options, flags);
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
  Status st = db_->Put(write_options_, key_, value_);
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
  //Status st = db_->FlushWAL(true);
  //std::cout <<st.ToString()<<std::endl;
  std::cout <<"(Ldb-Tool) Invalid: flushwal is not supported by hyperleveldb" <<std::endl;
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
        output += it->key().ToString() + " " + it->value().ToString() + " ";
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
        output += it->key().ToString() + " " + it->value().ToString() + " ";
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
        output += it->key().ToString() + " " + it->value().ToString() + " ";
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
  Status st = db_->Write(write_options_, &batch);
  std::cout <<st.ToString()<<std::endl;
  //std::cout <<st.ToString()<<": code()= "<<st.code()<<" subcode()= "<<st.subcode()<<std::endl;
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

  if ((0 <= snapshot_index_) && (snapshot_index_ <snapshot_.size())){
    read_options_.snapshot = snapshot_[snapshot_index_];
  }else if (snapshot_index_ == -1){
    //read_options_.snapshot = nullptr;
  }
  else{
    std::cout <<"(Ldb-Tool) Invalid: snapshot_index == "<< snapshot_index_ <<" snapshot_.size() == "<< snapshot_.size()<<std::endl;
    return;
  }

  Status st = db_->Get(read_options_, key_, &value);
  //std::cout <<"key_: " <<key_<<std::endl;
  std::cout <<st.ToString()<< " : " <<value.c_str()<<std::endl;
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
  Status st = db_->Delete(write_options_, key_);
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
  std::cout <<"(Ldb-Tool) Invalid: singledelete is not supported by leveldb" <<std::endl;
  //Status st = db_->SingleDelete(write_options_, key_);
  //std::cout <<st.ToString()<<std::endl;
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

  // compact database
  db_->CompactRange(begin, end);
  //std::cout <<"OK"<<std::endl;
  std::cout <<"OK"<< " : " <<"begin="<< begin << ", end=" << end <<std::endl;
}

// ----------------------------------------------------------------------------

DestroyDBCommand::DestroyDBCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {
  
  ParseOpenDBOption(options, flags); 
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

  ParseOpenDBOption(options, flags);
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

LiveBackupCommand::LiveBackupCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    db_path_ = params.at(0);
  }
}

void LiveBackupCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // LiveBackup DB
  Status st;
  Slice db_path(db_path_);
  st = db_->LiveBackup(db_path);
  std::cout <<st.ToString() <<std::endl;
}

// ----------------------------------------------------------------------------

GetReplayTimestampCommand::GetReplayTimestampCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

}

void GetReplayTimestampCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // GetReplayTimestamp DB

  uint64_t ts_file, ts_seqno;
  std::string ts;
  db_->GetReplayTimestamp(&ts);
  Slice s1(ts);
  GetVarint64(&s1, &ts_file);
  GetVarint64(&s1, &ts_seqno);

  //std::cout <<"OK"<< " : " <<"timestamp="<< ts_uint64t <<std::endl;
  printf("OK : timestamp= %lu_%lu\n", ts_file, ts_seqno);
}

// ----------------------------------------------------------------------------

AllowGarbageCollectBeforeTimestampCommand::AllowGarbageCollectBeforeTimestampCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    timestamp_ = params.at(0);
  }
}

void AllowGarbageCollectBeforeTimestampCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  std::string ts;
  if (timestamp_ == "now" || timestamp_ == "all" ){
    ts = timestamp_;
  }else{
    std::vector<std::string> splits = StrSplit(timestamp_, '_');
    if (splits.size() == 2) {
      uint64_t ts_file = std::stoull(splits[0]);
      uint64_t ts_seqno = std::stoull(splits[1]);
      PutVarint64(&ts, ts_file);
      PutVarint64(&ts, ts_seqno);
    } else {
      std::cout <<"(Ldb-Tool) Invalid: timestamp_ " << timestamp_ <<std::endl;
      return;
    }
  }

  db_->AllowGarbageCollectBeforeTimestamp(ts);
  std::cout <<"OK"<< " : " <<"timestamp="<< ts <<std::endl;
}

// ----------------------------------------------------------------------------

ValidateTimestampCommand::ValidateTimestampCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    timestamp_ = params.at(0);
  }
}

void ValidateTimestampCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ValidateTimestamp DB
  std::string ts;
  uint64_t ts_uint64t = 12;
  if (timestamp_ == "now" || timestamp_ == "all" ){
    ts = timestamp_;
  }else{
    std::vector<std::string> splits = StrSplit(timestamp_, '_');
    if (splits.size() == 2) {
      uint64_t ts_file = std::stoull(splits[0]);
      uint64_t ts_seqno = std::stoull(splits[1]);
      PutVarint64(&ts, ts_file);
      PutVarint64(&ts, ts_seqno);
    } else {
      std::cout <<"(Ldb-Tool) Invalid: timestamp_ " << timestamp_ <<std::endl;
      return;
    }
  }

  bool result = db_->ValidateTimestamp(ts);
  //std::cout <<"OK"<< " : " << result << " timestamp_: " << timestamp_<<std::endl;
  std::cout <<"OK"<< " : " << result <<std::endl;
}

// ----------------------------------------------------------------------------

CompareTimestampsCommand::CompareTimestampsCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 2){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  }else{
    timestamp1_ = params.at(0);
    timestamp2_ = params.at(1);
  }
}

void CompareTimestampsCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // CompareTimestamps DB
  
  std::string ts1;
  if (timestamp1_ == "now" || timestamp1_ == "all" ){
    ts1 = timestamp1_;
    //std::cout <<"ts1"<< " : " << ts1 <<std::endl;
  }else{
    std::vector<std::string> splits = StrSplit(timestamp1_, '_');
    if (splits.size() == 2) {
      uint64_t ts_file = std::stoull(splits[0]);
      uint64_t ts_seqno = std::stoull(splits[1]);
      PutVarint64(&ts1, ts_file);
      PutVarint64(&ts1, ts_seqno);
    } else {
      std::cout <<"(Ldb-Tool) Invalid: timestamp_ " << timestamp1_ <<std::endl;
      return;
    }
  }

  std::string ts2;
  if (timestamp2_ == "now" || timestamp2_ == "all" ){
    ts2 = timestamp2_;
    //std::cout <<"ts2"<< " : " << ts2 <<std::endl;
  }else{
    std::vector<std::string> splits = StrSplit(timestamp2_, '_');
    if (splits.size() == 2) {
      uint64_t ts_file = std::stoull(splits[0]);
      uint64_t ts_seqno = std::stoull(splits[1]);
      PutVarint64(&ts2, ts_file);
      PutVarint64(&ts2, ts_seqno);
    } else {
      std::cout <<"(Ldb-Tool) Invalid: timestamp_ " << timestamp2_ <<std::endl;
      return;
    }
  }

  int result = db_->CompareTimestamps(ts1, ts2);
  std::cout <<"OK"<< " : " << result <<std::endl;
}

// ----------------------------------------------------------------------------

GetReplayIteratorCommand::GetReplayIteratorCommand(const std::vector<std::string>& params,
                             const std::map<std::string, std::string>& options,
                             const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if(params.size() != 1){
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 1" <<std::endl;
    invalid_command_ = true;
  }else{
    timestamp_ = params.at(0);
  }
}

void GetReplayIteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // GetReplayIterator DB
  std::string ts;
  if (timestamp_ == "now" || timestamp_ == "all" ){
    ts = timestamp_;
  }else{
    std::vector<std::string> splits = StrSplit(timestamp_, '_');
    if (splits.size() == 2) {
      uint64_t ts_file = std::stoull(splits[0]);
      uint64_t ts_seqno = std::stoull(splits[1]);
      PutVarint64(&ts, ts_file);
      PutVarint64(&ts, ts_seqno);
    } else {
      std::cout <<"(Ldb-Tool) Invalid: timestamp_ " << timestamp_ <<std::endl;
      return;
    }
  }

  ReplayIterator* replay_iterator;
  Status st;
  st = db_->GetReplayIterator(ts, &replay_iterator);
  std::cout << st.ToString() << " : " << replay_iterator_.size() <<std::endl;
  replay_iterator_.push_back(replay_iterator);
  exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(replay_iterator_);
}

// ----------------------------------------------------------------------------

ReleaseReplayIteratorCommand::ReleaseReplayIteratorCommand(const std::vector<std::string>& params,
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

void ReleaseReplayIteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }
  // ReleaseReplayIterator DB
  int replay_iterator_index = std::stoi(key_);
  if ((0 <= replay_iterator_index) && (replay_iterator_index <replay_iterator_.size())){
    db_->ReleaseReplayIterator(replay_iterator_[replay_iterator_index]);
    std::cout <<"OK" << " : " << key_ <<std::endl;
    //exec_state_ = LDBCommandExecuteResult::ExeGetReleaseReplayIterator(replay_iterator_);
  }else{
    std::cout <<"(Ldb-Tool) Invalid: replay_iterator_index == "<< key_ <<" replay_iterator_.size() == "<< replay_iterator_.size()<<std::endl;
  }
}

// ----------------------------------------------------------------------------

ReplayIteratorCommand::ReplayIteratorCommand(const std::vector<std::string>& params,
                       const std::map<std::string, std::string>& options,
                       const std::vector<std::string>& flags)
    : LDBCommand(options, flags) {

  if (params.size() != 2) {
    std::cout <<"(Ldb-Tool) Invalid: params.size() != 2" <<std::endl;
    invalid_command_ = true;
  } else {
    replay_iterator_index_ = std::stoi(params.at(0));
    key_ = params.at(1);
  }
}

void ReplayIteratorCommand::DoCommand() {
  if (!db_) {
    std::cout <<"(Ldb-Tool) Invalid: db_ == nullptr" <<std::endl;
    return;
  }

  if ((0 <= replay_iterator_index_) && (replay_iterator_index_ <snapshot_.size())){
    ReplayIterator* it = replay_iterator_[replay_iterator_index_];
    std::string output = "";
    while(it->Valid()){
      if(it->HasValue()){
        output += it->key().ToString() + " " + it->value().ToString() + " ";
        //std::cout << it->key().ToString()<<" "<< it->value().ToString()<<" "<< std::endl;
      }else{
        output += "NO VALUE ";
      }
      it->Next();
    }
    Status st = it->status();
    std::cout <<st.ToString()<< " : " << output <<std::endl;

  }else{
    std::cout <<"(Ldb-Tool) Invalid: replay_iterator_index == "<< replay_iterator_index_ <<" replay_iterator_.size() == "<< replay_iterator_.size()<<std::endl;
  }
}
