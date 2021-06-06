##############################
## Leveldb Function Options ##
##############################

#bool create_if_missing = false;
#bool error_if_exists = false;
#bool paranoid_checks = false;
#bool reuse_logs = false;
#size_t write_buffer_size = 4 * 1024 * 1024;
#int max_open_files = 1000;
#CompressionType compression = kSnappyCompression;
#size_t block_size = 4 * 1024;
#int block_restart_interval = 16;
#size_t max_file_size = 2 * 1024 * 1024;
'''
LEVELDB_COMMAND_OPTIONS = {'opendb': ['create_if_missing','error_if_exists'], 'delete': ['sync',], 'deletedb': [None], 'put': ['sync',], 'get': ['verify_checksums', 'fill_cache' , 'snapshot' ], 'write': ['sync',], 'iterator': ['verify_checksums', 'fill_cache' , 'snapshot']}
LEVELDB_COMMAND_OPTIONS_DEFAULT = {'opendb': [['false',], ['false',]], 'delete': [['false',],], 'deletedb': [None], 'put': [['false',],], 'get': [['false',], ['true',] , ['-1',]], 'write': [['false',],], 'iterator': [['false',], ['true',] , ['-1',]]}

'''
LEVELDB_COMMAND_OPTIONS = {'opendb': [\
'create_if_missing',\
'error_if_exists',\
'paranoid_checks',\
'reuse_logs',\
'write_buffer_size',\
'max_open_files',\
'compression',\
'block_size',\
'block_restart_interval',\
'max_file_size'],\
'delete': ['sync',], 'deletedb': [None], 'put': ['sync',], 'get': ['verify_checksums', 'fill_cache' , 'snapshot' ], 'write': ['sync',], 'iterator': ['verify_checksums', 'fill_cache' , 'snapshot']}

LEVELDB_COMMAND_OPTIONS_DEFAULT = {'opendb': [\
['false',],\
['false',],\
['false',],\
['false',],\
['1024','1048576','16777216'],\
['4','10','100'],\
['kSnappyCompression','kNoCompression'],\
['256','1024','1048576'],\
['1','4','32'],\
['256','1024','1048576']],\
'delete': [['false',],], 'deletedb': [None], 'put': [['false',],], 'get': [['false',], ['true',] , ['-1',]], 'write': [['false',],], 'iterator': [['false',], ['true',] , ['-1',]]}

# Set of commands to be used in test generation.
# We currently support : open db, delete db, put, get, delete, iterator
LEVELDB_COMMAND_SET = (
    ('opendb', 'deletedb'), 
    ('opendb', 'deletedb'), 
    ('opendb', 'delete'), 
    ('opendb', 'iterator', 'put'), 
    ('opendb', 'iterator', 'put', 'get'), 
    ('opendb', 'deletedb', 'put', 'get', 'delete'),
    ('opendb', 'deletedb', 'put', 'get', 'delete', 'iterator'),
    ('opendb', 'deletedb', 'put', 'get', 'delete', 'iterator', 'write')
)


###################################
## Hyperleveldb Function Options ##
###################################

#bool create_if_missing = false;
#bool error_if_exists = false;
#bool paranoid_checks = false;
#bool manual_garbage_collection = false;
#size_t write_buffer_size = 4 * 1024 * 1024;
#int max_open_files = 1000;
#CompressionType compression = kSnappyCompression;
#size_t block_size = 4 * 1024;
#int block_restart_interval = 16;


HYPERLEVELDB_COMMAND_OPTIONS = {'opendb': [\
'create_if_missing',\
'error_if_exists',\
'paranoid_checks',\
'manual_garbage_collection',\
'write_buffer_size',\
'max_open_files',\
'compression',\
'block_size',\
'block_restart_interval'],\
'delete': ['sync',], 'deletedb': [None], 'put': ['sync',], 'get': ['verify_checksums', 'fill_cache' , 'snapshot' ], 'write': ['sync',], 'iterator': ['verify_checksums', 'fill_cache' , 'snapshot']}

HYPERLEVELDB_COMMAND_OPTIONS_DEFAULT = {'opendb': [\
['false',],\
['false',],\
['false',],\
['false',],\
['1024','1048576','16777216'],\
['4','10','100'],\
['kSnappyCompression','kNoCompression'],\
['256','1024','1048576'],\
['1','4','32']],\
'delete': [['false',],], 'deletedb': [None], 'put': [['false',],], 'get': [['false',], ['true',] , ['-1',]], 'write': [['false',],], 'iterator': [['false',], ['true',] , ['-1',]]}

# Set of commands to be used in test generation.
# We currently support : open db, delete db, put, get, delete, iterator
HYPERLEVELDB_COMMAND_SET = (
    ('opendb', 'deletedb'), 
    ('opendb', 'deletedb'), 
    ('opendb', 'write'), 
    ('opendb', 'iterator', 'put'), 
    ('opendb', 'delete', 'put', 'get'), 
    ('opendb', 'deletedb', 'put', 'get', 'delete'),
    ('opendb', 'deletedb', 'put', 'get', 'delete', 'iterator'),
    ('opendb', 'deletedb', 'put', 'get', 'delete', 'iterator', 'write')
)


##############################
## Rocksdb Function Options ##
##############################


# ROCKSDB_COMMAND_OPTIONS = {'opendb': ['create_if_missing','error_if_exists'], 'delete': [None], 'singledelete': [None], 'deletedb': [None], 'put': [None], 'get': [None], 'write': [None], 'flushwal': [None], 'iterator': [None], 'flush': [None]}
# ROCKSDB_COMMAND_OPTIONS_DEFAULT = {'opendb': [('false',),('false',)], 'delete': [None], 'singledelete': [None], 'deletedb': [None], 'put': [None], 'get': [None], 'write': [None], 'flushwal': [None], 'iterator': [None], 'flush': [None]}


ROCKSDB_COMMAND_OPTIONS = {'opendb': [\
'create_if_missing',\
'create_missing_column_families',\
'error_if_exists',\
'paranoid_checks',\
'use_fsync',\
'allow_mmap_reads',\
'allow_mmap_writes',\
'use_direct_reads',\
'use_direct_io_for_flush_and_compaction',\
'allow_fallocate',\
'is_fd_close_on_exec',\
'skip_log_error_on_recovery',\
'persist_stats_to_disk',\
'advise_random_on_open',\
'new_table_reader_for_compaction_inputs',\
'use_adaptive_mutex',\
'strict_bytes_per_sync',\
'enable_thread_tracking',\
'enable_pipelined_write',\
'unordered_write',\
'allow_concurrent_memtable_write',\
'enable_write_thread_adaptive_yield',\
'skip_stats_update_on_db_open',\
'skip_checking_sst_file_sizes_on_db_open',\
'allow_2pc',\
'fail_if_options_file_error',\
'dump_malloc_stats',\
'avoid_flush_during_recovery',\
'avoid_flush_during_shutdown',\
'allow_ingest_behind',\
'preserve_deletes',\
'two_write_queues',\
'manual_wal_flush',\
'atomic_flush',\
'avoid_unnecessary_blocking_io',\
'write_dbid_to_manifest', \
'max_open_files',\
'max_file_opening_threads',\
'max_total_wal_size',\
'delete_obsolete_files_period_micros',\
'max_background_jobs',\
'base_background_compactions',\
'max_background_compactions',\
'max_subcompactions',\
'max_background_flushes',\
'max_log_file_size',\
'log_file_time_to_roll',\
'keep_log_file_num',\
'recycle_log_file_num',\
'max_manifest_file_size',\
'table_cache_numshardbits',\
'WAL_ttl_seconds',\
'WAL_size_limit_MB',\
'manifest_preallocation_size',\
'stats_dump_period_sec',\
'stats_persist_period_sec',\
'stats_history_buffer_size',\
'db_write_buffer_size',\
'compaction_readahead_size',\
'random_access_max_buffer_size',\
'writable_file_max_buffer_size',\
'bytes_per_sync',\
'wal_bytes_per_sync',\
'delayed_write_rate',\
'max_write_batch_group_size_bytes',\
'write_thread_max_yield_usec',\
'write_thread_slow_yield_usec',\
'log_readahead_size'],\
'opendbwithttl': [None], 'openassecondary': [None], 'delete': [None], 'singledelete': [None], 'deletedb': [None], 'put': [None], 'get': [None], 'write': [None], 'flushwal': [None], 'iterator': [None], 'flush': [None], 'close': [None], 'closewithttl': [None] }


ROCKSDB_COMMAND_OPTIONS_DEFAULT = {'opendb': [\
('false',),\
('false',),\
('false',),\
('true',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('true',),\
('true',),\
('false',),\
('false',),\
('true',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('true',),\
('true',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('false',),\
('1024','10','1'),\
('1','6','160'),\
('1','1000','1048576'),\
('100000','36000000000'),\
('1','10'),\
('1','100','0'),\
('0','1','100'),\
('100','0','2'),\
('1','10','0'),\
('1','2','1000'),\
('1','2','100'),\
('0','1','10'),\
('0','1','1000'),\
('128','1024','1048576'),\
('0','1','100'),\
('1','2','100000'),\
('1','2','100000'),\
('256','1024','64'),\
('1','100','1048576'),\
('1','100','1048576'),\
('16','1048576','64'),\
('1','100','1048576'),\
('1','100','1048576'),\
('0','1','8388608'),\
('0','1','8388608'),\
('1','100','1048576'),\
('1','100','1048576'),\
('1','100','1048576'),\
('1','1024','8388608'),\
('1','10','8388608'),\
('1','2','8388608'),\
('1','2','8388608')],\
'opendbwithttl': [None], 'openassecondary': [None], 'delete': [None], 'singledelete': [None], 'deletedb': [None], 'put': [None], 'get': [None], 'write': [None], 'flushwal': [None], 'iterator': [None], 'flush': [None], 'close': [None], 'closewithttl': [None]}

'''
'max_open_files',\  #int  = -1;
'max_file_opening_threads',\ #int  = 16;
'max_total_wal_size',\ #uint64_t  = 0;
'delete_obsolete_files_period_micros',\ #uint64_t  = 6ULL * 60 * 60 * 1000000;
'max_background_jobs',\ #int  = 2;
'base_background_compactions',\ #int  = -1;
'max_background_compactions',\ #int  = -1;
'max_subcompactions',\ #uint32_t  = 1;
'max_background_flushes',\ #int  = -1;
'max_log_file_size',\ #size_t  = 0;
'log_file_time_to_roll',\ #size_t  = 0;
'keep_log_file_num',\ #size_t  = 1000;
'recycle_log_file_num',\ #size_t  = 0;
'max_manifest_file_size',\ #uint64_t  = 1024 * 1024 * 1024;
'table_cache_numshardbits',\ #int  = 6;
'WAL_ttl_seconds',\ #uint64_t  = 0;
'WAL_size_limit_MB',\ #uint64_t  = 0;
'manifest_preallocation_size',\ #size_t  = 4 * 1024 * 1024;
'stats_dump_period_sec',\ #unsigned int  = 600;
'stats_persist_period_sec',\ #unsigned int  = 600;
'stats_history_buffer_size',\ #size_t  = 1024 * 1024;
'db_write_buffer_size',\ #size_t  = 0;
'compaction_readahead_size',\ #size_t  = 0;
'random_access_max_buffer_size',\ #size_t  = 1024 * 1024;
'writable_file_max_buffer_size',\ #size_t  = 1024 * 1024;
'bytes_per_sync',\ #uint64_t  = 0;
'wal_bytes_per_sync',\ #uint64_t  = 0;
'delayed_write_rate',\ #uint64_t  = 0;
'max_write_batch_group_size_bytes',\ #uint64_t  = 1 << 20;
'write_thread_max_yield_usec',\ #uint64_t  = 100;
'write_thread_slow_yield_usec',\ #uint64_t  = 3;
'log_readahead_size',\ #size_t  = 0;
'''


# Set of commands to be used in test generation.
# We currently support : open db, delete db, put, get, delete, scan, write, flushwal, iterator
ROCKSDB_COMMAND_SET = (
    ('opendb', 'deletedb'), 
    ('opendb', 'deletedb'), 
    ('opendb', 'put'), 
    ('opendb', 'deletedb', 'put'), 
    ('opendb', 'write', 'put', 'iterator'), 
    ('opendb', 'write', 'put', 'delete', 'singledelete'), 
    ('opendb', 'deletedb', 'put', 'get', 'singledelete', 'delete'), 
    ('opendb', 'deletedb', 'put', 'get', 'singledelete', 'delete', 'write'),
    ('opendb', 'deletedb', 'put', 'get', 'singledelete', 'delete', 'write', 'flushwal'),
    ('opendb', 'deletedb', 'put', 'get', 'singledelete', 'delete', 'write', 'flushwal', 'iterator')
)

# Number of option combination in Rocksdb's opendb. Only for opendb and -1 means disable.
OPENDB_OPTION_COMBIN_DEPTH = 3

# Set of valid test width.
VALID_TEST_WIDTH = (2, 3, 4, 5, 6, 7, 8, 9)

# Set of db-system to be used in test generation.
VALID_TEST_TYPES = ('rocksdb', 'leveldb', 'hyperleveldb')

DB_PATH = 'test_db'

#DATA_PLAN = ('allSame', 'parDiffer')  
DATA_PLAN = ('parDiffer',)

# disable_manual_wal_flush_unordered_write : manual_wal_flush and unordered_write open db option do not set true at the same time
# PRUNE_STRATEGY = ('first_cmd_opendb', 'unique_first_opendb', 'unique_last_deletedb', 'enable_create_if_missing')
# PRUNE_STRATEGY = ('unique_first_opendb', 'unique_last_deletedb', 'enable_create_if_missing')
# PRUNE_STRATEGY = ('unique_first_opendb', 'enable_create_if_missing')
PRUNE_STRATEGY = ('unique_first_opendb', 'enable_create_if_missing', 'disable_manual_wal_flush_unordered_write')
# PRUNE_STRATEGY = ('unique_first_opendb',)
# PRUNE_STRATEGY = ('first_cmd_opendb', 'unique_first_opendb', 'enable_create_if_missing', 'disable_error_if_exists', 'second_cmd_put', 'disable_manual_wal_flush', 'put_get_delete_sequence', 'put_get_singledelete_sequence', 'unique_last_deletedb', 'unique_last_flushwal','second_cmd_put')