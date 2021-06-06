#!/usr/bin/env python3
# To run : python3 ldb_gen.py -l <seq_length> -w <seq_width> -d <demo : True|False>
# To run : python3 ldb_gen.py -l 4 -w 3 -d True
# To run : python3 ldb_gen.py -l 4 -w 2 -d True
import os
import re
import sys
import stat
import subprocess
import argparse
import time
import itertools
import json
import pprint
import collections
import threading
import random
from shutil import copyfile
from multiprocessing import Pool
# from progress.bar import FillingCirclesBar
from collections import Counter
# Import list of all cmd options
from common import (ROCKSDB_COMMAND_SET, ROCKSDB_COMMAND_OPTIONS, ROCKSDB_COMMAND_OPTIONS_DEFAULT)
from common import (LEVELDB_COMMAND_SET, LEVELDB_COMMAND_OPTIONS, LEVELDB_COMMAND_OPTIONS_DEFAULT)
from common import (HYPERLEVELDB_COMMAND_SET, HYPERLEVELDB_COMMAND_OPTIONS, HYPERLEVELDB_COMMAND_OPTIONS_DEFAULT)
# Import list of all test seting
from common import (VALID_TEST_WIDTH, VALID_TEST_TYPES, DB_PATH, DATA_PLAN, PRUNE_STRATEGY, OPENDB_OPTION_COMBIN_DEPTH)



def build_parser():
    parser = argparse.ArgumentParser(description='Automatic Ldb Command Line Generator v0.1')

    # global args
    parser.add_argument('--sequence_len', '-l', default='3', help='Total number of critical ops in the workload')
    parser.add_argument('--sequence_wid', '-w', default='2', help='Number of unique critical ops in the workload')
    parser.add_argument('--demo', '-d', default='False', help='Create a demo workload set?')
    parser.add_argument('--test_type', '-t', default='rocksdb', required=False, 
            help='Type of test to generate <{}>. (Default: Rocksdb)'.format("/".join(VALID_TEST_TYPES)))

    return parser


def print_setup(parsed_args):
    print('\n{: ^50s}'.format('Automatic Ldb Command Line Generator v0.1\n'))
    print('='*20, 'Setup' , '='*20, '\n')
    print('{0:20}  {1}'.format('Sequence length', parsed_args.sequence_len))
    print('{0:20}  {1}'.format('Sequence width', parsed_args.sequence_wid))
    print('{0:20}  {1}'.format('Demo', parsed_args.demo))
    print('{0:20}  {1}'.format('Test Type', parsed_args.test_type))
    print('\n', '='*48, '\n')


# Command line Pruning:
def isCmdPruning(cmd_line, test_type, pruning_strat):
    
    # the first Cmd line must be Opendb
    if 'first_cmd_opendb' in pruning_strat:
        if cmd_line[0] != 'opendb':
            return True

    # the first Cmd line must be unique opendb
    if 'unique_first_opendb' in pruning_strat:
        temp = Counter(cmd_line)
        if cmd_line[0] != 'opendb' or temp['opendb'] != 1:
            return True

    # the second Cmd line must be put
    if 'second_cmd_put' in pruning_strat:
        if len(cmd_line) > 1 and cmd_line[1] != 'put':
            return True

    # the iterator Cmd line must be two
    if 'two_iterator_cmd' in pruning_strat:
        count = 0
        if len(cmd_line) > 2:
            temp = Counter(cmd_line)
            if temp['iterator'] != 2:  
                return True

    # the last Cmd line must be iterator
    if 'last_iterator_cmd' in pruning_strat:
        if cmd_line[-1] != 'iterator':
            return True

    # the last Cmd line must be unique deletedb
    if 'unique_last_deletedb' in pruning_strat:
        temp = Counter(cmd_line)
        if cmd_line[-1] != 'deletedb' or temp['deletedb'] != 1:
            return True

    # the last Cmd line must be unique flushwal
    if 'unique_last_flushwal' in pruning_strat:
        temp = Counter(cmd_line)
        if cmd_line[-1] != 'flushwal' or temp['flushwal'] != 1:
            return True

    # the put get singledelete sequence Cmd line
    if 'put_get_singledelete_sequence' in pruning_strat:
        if len(cmd_line) < 4 or cmd_line[1] != 'put' or cmd_line[2] != 'get' or cmd_line[3] != 'singledelete':
            return True

    # the put get delete sequence Cmd line
    if 'put_get_delete_sequence' in pruning_strat:
        if len(cmd_line) < 4 or cmd_line[1] != 'put' or cmd_line[2] != 'get' or cmd_line[3] != 'delete':
            return True
    return False


# Helper to merge lists
def flatList(op_list):
    flat_list = list()
    if not isinstance(op_list, str):
        for sublist in op_list:
            if not isinstance(sublist, str):
                for item in sublist:
                    flat_list.append(item)
            else:
                flat_list.append(sublist)
    else:
        flat_list.append(op_list)

    return flat_list


# Load data object to ops in the Ldb-tool file.
def loadData(suit, data_plan):
    seek_order_list = [['seektofirst', 'next'], ['seektolast', 'prev']]
    for i in range(0, len(suit)):
        workload = list()
        key_int = 1
        value_int = 1
        kv_range = len(suit[i])*2
        for j in range(0, len(suit[i])):
            cmd_line = list()
            cmd_line = suit[i][j]

            if cmd_line[0] == 'iterator':
                seek_order = random.choice(seek_order_list)
                cmd_line.append(seek_order[0])
                cmd_line.append(seek_order[1])
            elif cmd_line[0] == 'write':
                kv1_int = random.randint(1, key_range)
                kv2_int = random.randint(1, key_range)
                kv3_int = random.randint(1, key_range)
                if kv1_int >= kv2_int:
                    for kv_int in range(kv1_int, kv2_int): 
                        key = 'key'+str(kv_int)
                        value = 'value'+str(kv_int)
                        cmd_line.append('put:' + key + ':' + value)
                else:
                    for kv_int in range(kv2_int, kv1_int): 
                        key = 'key'+str(kv_int)
                        value = 'value'+str(kv_int)
                        cmd_line.append('put:' + key + ':' + value)
                cmd_line.append('delete:' + str(kv3_int))    
            elif cmd_line[0] == 'get':
                kv_int = random.randint(1, key_range)
                key = 'key'+str(kv_int)
                cmd_line.append(key)
            elif cmd_line[0] == 'delete':
                kv_int = random.randint(1, key_range)
                key = 'key'+str(kv_int)
                cmd_line.append(key)
            elif cmd_line[0] == 'singledelete':
                kv_int = random.randint(1, key_range)
                key = 'key'+str(kv_int)
                cmd_line.append(key)
            elif cmd_line[0] == 'put':
                if data_plan == 'parDiffer':
                    key = 'key'+str(key_int)
                    value = 'value'+str(value_int)
                    cmd_line.append(key)
                    cmd_line.append(value)
                    key_int += 1
                    value_int += 1
                else:
                    key = 'key'+str(key_int)
                    value = 'value'+str(value_int)
                    cmd_line.append(key)
                    cmd_line.append(value)
                    key_int = 1
                    value_int += 1

            workload.append(cmd_line)
        suit.append(workload)
    return suit


# Creates the actual Ldb-tool file.
def formatLdbCmd(cmd_list, test_type):
    global global_count
    command_str = ''
    option_str  = ''
    
    command = cmd_list[0]
    options = cmd_list[1]
    opt_list = []
    default_list = []
    if test_type == VALID_TEST_TYPES[0]:
        opt_list = ROCKSDB_COMMAND_OPTIONS[command]
        default_list  = ROCKSDB_COMMAND_OPTIONS_DEFAULT[command]
    elif test_type == VALID_TEST_TYPES[1]:
        opt_list = LEVELDB_COMMAND_OPTIONS[command]
        default_list  = LEVELDB_COMMAND_OPTIONS_DEFAULT[command]
    elif test_type == VALID_TEST_TYPES[2]:
        opt_list = HYPERLEVELDB_COMMAND_OPTIONS[command]
        default_list  = HYPERLEVELDB_COMMAND_OPTIONS_DEFAULT[command]
    else:
        print("Invalid test type '{}'\nTest type must be one of <{}>".format(test_type, "/".join(VALID_TEST_TYPES)))
        sys.exit(1)

    opt_len = len(opt_list)
    
    # add options to a command, if there are options
    if opt_list[0] != None:
        for j in range(0, opt_len): 
            if j in options:
                if default_list[j][0] == 'true':
                    option_str = option_str + ' --{0}={1}'.format(opt_list[j], 'false')
                elif default_list[j][0] == 'false':
                    option_str = option_str + ' --{0}={1}'.format(opt_list[j], 'true')
                else:
                    random_default = random.choice(default_list[j])
                    option_str = option_str + ' --{0}={1}'.format(opt_list[j], random_default)
                    
    # assemble options, operations into a command.
    if command  in ('opendb',):
        db_folder = 'ldb-cmd' + str(global_count)
        db_full_path = os.path.join(DB_PATH, db_folder)
        command_str = command + option_str + ' ' + db_full_path

    elif command in ('put', 'iterator'):
        command_str = command + option_str + ' ' + cmd_list[2] + ' ' + cmd_list[3]

    elif command in ('get', 'delete', 'singledelete'):
        # print("cmd_list {}".format(cmd_list))
        command_str = command + option_str + ' ' + cmd_list[2]

    elif command in ('compactrange',):
        # print("cmd_list {}".format(cmd_list))
        command_str = command + ' ' + cmd_list[2] + ' ' + cmd_list[3]
        # compactrange nullptr nullptr , compactrange key1 key2

    elif command in ('write',):
        # print("cmd_list {}".format(cmd_list))
        command_str = command + option_str
        cmd_len = len(cmd_list)
        for j in range(2, cmd_len): 
            command_str = command_str + ' ' + cmd_list[j]
        # compactrange nullptr nullptr , compactrange key1 key2

    else:
        command_str = cmd_list[0]
        
    return command_str


# Cmd Options Pruning:
def isOptionPruning(cmd, option_combine_tuple, opt_list, pruning_strat):

    # option prune strategy, must enable opendb create_if_missing
    if 'enable_create_if_missing' in pruning_strat:
        if cmd == 'opendb' and 'create_if_missing' in opt_list:
            index_1 = opt_list.index('create_if_missing')
            if index_1 not in option_combine_tuple:
                return True

    # option prune strategy, must NOT enable opendb error_if_exists
    if 'disable_error_if_exists' in pruning_strat:
        if cmd == 'opendb' and 'error_if_exists' in opt_list:
            index_1 = opt_list.index('error_if_exists')
            if index_1 in option_combine_tuple:
                return True

    # option prune strategy, disable manual_wal_flush and unordered_write in the same time
    if 'disable_manual_wal_flush_unordered_write' in pruning_strat:
        if cmd == 'opendb' and 'manual_wal_flush' in opt_list and 'unordered_write' in opt_list:
            index_1 = opt_list.index('manual_wal_flush')
            index_2 = opt_list.index('unordered_write')
            if index_1 in option_combine_tuple and index_2 in option_combine_tuple:
                return True
    
    # option prune strategy, disable manual_wal_flush
    if 'disable_manual_wal_flush' in pruning_strat:
        if cmd == 'opendb' and 'manual_wal_flush' in opt_list:
            index_1 = opt_list.index('manual_wal_flush')
            if index_1 in option_combine_tuple:
                return True
    return False


# Given a restricted list of files, this function builds all combinations of input parameters to persistence operations.
# Once the parameters to core-ops are picked, it is not required to persist a file totally unrelated to the set of used files in the workload. So we can restrict the set of files for persistence to either related files(includes the parent and siblings of files used in the workload) or further restrict it to strictly pick from the set of used files only.
# We can optionally add a persistence point after each core-FS op, except for the last one. The last core-op must be followed by a persistence op, so that we don't truncate it to a workload of lower sequence.
def optionAssemble(cmd, test_type):
    cmd_options = []
    opt_list = []
    if test_type == VALID_TEST_TYPES[0]:
        opt_list = ROCKSDB_COMMAND_OPTIONS[cmd]
    elif test_type == VALID_TEST_TYPES[1]:
        opt_list = LEVELDB_COMMAND_OPTIONS[cmd]
    elif test_type == VALID_TEST_TYPES[2]:
        opt_list = HYPERLEVELDB_COMMAND_OPTIONS[cmd]
    else:
        print("Invalid test type '{}'\nTest type must be one of <{}>".format(test_type, "/".join(VALID_TEST_TYPES)))
        sys.exit(1)

    opt_len = len(opt_list)
    
    if opt_list[0] == None:
        return [None]
    
    if cmd == 'opendb' and opt_len >= OPENDB_OPTION_COMBIN_DEPTH and OPENDB_OPTION_COMBIN_DEPTH != -1:
        opt_index_list = [n for n in range(0, opt_len)]

        for option_combine_tuple in itertools.combinations(opt_index_list, OPENDB_OPTION_COMBIN_DEPTH):
        
            if isOptionPruning(cmd, option_combine_tuple, opt_list, PRUNE_STRATEGY):
                continue
        
            cmd_options.append(option_combine_tuple)

    else: # full permutation by binary integer, E.X.: 0b1011
        for i in range(0, pow(2, opt_len)):
            bin_str = bin(i) # 0b1011
            rev_bin_str = ''.join(reversed(bin_str)) # 0b1011 -> 1101b0
            index = 0
            index_list = []
            for j in rev_bin_str:
                if j == '1':
                    index_list.append(index)
                    # print("{} index_list: {}\n".format(cmd, index_list))
                index += 1
            option_combine_tuple = tuple(index_list)   
            if isOptionPruning(cmd, option_combine_tuple, opt_list, PRUNE_STRATEGY):
                continue
        
            cmd_options.append(option_combine_tuple)
            # print("{} cmd_options: {}\n".format(cmd, cmd_options))
    return cmd_options


# Given a restricted list of files, this function builds all combinations of input parameters to persistence operations.
# Once the parameters to core-ops are picked, it is not required to persist a file totally unrelated to the set of used files in the workload. So we can restrict the set of files for persistence to either related files(includes the parent and siblings of files used in the workload) or further restrict it to strictly pick from the set of used files only.
# We can optionally add a persistence point after each core-FS op, except for the last one. The last core-op must be followed by a persistence op, so that we don't truncate it to a workload of lower sequence.
def cmdOptionPerm(cmd_tuple, test_type): 

    option_list = list()
    cmd_option_list = list()
    cmd_len = len(cmd_tuple)
    for i in range(0, cmd_len):
        cmd_options = optionAssemble(cmd_tuple[i], test_type)
        option_list.append(cmd_options)

    if int(cmd_len) == 1:
        for i in itertools.product(option_list[0]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 2:
        for i in itertools.product(option_list[0], option_list[1]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 3:
        for i in itertools.product(option_list[0], option_list[1], option_list[2]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 4:
        for i in itertools.product(option_list[0], option_list[1], option_list[2], option_list[3]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 5:
        for i in itertools.product(option_list[0], option_list[1], option_list[2], option_list[3], option_list[4]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 6:
        for i in itertools.product(option_list[0], option_list[1], option_list[2], option_list[3], option_list[4], option_list[5]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 7:
        for i in itertools.product(option_list[0], option_list[1], option_list[2], option_list[3], option_list[4], option_list[5], option_list[6]):
            cmd_option_list.append(i)

    elif int(cmd_len) == 8:
        for i in itertools.product(option_list[0], option_list[1], option_list[2], option_list[3], option_list[4], option_list[5], option_list[6], option_list[7]):
            cmd_option_list.append(i)

    return cmd_option_list


# Main function that exhaustively generates combinations of ops.
def doPermutation(cmd, num_ops, unq_ops, test_type, data_plan):
    
    global global_count
    global log_file_handle
    if demo:
        log = '\ntest suit in PHASE 1 = {0}\n'.format(cmd)
        log_file_handle.write(log)
    
    # **PHASE 2** : Exhaustively populate parameters to the chosen skeleton in phase 1
    suit_p2 = list()
    option_perm_list = list()
    option_perm_list = cmdOptionPerm(cmd, test_type)
    for i in range(0, len(option_perm_list)):
        workload = list()
        for j in range(0, len(option_perm_list[i])):
            cmd_line = list()
            cmd_line.append(cmd[j])
            cmd_line.append(option_perm_list[i][j])
            workload.append(cmd_line)
        suit_p2.append(workload)
    if demo:
        log = '\ntest suit in PHASE 2 = {0}\n'.format(suit_p2)
        log_file_handle.write(log)

    # **PHASE 3** : Insert all data workload to the generated phase-2 workloads.
    suit_p3 = list()
    for i in data_plan:
        suit = loadData(suit_p2, i)
        suit_p3.extend(suit)
    if demo:
        log = '\ntest suit in PHASE 3 = {0}\n'.format(suit_p3)
        log_file_handle.write(log)
    
    # **PHASE 4** : Remove duplicated test workload.
    suit_p4 = list()
    for i in range(0, len(suit_p3)):
        workload = list()
        for j in range(0, len(suit_p3[i])):
            cmd_line = tuple(suit_p3[i][j])
            workload.append(cmd_line)
        workload_tuple = tuple(workload)
        suit_p4.append(workload_tuple)
    suit_p4 = list(set(suit_p4))
    if demo:
        log = '\ntest suit in PHASE 4 = {0}\n'.format(suit_p4)
        log_file_handle.write(log)

    # **PHASE 5** : Build the ldb-tool command line file.
    dest_dir = test_type + '_l'+str(num_ops)+'_w'+str(unq_ops)
    # Create a target directory for the final .cpp test files
    # The corresponding high-level language file for each test case can be found wthin the directory j-lang-files in the same target directory.
    if demo:
        log = '\ntest suit in PHASE 5 =\n'
        log_file_handle.write(log)

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    for i in range(0, len(suit_p4)):
        ldb_cmd_file = 'ldb-cmd' + str(global_count)
        dest_file = os.path.join(dest_dir, ldb_cmd_file)
        if demo:
            log = '\n***  ' + dest_file + '  ***\n\n'
            log_file_handle.write(log)
        with open(dest_file, 'a') as f:
            #format the ldb file head
            
            #cur_line = './ldb --db=' + DB_PATH
            #cur_line_format = '{0}'.format(cur_line) + '\n'
            #f.write(cur_line_format)
            #if demo:
            #    log = '\t' + cur_line_format
            #    log_file_handle.write(log)

            for j in range(0, len(suit_p4[i])):
                #format the ldb file body
                cur_line = formatLdbCmd(suit_p4[i][j], test_type)
                cur_line_format = '{0}'.format(cur_line) + '\n'
                f.write(cur_line_format)
                if demo:
                    log = '\t' + cur_line_format
                    log_file_handle.write(log)
        f.close()
        global_count += 1
        print('\rglobal_count: {0}'.format(global_count),end='')

'''
class SlowBar(FillingCirclesBar):
    suffix = '%(percent).0f%%  (Completed %(index)d skeletons with %(global_count)d workloads)'
    @property
    def global_count(self):
        return global_count
'''

global_count = 0
log_file_handle = 0
demo = False

def main():
    
    global global_count
    global log_file_handle
    global demo
    num_ops = 0
    unq_ops = 0

    # open log file
    log_file = time.strftime('%Y%m%d_%H%M%S') + '-WorkloadGen.log'
    log_file_handle = open(log_file, 'w')
    
    # Parse input args
    parsed_args = build_parser().parse_args()
    
    # Print the test setup - just for sanity
    print_setup(parsed_args)
    
    # Parse input args
    test_type = parsed_args.test_type
    if test_type not in VALID_TEST_TYPES:
        print("Invalid test type '{}'\nTest type must be one of <{}>".format(test_type, "/".join(VALID_TEST_TYPES)))
        sys.exit(1)
    
    if int(parsed_args.sequence_wid) in VALID_TEST_WIDTH:
        unq_ops = int(parsed_args.sequence_wid)
    else:
        unq_ops = 2

    test_cmd_set = set()
    if test_type == VALID_TEST_TYPES[0]:
        test_cmd_set = ROCKSDB_COMMAND_SET[unq_ops]
    elif test_type == VALID_TEST_TYPES[1]:
        test_cmd_set = LEVELDB_COMMAND_SET[unq_ops]
    elif test_type == VALID_TEST_TYPES[2]:
        test_cmd_set = HYPERLEVELDB_COMMAND_SET[unq_ops]
    else:
        print("Invalid test type '{}'\nTest type must be one of <{}>".format(test_type, "/".join(VALID_TEST_TYPES)))
        sys.exit(1)

    if int(parsed_args.sequence_len) >= 2:
        num_ops = int(parsed_args.sequence_len)
    else:
        num_ops = 2

    if parsed_args.demo == ('True' or 'true'):
        demo = True
    else:
        demo = False

    # This is the number of unique operations
    log = '{0} operations width = {1}, {2}\n'.format(test_type, len(test_cmd_set), test_cmd_set)
    print(log)
    log_file_handle.write(log)
    
    # This is the total number of operations
    log = test_type + ' operations length = ' + str(num_ops) + '\n'
    print(log)
    log_file_handle.write(log)
    
    #  The pruning stratagy used
    log = 'Pruning stratagy = {0}\n'.format(PRUNE_STRATEGY)
    print(log)
    log_file_handle.write(log)

    #  The workload data plan used
    log = 'Workload data plan = {0}\n'.format(DATA_PLAN)
    print(log)
    log_file_handle.write(log)

    # Time workload generation
    start_time = time.time()

    # **PHASE 1** : Create all permutations of file-system operations of given sequence lengt. ops can repeat.
    # To create only permutations of ops with no repeptions allowed, use this
    totalOpPermutations = len(test_cmd_set) ** int(num_ops)
    # Workloads can take really long to generate. So let's create a progress bar.
    # bar = FillingCirclesBar('Generating workloads.. ', max=totalOpCombinations, suffix='%(percent).0f%%  (Completed %(index)d/%(global_count)d)')
    # bar = SlowBar('Generating workloads.. ', max=totalOpPermutations)

    # This is the number of input operations
    log = 'Total Phase-1 Skeletons = ' + str(totalOpPermutations) + '\n'
    print(log)
    log_file_handle.write(log)
    pruning_count = 0
    # bar.start()
    for i in itertools.product(test_cmd_set, repeat=int(num_ops)):
        if isCmdPruning(i, test_type, PRUNE_STRATEGY):
            pruning_count += 1
            if demo:
                log = '\npruned suit # {0} in PHASE 1 = {1}\n'.format(pruning_count, i)
                log_file_handle.write(log)
            continue
        # print("Cmd Permutation: {}\n".format(i))
        doPermutation(i, num_ops, unq_ops, test_type, DATA_PLAN)
        # bar.next()

    # To parallelize workload generation, we will need to modify how we number the workloads. If we enable this as it is, the threads will overwrite workloads, as they don't see the global_count.
    # pool = Pool(processes = 4)
    # pool.map(doPermutation, itertools.permutations(test_cmd_set, int(num_ops)))
    # pool.close()

    # End timer
    end_time = time.time()
    # bar.finish()

    # This is the total number of workloads pruned
    log = '\nTotal workloads pruned in PHASE 1 = '  + str(pruning_count)  + '\n'
    print(log)
    log_file_handle.write(log)

    # This is the total number of workloads generated
    log = '\nTotal workloads generated = '  + str(global_count)  + '\n'
    print(log)
    log_file_handle.write(log)

    # Time to create the above workloads
    log = 'Time taken to generate workloads = ' + str(round(end_time-start_time,2)) + ' seconds\n'
    print(log)
    log_file_handle.write(log)

    # Print target directory
    dest_dir = test_type + '_l'+str(num_ops)+'_w'+str(unq_ops)
    log = 'Generated workloads can be found at ' + dest_dir + '\n'
    print(log)
    log_file_handle.write(log)

    log_file_handle.close()


if __name__ == '__main__':
    main()
