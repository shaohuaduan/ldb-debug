#!/usr/bin/env python3

#To run : python3 ldb_run.py -t rocksdb_l4_w2 -d True
#To run : python3 ldb_run.py -t rocksdb_l4_w2 -d True -s ldb-cmd90 -r 10
import os
import re
import sys
import stat
import subprocess
import argparse
import time
import itertools
from shutil import copyfile
from subprocess import STDOUT, Popen, call

#All functions that has options go here
from common import ( VALID_TEST_TYPES, DB_PATH)


def build_parser():
    parser = argparse.ArgumentParser(description='Test Execution for Ldb-tool v1.0')

    # global args
    parser.add_argument('--test_path', '-p', default='', help='Directory of the test files')
    parser.add_argument('--demo', '-d', default='False', help='Create a demo test running time?')
    parser.add_argument('--start_file', '-s', default='0', required=False, help='Begin of test cases')
    parser.add_argument('--range_file', '-r', default='0', required=False, help='Range of test cases')
    parser.add_argument('--test_type', '-t', default='rocksdb', required=False, help='Type of test to generate <{}>. (Default: rocksdb)'.format("/".join(VALID_TEST_TYPES)))
    return parser


def print_setup(parsed_args):
    print('\n{: ^50s}'.format('Ldb-tool Workload Execution v0.1\n'))
    print('='*20, 'Setup' , '='*20, '\n')
    print('{0:20}  {1}'.format('Test path', parsed_args.test_path))
    print('{0:20}  {1}'.format('Demo', parsed_args.demo))
    print('{0:20}  {1}'.format('Test Type', parsed_args.test_type))
    print('{0:20}  {1}'.format('Begin test case', parsed_args.start_file))
    print('{0:20}  {1}'.format('Range test case', parsed_args.range_file))
    

    print('\n', '='*48, '\n')


def print_log(log, demo, is_print = False):
    global log_file_handle
    if demo:
        print(log)
        #time.sleep(2)
    elif is_print:
        print(log)
    # This is the number of unique operations
    log_file_handle.write(log)


def data_verify(kv_map, db_folder, test_type):
    global demo
    fail_flag = False
    print_log('kv_map: {0}\n'.format(kv_map), demo)
    #print_log('reboot Ldb-tool process.\n', demo)

    # Remove DB LOCK file
    db_full_path = os.path.join(DB_PATH, db_folder)
    lock_file = os.path.join(db_full_path, 'LOCK')
    if os.path.exists(lock_file):
        remove = 'rm {0}\n'.format(lock_file)
        print_log(remove, demo)
        subprocess.call(remove, shell = True)
    
    p = subprocess.Popen(["./ldb"], stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=STDOUT,shell=True)
    cmd_line = 'opendb {0}\n'.format(db_full_path)
    print_log('>>>>   {0}'.format(cmd_line), demo)
    p.stdin.write(cmd_line.encode("GBK"))
    try:
        p.stdin.flush()
    except BrokenPipeError:
        pass
        print_log('BrokenPipeError\n', demo)
    # p.stdin.flush()


    output = p.stdout.readline().decode("GBK")
    print_log(output, demo)
    if 'does not exist' not in output:
        # fail case: can NOT opendb
        if output.strip() != 'OK':
            fail_flag = True
        else:
            for key in kv_map:
                cmd_line = 'get {0}\n'.format(key)
                print_log('>>>>   {0}'.format(cmd_line), demo)
                p.stdin.write(cmd_line.encode("GBK"))
                try:
                    p.stdin.flush()
                except BrokenPipeError:
                    pass
                    print_log('BrokenPipeError\n', demo)
                # p.stdin.flush()

                output = p.stdout.readline().decode("GBK")

                print_log(output, demo)
                output_status = output.split(':')[0].strip()
                output_value = ''
                if len(output.split(':')) > 2 :
                    output_value = output.split(':')[1].strip()
                # correct case 1: data key-value is deleted and should return 'NOT Found' status
                if kv_map[key] == 'None' and (output_status == 'NotFound' or output_status == 'Key not found'): 
                    pass
                # correct case 2: data key-value is inserted and can access to and is correct value in db
                elif kv_map[key] != 'None' and output_status == 'OK' and output_value == kv_map[key]: 
                    pass
                # other fail cases:
                else:
                    fail_flag = True

                if test_type == VALID_TEST_TYPES[3]: # badgerdb
                    cmd_line = 'close\n'
                else: # rocksdb, leveldb, hyperleveldb
                    cmd_line = 'deletedb\n'
            print_log('>>>>   {0}'.format(cmd_line), demo)
            p.stdin.write(cmd_line.encode("GBK"))
            try:
                p.stdin.flush()
            except BrokenPipeError:
                pass
                print_log('BrokenPipeError\n', demo)
            # p.stdin.flush()


            output = p.stdout.readline().decode("GBK")

            print_log(output, demo)

    cmd_line = 'exit\n'
    print_log('>>>>   {0}'.format(cmd_line), demo)
    p.stdin.write(cmd_line.encode("GBK"))
    try:
        p.stdin.flush()
    except BrokenPipeError:
        pass
        print_log('BrokenPipeError\n', demo)
    # p.stdin.flush()
    # p.kill()
    p.wait()
    return fail_flag


def get_verify_data(cmd_line, kv_map, kv_map_snapshot):
    snapshot_key = ''
    kv_verify = {}
    #print('{0}  {1}'.format('get_verify_data.kv_map_snapshot: ', kv_map_snapshot))
    #print('{0}  {1}'.format('get_verify_data.cmd_line: ', cmd_line))
    #print('{0}  {1}'.format('get_verify_data.kv_map: ', kv_map))
    for option in cmd_line.split(' '):
        if '--snapshot=' in option:
            snapshot_key = option.split('=')[1].strip()
            # print('{0}  {1}'.format('get_verify_data.snapshot_key: ', snapshot_key))
    if snapshot_key == '' or snapshot_key == '-1':
        kv_verify = kv_map
    else:
        kv_verify = kv_map_snapshot[snapshot_key]
    # print('{0}  {1}'.format('get_verify_data.kv_verify: ', kv_verify))
    return kv_verify



log_file_handle = 0
demo = False

def main():

    global log_file_handle
    global demo
    
    # Open log file
    log_file = time.strftime('%Y%m%d_%H%M%S') + '-WorkloadRun.log'
    log_file_handle = open(log_file, 'w')

    # Parse input args
    parsed_args = build_parser().parse_args()
    
    # Print the test setup - just for sanity
    print_setup(parsed_args)


    if parsed_args.demo == ('True' or 'true'):
        demo = True
    else:
        demo = False

    test_path = parsed_args.test_path
    # Check if test path/file exists
    if not os.path.exists(test_path):
        print(test_path + ' : No such test directory or test file\n')
        exit(1)
    
    # Parse input args
    start_file = parsed_args.start_file
    range_file = int(parsed_args.range_file)

    test_type = parsed_args.test_type
    if test_type not in VALID_TEST_TYPES:
        print("Invalid test type '{}'\nTest type must be one of <{}>".format(test_type, "/".join(VALID_TEST_TYPES)))
        sys.exit(1)

    # Init report varient for the test suite
    fail_count = 0
    total_count = 0
    fail_list = []
    error_count = 0
    error_list = []
    status_list = []

    # Initiating test: Remove DB directory and SST files
    print_log('Initiating test ...\n', demo, True)
    if os.path.exists(DB_PATH):
        remove = 'rm -rf {0}\n'.format(DB_PATH)
        subprocess.call(remove, shell = True)
        print_log(remove, demo, True)
    mkdir = 'mkdir {0}\n'.format(DB_PATH)
    subprocess.call(mkdir, shell = True)
    print_log(mkdir, demo, True)


    # test path for mulitple test case files
    if os.path.isdir(test_path):
        # Open test suite directory and list all test case files
        files=os.listdir(test_path)
        files.sort()
        start_index = 0
        stop_index = len(files)
        if stop_index == 0:
            print(test_path + ' : No test file in directory\n')
            exit(1)
        
        if start_file in files:
            start_index = files.index(start_file)

        if 0 < range_file and start_index + range_file < stop_index:
            stop_index = start_index + range_file
            
        print_log('Total Test: {0} start_file(index): {1} ({2}), last_file(index): {3} ({4})\n'.format(stop_index - start_index, files[start_index], start_index, files[stop_index-1], stop_index-1), demo, True)
        print_log('Running test ...\n', demo, True)

        for file_index in range(start_index, stop_index): 
            workload_file = os.path.join(test_path, files[file_index])
            if os.path.isfile(workload_file):
                if demo:
                    print_log('\n####  {0}  ####\n'.format(files[file_index]), demo)
                else:
                    print('\r####  {0}  ####'.format(files[file_index]), end='')
                # Start Ldb-tool process
                p = subprocess.Popen(["./ldb"], stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=STDOUT,shell=True)
                # Init data varient for one test case
                total_count += 1
                kv_map = {}
                kv_map_snapshot = {}
                with open(workload_file, 'r') as f:
                    error_flag = False
                    for cmd_line in f:
                        print_log('\n>>>>   {0}'.format(cmd_line), demo)
                        p.stdin.write(cmd_line.encode("GBK"))
                        try:
                            p.stdin.flush()
                        except BrokenPipeError:
                            pass
                            print_log('\nBrokenPipeError\n', demo)
                        # p.stdin.flush()

                        output = p.stdout.readline().decode("GBK")
                        print_log('\n{0}'.format(output), demo)
                        status = output.split(':')[0].strip()
                        output_len = len(output.strip().split(':'))
                        # print('output_len:  {0} \n'.format(output_len))
                        # data NotFound for iterator
                        if cmd_line.split(' ')[0].strip() == 'iterator' and status == 'OK' and output_len == 1:
                            status = 'NotFound'

                        if status == 'OK':
                            #print('{0}  {1}'.format('cmd_line.split(' ')[0]: ', cmd_line.split(' ')[0]))
                            # put data collection for data_verify()
                            if cmd_line.split(' ')[0].strip() == 'put':
                                key = cmd_line.split(' ')[-2]
                                value = cmd_line.split(' ')[-1]
                                kv_map[key] = value.rstrip()
                            # delete/singledelete data collection for data_verify()
                            elif cmd_line.split(' ')[0] == 'delete' or cmd_line.split(' ')[0] == 'singledelete':
                                key = cmd_line.split(' ')[-1].rstrip()
                                kv_map[key] = 'None'
                            # write data collection for data_verify()
                            elif cmd_line.split(' ')[0].strip() == 'write':
                                sub_cmd_list = cmd_line.split(' ')
                                for sub_cmd in sub_cmd_list:
                                    if 'put:' in sub_cmd:
                                        key = sub_cmd.split(':')[-2]
                                        value = sub_cmd.split(':')[-1]
                                        kv_map[key] = value.rstrip()
                                    elif 'delete:' in sub_cmd:   
                                        key = cmd_line.split(':')[-1].rstrip()
                                        kv_map[key] = 'None'                                                             
                            # get data verify
                            elif cmd_line.split(' ')[0].strip() == 'get':
                                kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                                key = cmd_line.split(' ')[-1].rstrip()
                                value = output.split(':')[1].strip()
                                # wrong get case 1: data key-value is NOT correct.
                                if key not in kv_verify or kv_verify[key] != value:
                                    # print('{0}  {1}'.format('key: ', key))
                                    # print('{0}  {1}'.format('kv_verify: ', kv_verify))
                                    error_flag = True
                                    status_list.append('incorrect value')
                                    
                            # iterator data verify
                            elif cmd_line.split(' ')[0].strip() == 'iterator':
                                kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                                result = output.split(':')[1].strip()
                                kv_result = result.split(' ')
                                kv_map_result = {}
                                for i in range(0, len(kv_result), 2):
                                    kv_map_result[kv_result[i].rstrip()] = kv_result[i+1].rstrip()
                                kv_verify_size = 0
                                for key in kv_verify:
                                    if kv_verify[key] != 'None':
                                        kv_verify_size += 1
                                        if key in kv_map_result and kv_map_result[key] == kv_verify[key]:
                                            pass
                                        else:
                                            error_flag = True
                                            status_list.append('incorrect value')
                                if len(kv_map_result) != kv_verify_size:
                                    error_flag = True
                                    status_list.append('incorrect value')

                                # iterator data order verify
                                key_prev = ''
                                first = True
                                for key in kv_map_result:
                                    if first:
                                        key_prev = key
                                        first = False
                                        continue

                                    if (cmd_line.split(' ')[-1].strip() == 'next' or cmd_line.split(' ')[-1].strip() == 'validforprefix' or cmd_line.split(' ')[-1].strip() == 'valid') and key_prev < key:
                                        pass
                                    elif cmd_line.split(' ')[-1].strip() == 'prev' and key_prev > key:
                                        pass
                                    else:
                                        error_flag = True
                                        status_list.append('incorrect order')
                                    
                                    key_prev = key
                            # getsnapshot
                            elif cmd_line.split(' ')[0].strip() == 'getsnapshot':
                                snapshot_key = output.split(':')[1].strip()
                                kv_map_snapshot[snapshot_key] = kv_map.copy()
                                #print('{0}  {1}'.format('kv_map_snapshot: ', kv_map_snapshot))
                            # releasesnapshot
                            elif cmd_line.split(' ')[0].strip() == 'releasesnapshot':
                                snapshot_key = cmd_line.split(' ')[1].strip()
                                kv_map_snapshot[snapshot_key] = {}
                        elif status  == 'NotFound':
                            kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                            # get data verify
                            if cmd_line.split(' ')[0].strip() == 'get':
                                key = cmd_line.split(' ')[-1].rstrip()
                                if key in kv_verify and kv_verify[key] != 'None':
                                    error_flag = True
                                    status_list.append(status)
                            # iterator data verify
                            elif cmd_line.split(' ')[0].strip() == 'iterator':
                                for key in kv_verify:
                                    if kv_verify[key] != 'None':
                                        error_flag = True
                                if error_flag:
                                   status_list.append(status) 
                            # other cmd data verify
                            else:
                                error_flag = True
                                status_list.append(status)
                        else:
                            error_flag = True
                            status_list.append(status)
                f.close()

                print_log('Crashing test (kill -9 ldb) ...\n', demo)
                cmd_line = 'exit\n'
                print_log('>>>>   {0}'.format(cmd_line), demo)
                p.stdin.write(cmd_line.encode("GBK"))
                try:
                    p.stdin.flush()
                except BrokenPipeError:
                    pass
                    print_log('BrokenPipeError\n', demo)
                # p.stdin.flush()
                # p.kill()
                p.wait()
                
                print_log('Checking error status and consistency ...\n', demo)
                if error_flag == True:
                    
                    print_log('{0} has error status!!!\n'.format(files[file_index]), demo)
                    error_count += 1
                    error_list.append(files[file_index])

                if data_verify(kv_map, files[file_index], test_type):
                    print_log('{0} has NOT crash consistency!!!\n'.format(files[file_index]), demo)
                    fail_count += 1
                    fail_list.append(files[file_index])
    else:
        print_log('Running test ...\n', demo, True)
        # Open one test case file
        print_log('\n####  {0}  ####\n'.format(test_path), demo, True)
        # Start Ldb-tool process
        p = subprocess.Popen(["./ldb"], stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=STDOUT,shell=True)
        # Init data varient for one test case
        total_count += 1
        kv_map = {}
        kv_map_snapshot = {}
        with open(test_path, 'r') as f:
            error_flag = False
            for cmd_line in f:
                print_log('>>>>   {0}'.format(cmd_line), demo)
                p.stdin.write(cmd_line.encode("GBK"))
                try:
                    p.stdin.flush()
                except BrokenPipeError:
                    pass
                    print_log('BrokenPipeError\n', demo)
                # p.stdin.flush()
                
                output = p.stdout.readline().decode("GBK")
                
                print_log(output, demo)
                status = output.split(':')[0].strip()

                if status == 'OK':
                    # put data collection for data_verify()
                    if cmd_line.split(' ')[0].strip() == 'put':
                        key = cmd_line.split(' ')[-2]
                        value = cmd_line.split(' ')[-1]
                        kv_map[key] = value.rstrip()
                    # delete/singledelete data collection for data_verify()
                    elif cmd_line.split(' ')[0] == 'delete' or cmd_line.split(' ')[0] == 'singledelete':
                        key = cmd_line.split(' ')[-1].rstrip()
                        kv_map[key] = 'None'
                    # write data collection for data_verify()
                    elif cmd_line.split(' ')[0].strip() == 'write':
                        sub_cmd_list = cmd_line.split(' ')
                        for sub_cmd in sub_cmd_list:
                            if 'put:' in sub_cmd:
                                key = sub_cmd.split(':')[-2]
                                value = sub_cmd.split(':')[-1]
                                kv_map[key] = value.rstrip()
                            elif 'delete:' in sub_cmd:   
                                key = cmd_line.split(':')[-1].rstrip()
                                kv_map[key] = 'None'                                                             
                    # get data verify
                    elif cmd_line.split(' ')[0].strip() == 'get':
                        kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                        key = cmd_line.split(' ')[-1].rstrip()
                        value = output.split(':')[1].strip()
                        # wrong get case 1: data key-value is NOT correct.
                        if key not in kv_verify or kv_verify[key] != value: 
                            error_flag = True
                            status_list.append('incorrect value')
                                    
                    # iterator data verify
                    elif cmd_line.split(' ')[0].strip() == 'iterator':
                        kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                        result = output.split(':')[1].strip()
                        kv_result = result.split(' ')
                        kv_map_result = {}
                        for i in range(0, len(kv_result), 2):
                            kv_map_result[kv_result[i].rstrip()] = kv_result[i+1].rstrip()
                        kv_verify_size = 0
                        for key in kv_verify:
                            if kv_verify[key] != 'None':
                                kv_verify_size += 1
                                if key in kv_map_result and kv_map_result[key] == kv_verify[key]:
                                    pass
                                else:
                                    error_flag = True
                                    status_list.append('incorrect value')
                        if len(kv_map_result) != kv_verify_size:
                            error_flag = True
                            status_list.append('incorrect value')

                        # iterator data order verify
                        key_prev = ''
                        first = True
                        for key in kv_map_result:
                            if first:
                                key_prev = key
                                first = False
                                continue

                            if (cmd_line.split(' ')[-1].strip() == 'next' or cmd_line.split(' ')[-1].strip() == 'validforprefix' or cmd_line.split(' ')[-1].strip() == 'valid') and key_prev < key:
                                pass
                            elif cmd_line.split(' ')[-1].strip() == 'prev' and key_prev > key:
                                pass
                            else:
                                error_flag = True
                                status_list.append('incorrect order')
                                    
                            key_prev = key
                    # getsnapshot
                    elif cmd_line.split(' ')[0].strip() == 'getsnapshot':
                        snapshot_key = output.split(':')[1].strip()
                        kv_map_snapshot[snapshot_key] = kv_map.copy()
                    # releasesnapshot
                    elif cmd_line.split(' ')[0].strip() == 'releasesnapshot':
                        snapshot_key = cmd_line.split(' ')[1].strip()
                        kv_map_snapshot[snapshot_key] = {}
                elif status  == 'NotFound':
                    kv_verify = get_verify_data(cmd_line, kv_map, kv_map_snapshot)
                    # get data verify
                    if cmd_line.split(' ')[0].strip() == 'get':
                        key = cmd_line.split(' ')[-1].rstrip()
                        if key in kv_verify and kv_verify[key] != 'None':
                            error_flag = True
                            status_list.append(status)
                    # iterator data verify
                    elif cmd_line.split(' ')[0].strip() == 'iterator':
                        for key in kv_verify:
                            if kv_verify[key] != 'None':
                                error_flag = True
                        if error_flag:
                            status_list.append(status) 
                    # other cmd data verify
                    else:
                        error_flag = True
                        status_list.append(status)
                else:
                    error_flag = True
                    status_list.append(status)
        f.close()

        print_log('Crashing test (kill -9 ldb) ...\n', demo)
        cmd_line = 'exit\n'
        print_log('>>>>   {0}'.format(cmd_line), demo)
        p.stdin.write(cmd_line.encode("GBK"))
        try:
            p.stdin.flush()
        except BrokenPipeError:
            pass
            print_log('BrokenPipeError\n', demo)
        # p.stdin.flush()
        # p.kill()
        p.wait()

        print_log('Checking error status and consistency ...\n', demo)
        if error_flag == True:
            print_log('{0} has error status!!!\n'.format(test_path), demo)
            error_count += 1
            error_list.append(test_path)

        if data_verify(kv_map, test_type):
            print_log('{0} has NOT crash consistency!!!\n'.format(test_path), demo)
            fail_count += 1
            fail_list.append(test_path)


    status_list = list(set(status_list))            
    print_log('\n----- Test Report for test suite: {0}  -----\n'.format(test_path), demo, True)
    print_log('Total number of cases: {0}\n'.format(total_count), demo, True)
    print_log('Number of inconsistency cases: {0}\n'.format(fail_count), demo, True)
    print_log('Inconsistency cases list: {0}\n'.format(fail_list), demo, True)
    print_log('Number of error status cases: {0}\n'.format(error_count), demo, True)
    print_log('Error status list: {0}\n'.format(status_list), demo, True)
    print_log('Error status cases list: {0}\n'.format(error_list), demo, True)
        
    log_file_handle.close()


if __name__ == '__main__':
	main()
