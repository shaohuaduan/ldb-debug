// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#include <string>
#include <vector>
#include "hyperleveldb/db.h"
#include "hyperleveldb/options.h"
#include <iostream>

#include "ldb_cmd_execute_result.h"

using namespace leveldb;

class LDBTool {
 public:
  LDBTool();
  void RunCommand(std::string cmd);
 
 private:
 void UpdateState(LDBCommandExecuteResult ret);
  DB* db_;
  std::vector<const Snapshot*> snapshot_;
  std::vector< Iterator*> iterator_; 
  std::vector< ReplayIterator*> replay_iterator_; //hyperleveldb ONLY
};


