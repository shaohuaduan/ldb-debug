//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "ldb_tool.h"
#include "ldb_cmd.h"
using namespace ROCKSDB_NAMESPACE;

void LDBTool::RunCommand(std::string cmd) {

#ifdef DEBUG
	std::cout <<"RunCommand: "<< cmd<<std::endl;
#endif
  LDBCommand* cmdObj = LDBCommand::InitCommand(cmd, db_, db_with_ttl_, snapshot_, iterator_, batch_);
  if (cmdObj == nullptr) {
    return;
  }

  cmdObj->Run();
  LDBCommandExecuteResult ret = cmdObj->GetExecuteState();
  UpdateState(ret);
  delete cmdObj;
  return;
}


void LDBTool::UpdateState(LDBCommandExecuteResult ret) {
  
  if(ret.IsOpenDeleteDB()){
    db_ = ret.GetDB();
  }else if(ret.IsOpenDeleteDBWithTTL()){
    db_with_ttl_ = ret.GetDBWithTTL();
  }else if (ret.IsGetReleaseSnapshot()){
    snapshot_ = ret.GetSnapshot();
  }else if (ret.IsGetReleaseIterator()){
    iterator_ = ret.GetIterator();
  }else if (ret.IsGetReleaseBatch()){
    batch_ = ret.GetBatch();
  }else {}
}


LDBTool::LDBTool() 
    : db_(nullptr), db_with_ttl_(nullptr), batch_(nullptr){}