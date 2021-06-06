//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <string>
#include <vector>
#include "hyperleveldb/db.h"

#include <cstddef>
using namespace leveldb;

class LDBCommandExecuteResult {
 public:
  enum ExeCmd {
    EXEC_OTHERS = 0,
    EXEC_OPEN_DELETE_DB = 1,
    EXEC_GET_RELEASE_SNAPSHOT = 2,
	EXEC_GET_RELEASE_ITERATOR = 3,
    EXEC_GET_RELEASE_REPLAY_ITERATOR = 4,
    EXEC_DELETE_DB = 5,
  };

  LDBCommandExecuteResult() : cmd_(EXEC_OTHERS), db_(nullptr){}

  LDBCommandExecuteResult(ExeCmd cmd, DB* db)
      : cmd_(cmd), db_(db) {}

  LDBCommandExecuteResult(ExeCmd cmd, std::vector<const Snapshot*> snapshot, std::vector<Iterator*> iterator, std::vector< ReplayIterator*> replay_iterator) //hyperleveldb ONLY
      : cmd_(cmd), snapshot_(snapshot), iterator_(iterator), replay_iterator_(replay_iterator) {}

  DB* GetDB() {
    return db_;
  }

  std::vector<const Snapshot*> GetSnapshot() {
    return snapshot_;
  }

  std::vector< ReplayIterator*> GetReplayIterator() {
    return replay_iterator_;
  }
  
  std::vector< Iterator*> GetIterator() {
    return iterator_;
  }
  
  bool IsOpenDeleteDB() { return cmd_ == EXEC_OPEN_DELETE_DB; }
  bool IsGetReleaseSnapshot() { return cmd_ == EXEC_GET_RELEASE_SNAPSHOT; }
  bool IsGetReleaseReplayIterator() { return cmd_ == EXEC_GET_RELEASE_REPLAY_ITERATOR; }
  bool IsGetReleaseIterator() { return cmd_ == EXEC_GET_RELEASE_ITERATOR; }
  bool IsDeleteDB() { return cmd_ == EXEC_DELETE_DB; }

  static LDBCommandExecuteResult ExeOpenDeleteDB(DB* db) {
    return LDBCommandExecuteResult(EXEC_OPEN_DELETE_DB, db);
  }
  
  static LDBCommandExecuteResult ExeGetReleaseSnapshot(std::vector<const Snapshot*> snapshot) {
    std::vector<ReplayIterator*> replay_iterator;
	std::vector<Iterator*> iterator;
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_SNAPSHOT, snapshot, iterator, replay_iterator);
  }

  static LDBCommandExecuteResult ExeGetReleaseReplayIterator(std::vector< ReplayIterator*> replay_iterator) {
    std::vector<const Snapshot*> snapshot;
	std::vector<Iterator*> iterator;
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_REPLAY_ITERATOR, snapshot, iterator, replay_iterator);
  }

  static LDBCommandExecuteResult ExeGetReleaseIterator(std::vector< Iterator*> iterator) {
    std::vector<const Snapshot*> snapshot;
	std::vector<ReplayIterator*> replay_iterator;
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_ITERATOR, snapshot, iterator, replay_iterator);
  }

  bool operator==(const LDBCommandExecuteResult&);
  bool operator!=(const LDBCommandExecuteResult&);
 
 private:
  ExeCmd cmd_;
  DB* db_;
  std::vector<const Snapshot*> snapshot_;
  std::vector< ReplayIterator*> replay_iterator_;
  std::vector< Iterator*> iterator_;
};

