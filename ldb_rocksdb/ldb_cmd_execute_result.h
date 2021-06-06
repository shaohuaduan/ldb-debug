//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include <cstddef>
using namespace ROCKSDB_NAMESPACE;

class LDBCommandExecuteResult {
 public:
  enum ExeCmd {
    EXEC_OTHERS = 0,
    EXEC_OPEN_DELETE_DB = 1,
    EXEC_OPEN_DELETE_DB_WITH_TTL = 2,
    EXEC_GET_RELEASE_SNAPSHOT = 3,
    EXEC_GET_RELEASE_ITERATOR = 4,
    EXEC_GET_RELEASE_BATCH = 5,
    EXEC_DELETE_DB = 6,
  };

  LDBCommandExecuteResult() : cmd_(EXEC_OTHERS), db_(nullptr){}

  LDBCommandExecuteResult(ExeCmd cmd, DB* db)
      : cmd_(cmd), db_(db) {}

  LDBCommandExecuteResult(ExeCmd cmd, DBWithTTL* db_with_ttl)
      : cmd_(cmd), db_with_ttl_(db_with_ttl) {}

  LDBCommandExecuteResult(ExeCmd cmd, std::vector<const Snapshot*> snapshot)
      : cmd_(cmd), snapshot_(snapshot) {}

  LDBCommandExecuteResult(ExeCmd cmd, std::vector<Iterator*> iterator)
      : cmd_(cmd), iterator_(iterator) {}

  LDBCommandExecuteResult(ExeCmd cmd, WriteBatchWithIndex* batch)
      : cmd_(cmd), batch_(batch) {}

  DB* GetDB() {
    return db_;
  }

  DBWithTTL* GetDBWithTTL() {
    return db_with_ttl_;
  }

  std::vector<const Snapshot*> GetSnapshot() {
    return snapshot_;
  }

  std::vector< Iterator*> GetIterator() {
    return iterator_;
  }

  WriteBatchWithIndex* GetBatch() {
    return batch_;
  }

  bool IsOpenDeleteDB() { return cmd_ == EXEC_OPEN_DELETE_DB; }
  bool IsOpenDeleteDBWithTTL() { return cmd_ == EXEC_OPEN_DELETE_DB_WITH_TTL; }
  bool IsGetReleaseSnapshot() { return cmd_ == EXEC_GET_RELEASE_SNAPSHOT; }
  bool IsGetReleaseIterator() { return cmd_ == EXEC_GET_RELEASE_ITERATOR; }
  bool IsGetReleaseBatch() { return cmd_ == EXEC_GET_RELEASE_BATCH; }  
  bool IsDeleteDB() { return cmd_ == EXEC_DELETE_DB; }

  static LDBCommandExecuteResult ExeOpenDeleteDB(DB* db) {
    return LDBCommandExecuteResult(EXEC_OPEN_DELETE_DB, db);
  }
    
  static LDBCommandExecuteResult ExeOpenDeleteDBWithTTL(DBWithTTL* db_with_ttl) {
    return LDBCommandExecuteResult(EXEC_OPEN_DELETE_DB_WITH_TTL, db_with_ttl);
  }

  static LDBCommandExecuteResult ExeGetReleaseSnapshot(std::vector<const Snapshot*> snapshot) {
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_SNAPSHOT, snapshot);
  }

  static LDBCommandExecuteResult ExeGetReleaseIterator(std::vector< Iterator*> iterator) {
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_ITERATOR, iterator);
  }

  static LDBCommandExecuteResult ExeGetReleaseBatch(WriteBatchWithIndex* batch) {
    return LDBCommandExecuteResult(EXEC_GET_RELEASE_BATCH, batch);
  }

  bool operator==(const LDBCommandExecuteResult&);
  bool operator!=(const LDBCommandExecuteResult&);
 
 private:
  ExeCmd cmd_;
  DB* db_;
  DBWithTTL* db_with_ttl_;
  std::vector<const Snapshot*> snapshot_;
  std::vector< Iterator*> iterator_;
  WriteBatchWithIndex* batch_;
};

