//
//  binlog.h
//  kedis
//
//  Created by ziteng on 17/9/12.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __BINLOG_H__
#define __BINLOG_H__

#include "util.h"
#include "thread_pool.h"
#include "rocksdb/db.h"

class Binlog {
public:
    Binlog();
    virtual ~Binlog();
    
    int Init();
    void InitWithMaster(const string& binlog_id, int cur_db_idx, uint64_t seq);
    void Drop(); // clear all data
    int Store(int db_idx, const string& command);
    int Extract(uint64_t seq, string& command);
    void Purge();
    
    string GetBinlogId() { return binlog_id_; }
    int GetCurDbIdx() { return cur_db_idx_; }
    uint64_t GetMinSeq() { return min_seq_; }
    uint64_t GetMaxSeq() { return max_seq_; }
private:
    void GenerateBinlogId();
    string EncodeKey(uint64_t seq);
    uint64_t DecodeKey(const rocksdb::Slice& slice);
private:
    rocksdb::DB*    db_;
    mutex           mtx_;
    string          binlog_id_;
    int             cur_db_idx_;
    uint64_t        min_seq_;
    uint64_t        max_seq_;
    bool            empty_;
};

class PurgeBinlogThread : public Thread {
public:
    PurgeBinlogThread() {}
    virtual ~PurgeBinlogThread() {}
    
    virtual void OnThreadRun(void);
};

#endif /* __BINLOG_H__ */
