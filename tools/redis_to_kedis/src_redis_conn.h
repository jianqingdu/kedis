//
//  src_redis_conn.h
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#ifndef __SRC_REDIS_CONN_H__
#define __SRC_REDIS_CONN_H__

#include "base_conn.h"

enum {
    CONN_STATE_AUTH,
    CONN_STATE_READ_RDB_LEN,
    CONN_STATE_READ_RDB_DATA,
    CONN_STATE_SYNC_CMD,
};

class SrcRedisConn : public BaseConn {
public:
    SrcRedisConn() : rdb_file_(NULL), aof_file_(NULL), cur_db_num_(-1) {}
    virtual ~SrcRedisConn() {}
    
    virtual void OnConfirm();
    virtual void OnRead();
    virtual void OnClose();
    virtual void OnTimer(uint64_t curr_tick);
    
private:
    void _LimitRDBReceiveSpeed(int recv_bytes);
    void _HandleAuth();
    void _ReadRdbLength();
    void _ReadRdbData();
    void _SyncWriteCommand();
    void _RemoveKeyPrefix(string& key);
    void _CommitAofTask();
    void _HandleRedisCommand(vector<string>& cmd_vec);
private:
    int     conn_state_;
    FILE*   rdb_file_;
    long    rdb_remain_len_;
    FILE*   aof_file_;
    int     cur_db_num_;
    char*   req_buf;
    int     req_len;
    uint64_t start_tick_;   // 接收数据每秒开始的时刻
    int     recv_bytes_;    // 当前秒接收的数据量
};

#endif
