//
//  sync_task.h
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#ifndef __SYNC_TASK_H__
#define __SYNC_TASK_H__

#include "util.h"
#include "redis_conn.h"
#include "thread_pool.h"

class SyncRdbTask : public Task {
public:
    SyncRdbTask() {}
    virtual ~SyncRdbTask() {}
    
    virtual void run();
};

class SyncAofTask : public Task {
public:
    SyncAofTask() {}
    virtual ~SyncAofTask() {}
    
    virtual void run();
};

class SyncCmdTask : public Task {
public:
    SyncCmdTask(const string& raw_cmd) : raw_cmd_(raw_cmd) {}
    virtual ~SyncCmdTask() {}
    
    virtual void run();
private:
    string  raw_cmd_;
};

class KeepalivePingTask : public Task {
public:
    KeepalivePingTask() {}
    virtual ~KeepalivePingTask() {}
    
    virtual void run();
};

extern ThreadPool g_thread_pool;
extern bool g_sync_rdb_finished;

void execute_pipeline(int pipeline_cnt, RedisConn& redis_conn);

#endif
