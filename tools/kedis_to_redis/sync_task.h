//
//  sync_task.h
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __SYNC_TASK_H__
#define __SYNC_TASK_H__

#include "util.h"
#include "thread_pool.h"

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

void init_sync_task();

#endif /* __SYNC_TASK_H__ */
