//
//  redis_to_kedis.cpp
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "util.h"
#include "cmd_line_parser.h"
#include "event_loop.h"
#include "src_redis_conn.h"
#include "sync_task.h"
#include "simple_log.h"

int main(int argc, char* argv[])
{
    parse_cmd_line(argc, argv);
    
    int io_thread_num = 0; // all nonblock network io in main thread
    init_thread_event_loops(io_thread_num);
    init_thread_base_conn(io_thread_num);
    
    init_simple_log(kLogLevelDebug, "log");
    
    g_thread_pool.Init(1); // only one thread in the pool，so tasks will be executed in order
    
    if (g_config.src_from_rdb) {
        SyncRdbTask* task = new SyncRdbTask();
        g_thread_pool.AddTask(task);
    } else {
        SrcRedisConn* src_conn = new SrcRedisConn();
        src_conn->Connect(g_config.src_redis_host, g_config.src_redis_port);
    }
    
    get_main_event_loop()->Start();
    
    return 0;
}
