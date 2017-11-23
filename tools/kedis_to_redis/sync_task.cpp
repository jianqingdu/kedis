//
//  sync_task.cpp
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "sync_task.h"
#include "redis_conn.h"
#include "simple_log.h"
#include "kedis_to_redis.h"
#include "event_loop.h"
#include "redis_parser.h"

ThreadPool g_thread_pool;
RedisConn g_redis_conn;
time_t g_last_cmd_time;

static void ping_timer_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    time_t current_time = time(NULL);
    if (current_time > g_last_cmd_time + 60) {
        // start a ping command if idled for 1 minutes,
        // so the redis connection will not be close by the server for idled too long
        KeepalivePingTask* task = new KeepalivePingTask();
        g_thread_pool.AddTask(task);
    }
}

void init_sync_task()
{
    g_thread_pool.Init(1); // only one thread in the pool，so tasks will be executed in order
    
    g_redis_conn.SetAddr(g_config.dst_redis_host, g_config.dst_redis_port);
    g_redis_conn.SetPassword(g_config.dst_redis_password);
    g_redis_conn.Init();
    if (g_config.dst_redis_db != -1) {
        string request;
        vector<string> cmd_vec = {"SELECT", to_string(g_config.dst_redis_db)};
        build_request(cmd_vec, request);
        redisReply* reply = g_redis_conn.DoRawCmd(request);
        if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
            log_message(kLogLevelError, "RedisConn DoRawCmd failed: %s\n", request.c_str());
            exit(1);
        }
    }
    
    g_last_cmd_time = time(NULL);
    get_main_event_loop()->AddTimer(ping_timer_callback, NULL, 5000);
}

void SyncCmdTask::run()
{
    redisReply* reply = g_redis_conn.DoRawCmd(raw_cmd_);
    if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
        log_message(kLogLevelInfo, "RedisConn DoRawCmd failed: %s, error:%s\n", raw_cmd_.c_str(), reply ? reply->str : "");
    }
    
    g_last_cmd_time = time(NULL);
    int remain_task_cnt = g_thread_pool.GetTotalTaskCnt() - 1;
    if (remain_task_cnt > 0) {
        log_message(kLogLevelInfo, "SyncCmdTask, remain_cnt=%d\n", remain_task_cnt);
    }
}

void KeepalivePingTask::run()
{
    g_redis_conn.DoCmd("PING");
    g_last_cmd_time = time(NULL);
}
