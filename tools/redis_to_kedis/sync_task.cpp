//
//  sync_task.cpp
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "sync_task.h"
#include "cmd_line_parser.h"
#include "redis_conn.h"
#include "redis_parser.h"
#include "rdb_reader.h"
#include "event_loop.h"
#include "simple_log.h"

ThreadPool g_thread_pool;
bool g_sync_rdb_finished = false;
RedisConn g_kedis_conn;
time_t g_last_cmd_time;

/*
 * Redis数据迁移分3个阶段
 *  1. 发送SYNC命令后同步RDB文件，用pipeline加速
 *  2. 同步RDB文件时的增量命令用AOF文件同步，用pipeline加速
 *  3. 同步AOF文件后的增量命令直接同步
 */

void ping_timer_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    time_t current_time = time(NULL);
    if (current_time > g_last_cmd_time + 60) {
        // start a ping command if idled for 1 minutes,
        // so the redis connection will not be close by the server for idled too long
        KeepalivePingTask* task = new KeepalivePingTask();
        g_thread_pool.AddTask(task);
    }
}

void SyncRdbTask::run()
{
    log_message(kLogLevelInfo, "SyncRdbTask::Run\n");
    uint64_t start_tick = get_tick_count();
    
    g_kedis_conn.SetAddr(g_config.dst_kedis_host, g_config.dst_kedis_port);
    if (!g_config.dst_kedis_password.empty()) {
        g_kedis_conn.SetPassword(g_config.dst_kedis_password);
    }
    
    if (g_kedis_conn.Init()) {
        log_message(kLogLevelError, "can not connect to destination kedis server %s:%d, exit\n",
                g_config.dst_kedis_host.c_str(), g_config.dst_kedis_port);
        exit(1);
    }
    
    if (g_config.dst_kedis_db != -1) {
        string request;
        vector<string> cmd_vec = {"SELECT", to_string(g_config.dst_kedis_db)};
        build_request(cmd_vec, request);
        redisReply* reply = g_kedis_conn.DoRawCmd(request);
        if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
            log_message(kLogLevelError, "RedisConn DoRawCmd failed: %s\n", request.c_str());
            exit(1);
        }
    }
    
    RdbReader rdb_reader;
    if (rdb_reader.Open(g_config.rdb_file)) {
        log_message(kLogLevelError, "open rdb file failed, exit\n");
        exit(1);
    }
    
    if (rdb_reader.RestoreDB(g_config.src_redis_db, g_kedis_conn)) {
        log_message(kLogLevelError, "RestoreDb failed, exit\n");
        exit(1);
    }
    
    uint64_t cost_tick = get_tick_count() - start_tick;
    log_message(kLogLevelInfo, "Sync Rdb file complete in %ld milliseconds\n", (long)cost_tick);
    
    g_sync_rdb_finished = true;
    
    g_last_cmd_time = time(NULL);
    get_main_event_loop()->AddTimer(ping_timer_callback, NULL, 10000);
}

void SyncAofTask::run()
{
    log_message(kLogLevelInfo, "SyncAofTask::run\n");
    uint64_t start_tick = get_tick_count();
    FILE* fp = fopen(g_config.aof_file.c_str(), "rb");
    if (!fp) {
        log_message(kLogLevelError, "open aof file %s failed, exit\n", g_config.aof_file.c_str());
        exit(1);
    }
    
    int cmd_len;
    char cmd_buf[1024];
    int pipeline_cnt = 0;
    
    while (true) {
        if (fread(&cmd_len, 4, 1, fp) != 1) {
            log_message(kLogLevelError, "fread cmd_len failed, exit\n");
            exit(1);
        }
        
        if (cmd_len == 0) {
            log_message(kLogLevelInfo, "read all aof file content\n");
            break;
        }
        
        char* buf;
        if (cmd_len > 1024) {
            buf = new char[cmd_len];
        } else {
            buf = cmd_buf;
        }
        
        if (fread(buf, 1, cmd_len, fp) != (size_t)cmd_len) {
            log_message(kLogLevelError, "fread cmd failed, exit\n");
            exit(1);
        }
        
        string cmd(buf, cmd_len);
        if (cmd_len > 1024) {
            delete [] buf;
        }
        
        g_kedis_conn.PipelineRawCmd(cmd);
        ++pipeline_cnt;
        
        if (pipeline_cnt >= g_config.pipeline_cnt) {
            execute_pipeline(pipeline_cnt, g_kedis_conn);
            pipeline_cnt = 0;
            g_last_cmd_time = time(NULL);
        }
    }
    
    if (pipeline_cnt >= 0) {
        execute_pipeline(pipeline_cnt, g_kedis_conn);
        g_last_cmd_time = time(NULL);
    }
    
    fclose(fp);
    
    uint64_t cost_tick = get_tick_count() - start_tick;
    log_message(kLogLevelInfo, "Sync AOF file complete in %ld milliseconds\n", (long)cost_tick);
}

void SyncCmdTask::run()
{
    redisReply* reply = g_kedis_conn.DoRawCmd(raw_cmd_);
    if ((reply == NULL) || (reply->type == REDIS_REPLY_ERROR)) {
        log_message(kLogLevelError, "RedisConn DoRawCmd failed: %s, error:%s\n", raw_cmd_.c_str(), reply ? reply->str : "");
    }
    
    g_last_cmd_time = time(NULL);
    int remain_task_cnt = g_thread_pool.GetTotalTaskCnt() - 1;
    if (remain_task_cnt > 0) {
        log_message(kLogLevelInfo, "SyncCmdTask, remain_cnt=%d\n", remain_task_cnt);
    }
}

void KeepalivePingTask::run()
{
    g_kedis_conn.DoCmd("PING");
    g_last_cmd_time = time(NULL);
}

void execute_pipeline(int pipeline_cnt, RedisConn& redis_conn)
{
    for (int i = 0; i < pipeline_cnt; i++) {
        redisReply* reply = redis_conn.GetReply();
        if (!reply || (reply->type == REDIS_REPLY_ERROR)) {
            log_message(kLogLevelError, "PipelineRawCmd failed, error: %s\n", reply ? reply->str : "");
        }
    }
}
