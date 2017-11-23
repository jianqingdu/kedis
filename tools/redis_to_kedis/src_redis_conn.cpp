//
//  src_redis_conn.cpp
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "src_redis_conn.h"
#include "redis_parser.h"
#include "cmd_line_parser.h"
#include "sync_task.h"
#include "simple_log.h"
#include <ctype.h>
#include <algorithm>
using namespace std;

void SrcRedisConn::OnConfirm()
{
    log_message(kLogLevelInfo, "connect to redis %s:%d success\n", m_peer_ip.c_str(), m_peer_port);
    BaseConn::OnConfirm();
    
    if (!g_config.src_redis_password.empty()) {
        vector<string> cmd_vec = {"AUTH", g_config.src_redis_password};
        string request;
        build_request(cmd_vec, request);
        Send((void*)request.data(), (int)request.size());
        
        conn_state_ = CONN_STATE_AUTH;
    } else {
        vector<string> cmd_vec = {"SYNC"};
        string request;
        build_request(cmd_vec, request);
        Send((void*)request.data(), (int)request.size());
        
        conn_state_ = CONN_STATE_READ_RDB_LEN;
    }
}

void SrcRedisConn::OnRead()
{
    int write_offset = m_in_buf.GetWriteOffset();
    _RecvData();
    int recv_bytes  = m_in_buf.GetWriteOffset() - write_offset;
    
    // 处理redis协议需要按C语言以'\0'结尾的字符串处理，接收buf预留了至少1个字节来放'\0'
    uchar_t* write_pos = m_in_buf.GetWriteBuffer();
    write_pos[0] = 0;
    
    if (conn_state_ == CONN_STATE_AUTH) {
        _HandleAuth();
    }
    
    if (conn_state_ == CONN_STATE_READ_RDB_LEN) {
        _ReadRdbLength();
    }
    
    if (conn_state_ == CONN_STATE_READ_RDB_DATA) {
        _ReadRdbData();
        _LimitRDBReceiveSpeed(recv_bytes);
    }
    
    if (conn_state_ == CONN_STATE_SYNC_CMD) {
        _SyncWriteCommand();
    }
    
    m_in_buf.ResetOffset();
}

void SrcRedisConn::OnClose()
{
    if (m_open) {
        log_message(kLogLevelInfo, "connection to redis %s:%d broken\n", m_peer_ip.c_str(), m_peer_port);
    } else {
        log_message(kLogLevelInfo, "connect to redis %s:%d failed\n", m_peer_ip.c_str(), m_peer_port);
    }
    
    BaseConn::Close();
}

void SrcRedisConn::OnTimer(uint64_t curr_tick)
{
    // if RDB sync finished wihout any more redis command, AOF sync task need to be triggered here
    if (g_sync_rdb_finished) {
        _CommitAofTask();
    }
}

void SrcRedisConn::_LimitRDBReceiveSpeed(int recv_bytes)
{
    uint64_t current_tick = get_tick_count();
    if (current_tick >= start_tick_ + 1000) {
        start_tick_ = current_tick;
        recv_bytes_ = recv_bytes;
    } else {
        recv_bytes_ += recv_bytes;
        
        if (recv_bytes_ > g_config.network_limit) {
            uint32_t sleep_time = (uint32_t)(current_tick - start_tick_) * 1000;
            log_message(kLogLevelInfo, "receive too fast, sleep %d microsecond\n", sleep_time);
            usleep(sleep_time);
        }
    }
}

void SrcRedisConn::_HandleAuth()
{
    RedisReply reply;
    int ret = parse_redis_response((char*)m_in_buf.GetReadBuffer(), m_in_buf.GetReadableLen(), reply);
    if (ret > 0) {
        if ((reply.GetType() == REDIS_TYPE_STATUS) && (reply.GetStrValue() == "OK")) {
            log_message(kLogLevelInfo, "Auth OK, continue\n");
            m_in_buf.Read(NULL, ret);
            
            vector<string> cmd_vec = {"SYNC"};
            string request;
            build_request(cmd_vec, request);
            Send((void*)request.data(), (int)request.size());
            
            conn_state_ = CONN_STATE_READ_RDB_LEN;
        } else {
            log_message(kLogLevelError, "Auth failed, exit\n");
            exit(1);
        }
    } else if (ret < 0) {
        log_message(kLogLevelError, "redis parse failed, exit\n");
        exit(1);
    }
}

void SrcRedisConn::_ReadRdbLength()
{
    // SYNC command return format:  $len\r\n rdb_data(len bytes)
    char* redis_cmd = (char*)m_in_buf.GetReadBuffer();
    int redis_len = m_in_buf.GetReadableLen();
    if (redis_len < 3) {
        return;
    }
    
    char* new_line = strstr(redis_cmd, "\r\n");
    if (!new_line) {
        return;
    }
    
    // Redis-Server may send multiple '\n' befor send $len\r\n
    int dollar_pos = 0;
    while ((redis_cmd[dollar_pos] != '$') && (redis_cmd + dollar_pos != new_line)) {
        dollar_pos++;
    }
    
    if (redis_cmd[dollar_pos] != '$') {
        log_message(kLogLevelError, "SYNC response without a $ exit: %s\n", redis_cmd);
        exit(1);
    }
    
    long rdb_total_len = atol(redis_cmd + dollar_pos + 1);
    int rdb_start_pos = (int)(new_line - redis_cmd) + 2;
    m_in_buf.Read(NULL, rdb_start_pos);
    
    log_message(kLogLevelInfo, "rdb_len=%ld\n", rdb_total_len);
    rdb_remain_len_ = rdb_total_len;
    conn_state_ = CONN_STATE_READ_RDB_DATA;
    recv_bytes_ = 0;
    start_tick_ = get_tick_count();
    
    // write to file cause rdb file may be too large that can not reside in memory
    rdb_file_ = fopen(g_config.rdb_file.c_str(), "wb");
    if (!rdb_file_) {
        log_message(kLogLevelError, "open file %s for write failed, exit\n", g_config.rdb_file.c_str());
        exit(1);
    }
}

void SrcRedisConn::_ReadRdbData()
{
    char* rdb_data = (char*)m_in_buf.GetReadBuffer();
    long rdb_len = m_in_buf.GetReadableLen();
    if (rdb_len > rdb_remain_len_) {
        rdb_len = rdb_remain_len_;
    }
    
    if (fwrite(rdb_data, 1, rdb_len, rdb_file_) != (size_t)rdb_len) {
        log_message(kLogLevelError, "fwrite failed, exit\n");
        exit(1);
    }
    
    rdb_remain_len_ -= rdb_len;
    m_in_buf.Read(NULL, (uint32_t)rdb_len);
    if (rdb_remain_len_ == 0) {
        conn_state_ = CONN_STATE_SYNC_CMD;
        fclose(rdb_file_);
        rdb_file_ = NULL;
        
        log_message(kLogLevelInfo, "read all rdb data\n");
        
        SyncRdbTask* task = new SyncRdbTask;
        g_thread_pool.AddTask(task);
    }
}

void SrcRedisConn::_SyncWriteCommand()
{
    while (true) {
        vector<string> cmd_vec;
        string err_msg;
        int ret = parse_redis_request((char*)m_in_buf.GetReadBuffer(), m_in_buf.GetReadableLen(), cmd_vec, err_msg);
        if (ret > 0) {
            if (!cmd_vec.empty()) {
                string& cmd = cmd_vec[0];
                transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
                
                req_buf = (char*)m_in_buf.GetReadBuffer();
                req_len = ret;
                _HandleRedisCommand(cmd_vec);
            }
            
            m_in_buf.Read(NULL, ret);
        } else if (ret < 0) {
            log_message(kLogLevelError, "parse redis failed: %s", err_msg.c_str());
            OnClose();
            break;
        } else {
            // not yet receive a whole command
            break;
        }
    }
}

void SrcRedisConn::_RemoveKeyPrefix(string& key)
{
    size_t pos = key.find(g_config.prefix);
    if (pos != string::npos) {
        key = key.substr(pos + g_config.prefix.size());
    }
}

void SrcRedisConn::_CommitAofTask()
{
    if (aof_file_) {
        int cmd_len = 0;
        fwrite(&cmd_len, 4, 1, aof_file_);
        fclose(aof_file_);
        aof_file_ = NULL;
        
        SyncAofTask* task = new SyncAofTask;
        g_thread_pool.AddTask(task);
    }
}

void SrcRedisConn::_HandleRedisCommand(vector<string>& cmd_vec)
{
    const string& cmd = cmd_vec[0];
    if (cmd == "PING") {
        ;
    } else if (cmd == "SELECT") {
        cur_db_num_ = atoi(cmd_vec[1].c_str());
    } else {
        if ((g_config.src_redis_db == -1) || (g_config.src_redis_db == cur_db_num_)) {
            string raw_cmd;
            if (g_config.prefix.empty()) {
                // do not need delete key prefix
                raw_cmd.append(req_buf, req_len);
            } else {
                // delete key prefix
                if ((cmd == "DEL") || (cmd == "MGET") || (cmd == "PFCOUNT")) {
                    int cmd_cnt = (int)cmd_vec.size();
                    for (int i = 1; i < cmd_cnt; ++i) {
                        _RemoveKeyPrefix(cmd_vec[i]);
                    }
                } else if (cmd == "MSET") {
                    int cmd_cnt = (int)cmd_vec.size();
                    for (int i = 1; i < cmd_cnt; i += 2) {
                        _RemoveKeyPrefix(cmd_vec[i]);
                    }
                } else {
                    _RemoveKeyPrefix(cmd_vec[1]);
                }
                
                build_request(cmd_vec, raw_cmd);
            }
            
            if (!g_sync_rdb_finished) {
                if (!aof_file_) {
                    aof_file_ = fopen(g_config.aof_file.c_str(), "wb");
                    if (!aof_file_) {
                        log_message(kLogLevelError, "fopen aof file %s for write failed\n", g_config.aof_file.c_str());
                        exit(1);
                    }
                }
                
                int cmd_len = (int)raw_cmd.size();
                fwrite(&cmd_len, 4, 1, aof_file_);
                fwrite(raw_cmd.data(), 1, raw_cmd.size(), aof_file_);
            } else {
                _CommitAofTask();
                
                SyncCmdTask* task = new SyncCmdTask(raw_cmd);
                g_thread_pool.AddTask(task);
            }
        }
    }
}
