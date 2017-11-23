//
//  src_kedis_conn.cpp
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "src_kedis_conn.h"
#include "kedis_port.h"
#include "redis_parser.h"
#include "simple_log.h"
#include "sync_task.h"
#include <ctype.h>
#include <algorithm>
using namespace std;

SrcKedisConn::SrcKedisConn()
{
    cur_db_num_ = 0;
    state_ = CONN_STATE_IDLE;
}

SrcKedisConn::~SrcKedisConn()
{

}

void SrcKedisConn::Close()
{
    log_message(kLogLevelInfo, "close conn to src kedis-server\n");
    BaseConn::Close();
}

void SrcKedisConn::OnConfirm()
{
    BaseConn::OnConfirm();
    
    string req;
    if (!g_config.src_kedis_password.empty()) {
        vector<string> cmd_vec = {"AUTH", g_config.src_kedis_password};
        build_request(cmd_vec, req);
        state_ = CONN_STATE_RECV_AUTH;
        log_message(kLogLevelInfo, "send AUTH, enter RECV_AUTH_STATE\n");
    } else {
        vector<string> cmd_vec = {"REPLCONF", "listening-port",  to_string(g_config.dst_kedis_port)};
        build_request(cmd_vec, req);
        state_ = CONN_STATE_RECV_PORT;
        log_message(kLogLevelInfo, "send REPLCONF, enter RECV_PORT_STATE\n");
    }
    
    Send((void*)req.data(), (int)req.size());
}

void SrcKedisConn::OnRead()
{
    _RecvData();
    
    // when parse redis protocol, this makes sure searching string pattern will not pass the boundary
    m_in_buf.GetWriteBuffer()[0] = 0;
    
    if (state_ <= CONN_STATE_RECV_PSYNC) {
        RedisReply reply;
        int ret = parse_redis_response((const char*)m_in_buf.GetReadBuffer(), m_in_buf.GetReadableLen(), reply);
        if (ret > 0) {
            if (reply.GetType() == REDIS_TYPE_ERROR) {
                log_message(kLogLevelError, "#Error before sync: %s\n", reply.GetStrValue().c_str());
                Close();
                return;
            }
            
            if (state_ == CONN_STATE_RECV_AUTH) {
                string req;
                vector<string> cmd_vec = {"REPLCONF", "listening-port",  to_string(g_config.dst_kedis_port)};
                build_request(cmd_vec, req);
                Send((void*)req.data(), (int)req.size());
                state_ = CONN_STATE_RECV_PORT;
                log_message(kLogLevelInfo, "send REPLCONF, enter RECV_PORT_STATE\n");
            } else if (state_ == CONN_STATE_RECV_PORT) {
                string req;
                vector<string> cmd_vec = {"PSYNC", "", "0"};
                build_request(cmd_vec, req);
                Send((void*)req.data(), (int)req.size());
                state_ = CONN_STATE_RECV_PSYNC;
                log_message(kLogLevelInfo, "send PSYNC, enter RECV_PSYNC_STATE\n");
            } else {
                string respone = reply.GetStrValue();
                if (respone == "CONTINUE") {
                    state_ = CONN_STATE_CONNECTED;
                    log_message(kLogLevelInfo, "receive CONTINUE, something must be wrong\n");
                } else {
                    state_ = CONN_STATE_RECV_SNAPSHOT;
                    log_message(kLogLevelInfo, "receive FULLRESYNC, enter RECV_SNAPSHOT_STATE\n");
                }
            }
            
            m_in_buf.Read(NULL, ret);
            m_in_buf.ResetOffset();
        } else if (ret < 0) {
            log_message(kLogLevelError, "parse redis protocol failed");
            Close();
        }
        
        if (state_ < CONN_STATE_RECV_SNAPSHOT) {
            return;
        }
    }
    
    while (true) {
        vector<string> cmd_vec;
        string err_msg;
        int ret = parse_redis_request((const char*)m_in_buf.GetReadBuffer(), m_in_buf.GetReadableLen(), cmd_vec, err_msg);
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
            log_message(kLogLevelError, "parse redis protocol failed");
            Close();
            return;
        } else {
            // not yet receive a whole command
            m_in_buf.ResetOffset();
            break;
        }
    }
}

void SrcKedisConn::OnTimer(uint64_t curr_tick)
{
    if (!IsOpen()) {
        if (curr_tick >= m_last_send_tick + 2000) {
            // if connection has not establish after 2 seconds, close the connection
            Close();
        }
        return;
    }
    
    if (curr_tick >= m_last_send_tick + 5000) {
        vector<string> cmd_vec = {"replconf", "ack", "0"};
        string req;
        build_request(cmd_vec, req);
        Send((char*)req.data(), (int)req.size());
    }
}

void SrcKedisConn::_RemoveKeyPrefix(string& key)
{
    size_t pos = key.find(g_config.prefix);
    if (pos != string::npos) {
        key = key.substr(pos + g_config.prefix.size());
    }
}

void SrcKedisConn::_HandleRedisCommand(vector<string>& cmd_vec)
{
    const string& cmd = cmd_vec[0];
    
    if (cmd == "PING" || cmd == "REPLCONF") {
        ;
    } else if (cmd == "SELECT") {
        cur_db_num_ = atoi(cmd_vec[1].c_str());
    } else if (cmd == "REPL_SNAPSHOT_COMPLETE") {
        cur_db_num_ = atoi(cmd_vec[2].c_str());
        state_ = CONN_STATE_CONNECTED;
        log_message(kLogLevelInfo, "receive all snapshot, cur_db_idx=%d\n", cur_db_num_);
    } else {
        if ((g_config.src_kedis_db == -1) || (g_config.src_kedis_db == cur_db_num_)) {
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
            
            SyncCmdTask* task = new SyncCmdTask(raw_cmd);
            g_thread_pool.AddTask(task);
        }
    }
}
