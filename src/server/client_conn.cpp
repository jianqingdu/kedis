//
//  client_conn.cpp
//  kedis
//
//  Created by ziteng on 17/7/19.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "client_conn.h"
#include "simple_log.h"
#include "server.h"
#include "cmd_db.h"
#include "replication.h"
#include "slowlog.h"
#include <algorithm>
#include <ctype.h>
using namespace std;

ClientConn::ClientConn()
{
    ++g_server.client_num;
    db_index_ = 0;
    authenticated_ = false;
    slave_port_ = 0;
    sync_seq_ = 0;
    repl_snapshot_ = nullptr;
}

ClientConn::~ClientConn()
{
    --g_server.client_num;
    if (repl_snapshot_) {
        delete repl_snapshot_;
    }
}

void ClientConn::Close()
{
    if (!pipeline_response_.empty()) {
        Send((char*)pipeline_response_.data(), (int)pipeline_response_.size());
        pipeline_response_.clear();
    }
    
    if (flag_ == CLIENT_MASTER) {
        // the connection to master was disconnected, reset the master handle,
        // this will triger the timer to reconnect to the master
        g_server.master_handle = NETLIB_INVALID_HANDLE;
        g_server.master_link_status = "down";
    } else if (flag_ == CLIENT_SLAVE) {
        g_server.slave_mutex.lock();
        g_server.slaves.remove(this);
        g_server.slave_mutex.unlock();
    }
    
    BaseConn::Close();
}

void ClientConn::OnConnect(BaseSocket* base_socket)
{
    BaseConn::OnConnect(base_socket);
    state_ = CONN_STATE_CONNECTED;
    flag_ = CLIENT_NORMAL;
    g_stat.total_connections_received++;
    
    log_message(kLogLevelDebug, "connect from %s:%d\n", m_peer_ip.c_str(), m_peer_port);
    if (g_server.client_num > g_server.max_clients) {
        log_message(kLogLevelError, "reach max client number\n");
        SendError("max number of clients reached");
        Close();
        g_stat.rejected_connections++;
    }
}

void ClientConn::OnConfirm()
{
    BaseConn::OnConfirm();
    
    g_server.master_link_status = "up";
    authenticated_ = true;
    
    string req;
    if (!g_server.master_auth.empty()) {
        vector<string> cmd_vec = {"AUTH", g_server.master_auth};
        build_request(cmd_vec, req);
        state_ = CONN_STATE_RECV_AUTH;
        log_message(kLogLevelInfo, "send AUTH, enter RECV_AUTH_STATE\n");
    } else {
        vector<string> cmd_vec = {"REPLCONF", "listening-port",  to_string(g_server.port)};
        build_request(cmd_vec, req);
        state_ = CONN_STATE_RECV_PORT;
        log_message(kLogLevelInfo, "send REPLCONF, enter RECV_PORT_STATE\n");
    }
    
    Send((void*)req.data(), (int)req.size());
}

void ClientConn::OnRead()
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
                vector<string> cmd_vec = {"REPLCONF", "listening-port",  to_string(g_server.port)};
                build_request(cmd_vec, req);
                Send((void*)req.data(), (int)req.size());
                state_ = CONN_STATE_RECV_PORT;
                log_message(kLogLevelInfo, "send REPLCONF, enter RECV_PORT_STATE\n");
            } else if (state_ == CONN_STATE_RECV_PORT) {
                string req;
                uint64_t expect_binlog_seq = g_server.binlog.GetMaxSeq() + 1;
                vector<string> cmd_vec = {"PSYNC", g_server.binlog.GetBinlogId(), to_string(expect_binlog_seq)};
                build_request(cmd_vec, req);
                Send((void*)req.data(), (int)req.size());
                state_ = CONN_STATE_RECV_PSYNC;
                log_message(kLogLevelInfo, "send PSYNC, enter RECV_PSYNC_STATE\n");
            } else {
                string respone = reply.GetStrValue();
                if (respone == "CONTINUE") {
                    state_ = CONN_STATE_CONNECTED;
                    log_message(kLogLevelInfo, "receive CONTINUE, enter CONNECTED_STATE\n");
                } else {
                    prepare_snapshot_replication();
                    state_ = CONN_STATE_RECV_SNAPSHOT;
                    log_message(kLogLevelInfo, "receive FULLRESYNC, enter RECV_SNAPSHOT_STATE\n");
                }
            }
            
            m_in_buf.Read(NULL, ret);
            m_in_buf.ResetOffset();
        } else if (ret < 0) {
            SendError("parse redis protocol failed");
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
                // save the request here, so it will not need to rebuild the command when storing binlog
                cur_req_buf_ = (char*)m_in_buf.GetReadBuffer();
                cur_req_len_ = ret;
                
                string& cmd = cmd_vec[0];
                transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
                
                _HandleRedisCommand(cmd_vec);
            }
            m_in_buf.Read(NULL, ret);
        } else if (ret < 0) {
            SendError(err_msg);
            Close();
            return;
        } else {
            // not yet receive a whole command
            m_in_buf.ResetOffset();
            break;
        }
    }
    
    if (!pipeline_response_.empty()) {
        Send((char*)pipeline_response_.data(), (int)pipeline_response_.size());
        pipeline_response_.clear();
    }
}

void ClientConn::OnTimer(uint64_t curr_tick)
{
    if (flag_ == CLIENT_NORMAL) {
        if (g_server.client_timeout && (curr_tick > m_last_recv_tick + g_server.client_timeout * 1000)) {
            log_message(kLogLevelDebug, "client timeout %s:%d\n", m_peer_ip.c_str(), m_peer_port);
            Close();
        }
    } else {
        // master/slave connection must send heartbeat packet
        if (!IsOpen()) {
            if (curr_tick >= m_last_send_tick + 2000) {
                // if connection has not establish after 2 seconds, close the connection
                Close();
            }
            return;
        }
        
        if (curr_tick >= m_last_send_tick + 5000) {
            vector<string> cmd_vec = {"replconf", "ack", to_string(g_server.binlog.GetMaxSeq())};
            string req;
            build_request(cmd_vec, req);
            Send((char*)req.data(), (int)req.size());
        }
        
        if (g_server.repl_timeout && (curr_tick > m_last_recv_tick + g_server.repl_timeout * 1000)) {
            log_message(kLogLevelDebug, "master/slave connection timeout %s:%d\n", m_peer_ip.c_str(), m_peer_port);
            Close();
        }
    }
}

void ClientConn::OnLoop()
{
    if (flag_ != CLIENT_SLAVE) {
        return;
    }
    
    if (repl_snapshot_) {
        repl_snapshot_->SendTo(this);
        if (repl_snapshot_->IsCompelte()) {
            string req;
            vector<string> cmd_vec = {"REPL_SNAPSHOT_COMPLETE", g_server.binlog.GetBinlogId(),
                                    to_string(db_index_), to_string(sync_seq_ - 1)};
            build_request(cmd_vec, req);
            Send((void*)req.data(), (int)req.size());
            delete repl_snapshot_;
            repl_snapshot_ = nullptr;
            log_message(kLogLevelInfo, "send snapshot complete\n");
        } else {
            return;
        }
    }
    
    uint64_t max_seq = g_server.binlog.GetMaxSeq();
    for (; sync_seq_ <= max_seq; sync_seq_++) {
        if (IsBusy()) {
            break;
        }
        
        string command;
        if (g_server.binlog.Extract(sync_seq_, command) == CODE_ERROR) {
            break;
        }
        
        Send((char*)command.data(), (int)command.size());
    }
}

void ClientConn::SendRawResponse(const string& resp)
{
    // for client connection, just accumulate the response, will be sent after all requests have be processed
    // for master/slave connection, just discard the response
    if (flag_ == CLIENT_NORMAL) {
        pipeline_response_.append(resp);
    }
}

void ClientConn::SendError(const string& error_msg)
{
    string resp = "-ERR ";
    resp += error_msg;
    resp += "\r\n";
    SendRawResponse(resp);
}

void ClientConn::SendInteger(long i)
{
    string resp = ":";
    resp += to_string(i);
    resp += "\r\n";
    SendRawResponse(resp);
}

void ClientConn::SendSimpleString(const string& str)
{
    string resp = "+";
    resp += str;
    resp += "\r\n";
    SendRawResponse(resp);
}

void ClientConn::SendBulkString(const string& str)
{
    string resp;
    if (str.empty()) {
        resp = "$-1\r\n";
    } else {
        resp = "$";
        resp += to_string(str.size());
        resp += "\r\n";
        resp += str;
        resp += "\r\n";
    }
    SendRawResponse(resp);
}

void ClientConn::SendArray(const vector<string> &str_vec)
{
    string resp;
    char tmp[32];
    int size = (int)str_vec.size();
    int pos = build_prefix(tmp, 32, '*', size);
    resp.append(tmp + pos, strlen(tmp + pos));
    for (auto it = str_vec.begin(); it != str_vec.end(); ++it) {
        if (it->empty()) {  // empty string means the value is not exist
            resp.append("$-1\r\n");
        } else {
            size = (int)it->size();
            pos = build_prefix(tmp, 32, '$', size);
            resp.append(tmp + pos, strlen(tmp + pos));
            resp.append(it->data(), size);
            resp.append("\r\n");
        }
    }
    
    SendRawResponse(resp);
}

void ClientConn::SendMultiBuldLen(long len)
{
    string resp;
    char tmp[32];
    int pos = build_prefix(tmp, 32, '*', (int)len);
    resp.append(tmp + pos, strlen(tmp + pos));
    
    SendRawResponse(resp);
}

void ClientConn::_HandleRedisCommand(vector<string>& cmd_vec)
{
    string& cmd = cmd_vec[0];
    if (cmd == "QUIT") {
        SendRawResponse("+OK\r\n");
        Close();
        return;
    }
    
    auto cmd_iter = g_server.kedis_commands.find(cmd);
    if (cmd_iter == g_server.kedis_commands.end()) {
        SendError("command not support");
        log_message(kLogLevelError, "command not support: %s\n", cmd.c_str());
        return;
    }
    
    KedisCommand kedis_cmd = cmd_iter->second;
    int cmd_vec_size = (int)cmd_vec.size();
    if (((kedis_cmd.arity > 0) && (kedis_cmd.arity != cmd_vec_size)) || (cmd_vec_size < -kedis_cmd.arity)) {
        string error_msg = "wrong number of arguments for '" + cmd + "' command";
        SendError(error_msg);
        return;
    }
    
    if (!g_server.require_pass.empty() && !authenticated_ && (kedis_cmd.proc != auth_command)) {
        SendError("NOAUTH Authentication required");
        return;
    }
    
    if (g_server.slave_read_only && !g_server.master_host.empty() && (flag_ == CLIENT_NORMAL) && kedis_cmd.is_write) {
        SendError("READONLY You can't write against a read only slave");
        return;
    }
    
    uint64_t proc_start_tick = get_tick_count();
    kedis_cmd.proc(this, cmd_vec);
    uint64_t proc_time = get_tick_count() - proc_start_tick;
    if ((g_server.slowlog_log_slower_than >= 0) && ((int)proc_time >= g_server.slowlog_log_slower_than)) {
        add_slowlog(cmd_vec, proc_time);
    }
    g_stat.total_commands_processed++;
}
