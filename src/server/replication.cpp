//
//  replication.cpp
//  kedis
//
//  Created by ziteng on 17/8/30.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "replication.h"
#include "key_lock.h"
#include "db_util.h"
#include "cmd_db.h"
#include "encoding.h"
#include "migrate.h"

void replconf_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size % 2 != 1) {
        conn->SendError("syntex error");
        return;
    }
    
    for (int i = 1; i < cmd_size; i += 2) {
        if (!strcasecmp(cmd_vec[1].c_str(), "listening-port")) {
            int port = atoi(cmd_vec[i + 1].c_str());
            conn->SetSlavePort(port);
        } else if (!strcasecmp(cmd_vec[1].c_str(), "ack")) {
            // just to keep the connection alive, do nothing
            return;
        }
    }
    
    conn->SendSimpleString("OK");
}

void psync_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    string binlog_id = cmd_vec[1];
    uint64_t expect_seq = atol(cmd_vec[2].c_str());
    log_message(kLogLevelInfo, "PSYNC %s %llu from: %s:%d\n", binlog_id.c_str(), expect_seq,
                conn->GetPeerIP(), conn->GetPeerPort());
    
    uint64_t min_seq = g_server.binlog.GetMinSeq();
    uint64_t max_seq = g_server.binlog.GetMaxSeq();
    bool expect_seq_in_range = (expect_seq >= min_seq) && (expect_seq <= max_seq + 1);
    
    if ((g_server.binlog.GetBinlogId() == binlog_id) && expect_seq_in_range) {
        string resp = "+CONTINUE\r\n";
        conn->Send((void*)resp.data(), (int)resp.size());
        conn->SetSyncSeq(expect_seq);
    } else {
        string resp = "+FULLRESYNC\r\n";
        conn->Send((void*)resp.data(), (int)resp.size());
        
        uint64_t start_tick = get_tick_count();
        spinlock_all();
        
        const rocksdb::Snapshot* snapshot = g_server.db->GetSnapshot();
        int cur_db_idx = g_server.binlog.GetCurDbIdx();
        max_seq = g_server.binlog.GetMaxSeq();
        assert(snapshot);
        ReplicationSnapshot* repl_snapshot = new ReplicationSnapshot(snapshot);
        unlock_all();
        uint64_t cost_tick = get_tick_count() - start_tick;
        log_message(kLogLevelInfo, "GetSnapshot takes %lu ms\n", cost_tick);
        
        conn->SetReplSnapshot(repl_snapshot);
        conn->SetSyncSeq(max_seq + 1);
        conn->SetDbIndex(cur_db_idx);
    }
    
    conn->SetFlag(CLIENT_SLAVE);
    
    g_server.slave_mutex.lock();
    g_server.slaves.push_back(conn);
    g_server.slave_mutex.unlock();
}

void slaveof_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    if (!strcasecmp(cmd_vec[1].c_str(), "no") && !strcasecmp(cmd_vec[2].c_str(), "one")) {
        if (!g_server.master_host.empty()) {
            log_message(kLogLevelInfo, "MASTER MODE enabled (user request from %s:%d)\n",
                        conn->GetPeerIP(), conn->GetPeerPort());
            
            g_server.master_host.clear();
            g_server.master_port = 0;
            g_server.master_link_status.clear();
            if (g_server.master_handle != NETLIB_INVALID_HANDLE) {
                BaseConn::CloseHandle(g_server.master_handle);
                g_server.master_handle = NETLIB_INVALID_HANDLE;
            }
        }
    } else {
        long port;
        if (get_long_from_string(cmd_vec[2], port)) {
            conn->SendError("syntax error");
            return;
        }
        
        if (g_server.master_host == cmd_vec[1] && g_server.master_port == port) {
            log_message(kLogLevelInfo, "SLAVEOF the same master that we are already connected with\n");
            conn->SendSimpleString("OK Already connected to specified master");
            return;
        }
        
        g_server.master_host = cmd_vec[1];
        g_server.master_port = (int)port;
        g_server.master_link_status = "down";
        if (g_server.master_handle != NETLIB_INVALID_HANDLE) {
            BaseConn::CloseHandle(g_server.master_handle);
            g_server.master_handle = NETLIB_INVALID_HANDLE;
        }
        
        g_server.slave_mutex.lock();
        if (!g_server.slaves.empty()) {
            for (ClientConn* conn: g_server.slaves) {
                BaseConn::CloseHandle(conn->GetHandle());
            }
            g_server.slaves.clear();
        }
        g_server.slave_mutex.unlock();
        
        log_message(kLogLevelInfo, "SLAVEOF %s:%d enabled (user request from %s:%d)\n",
                    g_server.master_host.c_str(), g_server.master_port, conn->GetPeerIP(), conn->GetPeerPort());
    }
    
    conn->SendSimpleString("OK");
}

void repl_snapshot_complete_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    string binlog_id = cmd_vec[1];
    int cur_db_idx = atoi(cmd_vec[2].c_str());
    uint64_t binlog_seq = atol(cmd_vec[3].c_str());
    
    conn->SetState(CONN_STATE_CONNECTED);
    conn->SetDbIndex(cur_db_idx);
    g_server.binlog.InitWithMaster(binlog_id, cur_db_idx, binlog_seq);
    log_message(kLogLevelInfo, "receive all snapshot, binlog_id=%s, cur_db_idx=%d, binlog_seq=%llu\n",
                binlog_id.c_str(), cur_db_idx, binlog_seq);
}

void replication_cron()
{
    if ((g_server.master_handle == NETLIB_INVALID_HANDLE) && !g_server.master_host.empty()) {
        ClientConn* conn = new ClientConn();
        conn->SetFlag(CLIENT_MASTER);
        g_server.master_handle = conn->Connect(g_server.master_host, g_server.master_port);
        g_server.master_link_status = "connecting";
        log_message(kLogLevelInfo, "Connect to master %s:%d\n", g_server.master_host.c_str(), g_server.master_port);
    }
}

void prepare_snapshot_replication()
{
    // stop binlog purge thread and restart later, this can prevent the thread to process the destroyed db
    log_message(kLogLevelInfo, "Purge binlog thread stop\n");
    g_binlog_purge_thread.StopThread();
    g_server.binlog.Drop();
    g_server.binlog.Init();
    g_binlog_purge_thread.StartThread();
    
    spinlock_all();
    for (int db_idx = 0; db_idx < g_server.db_num; db_idx++) {
        generic_flushdb(db_idx);
    }
    unlock_all();
}

ReplicationSnapshot::ReplicationSnapshot(const rocksdb::Snapshot* snapshot)
{
    snapshot_ = snapshot;
    iterator_ = nullptr;
    cur_db_idx_ = 0;
    g_server.repl_snapshot_count++;
}

ReplicationSnapshot::~ReplicationSnapshot()
{
    if (iterator_) {
        delete iterator_;
    }
    
    if (snapshot_) {
        g_server.db->ReleaseSnapshot(snapshot_);
    }
    
    g_server.repl_snapshot_count--;
}

void ReplicationSnapshot::SendTo(ClientConn* conn)
{
    uint64_t now = get_tick_count();
    while (!conn->IsBusy()) {
        if (IsCompelte()) {
            return;
        }
        
        if (iterator_ == nullptr) {
            rocksdb::ReadOptions option;
            option.snapshot = snapshot_;
            iterator_ = g_server.db->NewIterator(option, g_server.cf_handles_map[cur_db_idx_]);
            assert(iterator_);
            
            rocksdb::Slice prefix((const char*)&KEY_TYPE_META, 1);
            iterator_->Seek(prefix);
            if (!iterator_->Valid() || (iterator_->key()[0] != KEY_TYPE_META)) {
                // this db number does not has any key
                log_message(kLogLevelDebug, "no key for db: %d\n", cur_db_idx_);
                delete iterator_;
                iterator_ = nullptr;
                cur_db_idx_++;
                continue;
            }
            
            string request;
            vector<string> cmd_vec = {"SELECT", to_string(cur_db_idx_)};
            build_request(cmd_vec, request);
            conn->Send((char*)request.data(), (int)request.size());
        }
    
        if (iterator_->Valid() && (iterator_->key()[0] == KEY_TYPE_META)) {
            try {
                uint8_t type;
                uint64_t ttl;
                uint64_t count = 0;
                const rocksdb::Slice& encode_value = iterator_->value();
                ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
                stream >> type;
                stream >> ttl;
                if (type != KEY_TYPE_STRING) {
                    stream >> count;
                }
                
                if (ttl && ttl <= now) {
                    iterator_->Next();
                    continue;
                }
                
                int ret = 0;
                string serialized_value;
                string key(iterator_->key().data() + 1, iterator_->key().size() - 1);
                if (type == KEY_TYPE_STRING) {
                    ret = dump_string(key, ttl, encode_value.ToString(), serialized_value);
                } else {
                    ret = dump_complex_struct(cur_db_idx_, type + 1, key, ttl, encode_value.ToString(), count,
                                              serialized_value, snapshot_);
                }
                
                if (ret == kExpireDBError) {
                    iterator_->Next();
                    continue;
                }
                
                // ttl in restore command is a relative time interval
                if (ttl) {
                    ttl -= now;
                }
                
                string request;
                vector<string> cmd_vec = {"RESTORE", key, to_string(ttl), serialized_value};
                build_request(cmd_vec, request);
                conn->Send((char*)request.data(), (int)request.size());
                iterator_->Next();
            } catch (ParseException ex) {
                log_message(kLogLevelError, "parse byte stream failed\n");
                iterator_->Next();
                continue;
            }
        } else {
            delete iterator_;
            iterator_ = nullptr;
            cur_db_idx_++;
        }
    }
}

bool ReplicationSnapshot::IsCompelte()
{
    return (cur_db_idx_ == g_server.db_num) ? true : false;
}
