//
//  binlog.cpp
//  kedis
//
//  Created by ziteng on 17/9/12.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "binlog.h"
#include "server.h"
#include "byte_stream.h"

uint64_t kMaxPurgeSize = 1000;

void PurgeBinlogThread::OnThreadRun(void) {
    log_message(kLogLevelInfo, "purge binlog thread start\n");
    m_running = true;
    while (m_running) {
        usleep(10000);  // purge the binlog every 10ms
        g_server.binlog.Purge();
    }
}

Binlog::Binlog()
{
    db_ = nullptr;
    min_seq_ = max_seq_ = 0;
    cur_db_idx_ = 0;
    empty_ = true;
}

Binlog::~Binlog()
{
    if (db_ != nullptr) {
        delete db_;
    }
}

int Binlog::Init()
{
    if (db_) {
        return CODE_OK;
    }
    
    rocksdb::Status s = rocksdb::DB::Open(g_server.options, g_server.binlog_dir, &db_);
    if (!s.ok()) {
        log_message(kLogLevelError, "Open DB for binlog failed: %s", s.ToString().c_str());
        return CODE_ERROR;
    }
    
    string db_idx_str;
    s = db_->Get(g_server.read_option, "CUR_DB_IDX", &db_idx_str);
    if (s.ok()) {
        empty_ = false;
        cur_db_idx_ = atoi(db_idx_str.c_str());
        db_->Get(g_server.read_option, "BINLOG_ID", &binlog_id_);
        
        rocksdb::Iterator* it = db_->NewIterator(g_server.read_option);
        if (!it) {
            log_message(kLogLevelError, "New iterator failed\n");
            return CODE_ERROR;
        }
        
        string key = EncodeKey(0);
        it->Seek(key);
        if (it->Valid()) {
            min_seq_ = DecodeKey(it->key());
            
            key = EncodeKey(UINT64_MAX);
            it->SeekForPrev(key);
            if (it->Valid()) {
                max_seq_ = DecodeKey(it->key());
            } else {
                log_message(kLogLevelError, "seek failed for max_seq\n");
                delete it;
                return CODE_ERROR;
            }
        } else {
            // this is the situation that it has a full syncronization without any binlog update, and restart again
            string str_max_seq;
            db_->Get(g_server.read_option, "MAX_SEQ", &str_max_seq);
            min_seq_ = max_seq_ = atol(str_max_seq.c_str());
        }
        
        delete it;
        log_message(kLogLevelDebug, "binlog id=%s cur_db_idx=%d, min_seq=%llu, max_seq=%llu\n",
                    binlog_id_.c_str(), cur_db_idx_, min_seq_, max_seq_);
    } else if (s.IsNotFound()) {
        empty_ = true;
        GenerateBinlogId();
        db_->Put(g_server.write_option, "BINLOG_ID", binlog_id_);
        log_message(kLogLevelDebug, "a brand new binlog\n");
    } else {
        log_message(kLogLevelError, "get CUR_DB_IDX failed: %s", s.ToString().c_str());
        return CODE_ERROR;
    }
    
    return CODE_OK;
}

void Binlog::InitWithMaster(const string& binlog_id, int cur_db_idx, uint64_t seq)
{
    binlog_id_ = binlog_id;
    cur_db_idx_ = cur_db_idx;
    min_seq_ = max_seq_ = seq;
    empty_ = false;
    
    rocksdb::WriteBatch batch;
    batch.Put("BINLOG_ID", binlog_id_);
    batch.Put("CUR_DB_IDX", to_string(cur_db_idx));
    batch.Put("MAX_SEQ", to_string(max_seq_));
    db_->Write(g_server.write_option, &batch);
}

void Binlog::Drop()
{
    if (db_) {
        delete db_;
        db_ = nullptr;
    }
    
    cur_db_idx_ = 0;
    min_seq_ = max_seq_ = 0;
    empty_ = true;
    drop_db(g_server.binlog_dir);
}

int Binlog::Store(int db_idx, const string& command)
{
    bool update_db_idx = false;
    uint64_t update_db_seq = 0;
    
    mtx_.lock();
    if (empty_) {
        empty_ = false;
        ++min_seq_;
        cur_db_idx_ = db_idx;
        update_db_idx = true;
        update_db_seq = ++max_seq_;
    } else if (db_idx != cur_db_idx_) {
        cur_db_idx_ = db_idx;
        update_db_idx = true;
        update_db_seq = ++max_seq_;
    }
    uint64_t cmd_seq = ++max_seq_;
    mtx_.unlock();
    
    if (update_db_idx) {
        rocksdb::WriteBatch batch;
        batch.Put("CUR_DB_IDX", to_string(db_idx));
        
        string select_key = EncodeKey(update_db_seq);
        string select_command;
        vector<string> select_cmd_vec = {"SELECT", to_string(db_idx)};
        build_request(select_cmd_vec, select_command);
        
        batch.Put(select_key, select_command);
        
        string key = EncodeKey(cmd_seq);
        batch.Put(key, command);
        
        db_->Write(g_server.write_option, &batch);
    } else {
        string key = EncodeKey(cmd_seq);
        db_->Put(g_server.write_option, key, command);
    }
    
    return CODE_OK;
}

int Binlog::Extract(uint64_t seq, string& command)
{
    string key = EncodeKey(seq);
    rocksdb::Status s = db_->Get(g_server.read_option, key, &command);
    if (s.ok()) {
        return CODE_OK;
    } else {
        log_message(kLogLevelError, "binlog extract seq=%llu failed: %s\n", seq, s.ToString().c_str());
        return CODE_ERROR;
    }
}

void Binlog::Purge()
{
    if (!db_)
        return;
    
    uint64_t min_sync_seq = 0;
    g_server.slave_mutex.lock();
    for (ClientConn* conn: g_server.slaves) {
        if (conn->GetSyncSeq() < min_sync_seq) {
            min_sync_seq = conn->GetSyncSeq();
        }
    }
    g_server.slave_mutex.unlock();
    
    uint64_t binlog_size = max_seq_ - min_seq_;
    if ((int)binlog_size > g_server.binlog_capacity) {
        rocksdb::WriteBatch batch;
        uint64_t purge_size = binlog_size - g_server.binlog_capacity;
        if (min_sync_seq && (purge_size > min_sync_seq - min_seq_)) {
            purge_size = min_sync_seq - min_seq_;
        }
        
        if (purge_size == 0) {
            return;
        }
        
        if (purge_size > kMaxPurgeSize) {
            purge_size = kMaxPurgeSize;
        }
        
        for (uint64_t seq = min_seq_; seq < min_seq_ + purge_size; seq++) {
            string key = EncodeKey(seq);
            batch.Delete(key);
        }
        
        db_->Write(g_server.write_option, &batch);
        
        min_seq_ += purge_size;
        log_message(kLogLevelDebug, "binlog min_seq=%llu, max_seq=%llu\n", min_seq_, max_seq_);
    }
}

void Binlog::GenerateBinlogId()
{
    // binlog_id will be read from the binlog, if there is no such key named BINLOG_ID,
    // use this method to generate a random 40 byte id
    if (!binlog_id_.empty()) {
        return;
    }
    
    const int kIdlen = 40;
    const char *charset = "0123456789abcdef";
    char buf[kIdlen] = {0};
    char *x = buf;
    unsigned int l = kIdlen;
    struct timeval tv;
    pid_t pid = getpid();
    
    /* Use time and PID to fill the initial array. */
    gettimeofday(&tv,NULL);
    if (l >= sizeof(tv.tv_usec)) {
        memcpy(x,&tv.tv_usec,sizeof(tv.tv_usec));
        l -= sizeof(tv.tv_usec);
        x += sizeof(tv.tv_usec);
    }
    if (l >= sizeof(tv.tv_sec)) {
        memcpy(x,&tv.tv_sec,sizeof(tv.tv_sec));
        l -= sizeof(tv.tv_sec);
        x += sizeof(tv.tv_sec);
    }
    if (l >= sizeof(pid)) {
        memcpy(x,&pid,sizeof(pid));
        l -= sizeof(pid);
        x += sizeof(pid);
    }
    /* Finally xor it with rand() output, that was already seeded with
     * time() at startup, and convert to hex digits. */
    for (int j = 0; j < kIdlen; j++) {
        buf[j] ^= rand();
        buf[j] = charset[buf[j] & 0x0F];
    }
    
    binlog_id_.append(buf, kIdlen);
    log_message(kLogLevelInfo, "binlog_id=%s\n", binlog_id_.c_str());
}

string Binlog::EncodeKey(uint64_t seq)
{
    string key;
    ByteStream bs(&key);
    bs << (uint8_t)'S';
    bs << seq;
    return key;
}

uint64_t Binlog::DecodeKey(const rocksdb::Slice& slice)
{
    assert(slice.size() == 9);
    uint8_t type;
    uint64_t seq;
    ByteStream bs((uchar_t*)slice.data(), (uint32_t)slice.size());
    bs >> type;
    bs >> seq;
    return seq;
}
