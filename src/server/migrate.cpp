//
//  migrate.cpp
//  kedis
//
//  Created by ziteng on 17/9/18.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "migrate.h"
#include "migrate_conn.h"
#include "db_util.h"
#include "encoding.h"

struct MigrateContext {
    int from_handle; // handle for ClientConn
    uint64_t timeout;
    string key;
    int src_db_id;
    int dst_db_id;
    uint8_t key_type;
    uint64_t ttl;
    bool wait_select_reply;
    bool use_prefix;
};

struct MigrateServer {
    int handle; // handle for MigrateConn object
    int cur_db_index;
    bool is_connected;
    list<MigrateContext*> request_list;
};

map<string, MigrateServer*> g_migrate_server_map;
mutex g_migrate_mutex;

int deserialize_map(const string& serialized_value, map<string, string>& kv_map)
{
    ByteStream bs((uchar_t*)serialized_value.data(), (uint32_t)serialized_value.size());
    
    try {
        uint32_t count = bs.ReadVarUInt();
        for (uint32_t i = 0; i < count; i++) {
            string key, value;
            bs >> key;
            bs >> value;
            kv_map[key] = value;
        }
        
        return CODE_OK;
    } catch (ParseException& ex) {
        log_message(kLogLevelError, "deserialize failed: %s\n", ex.GetErrorMsg());
        return CODE_ERROR;
    }
}

void dump_ttl(const string& key, uint64_t ttl, uint8_t type, ByteStream& bs)
{
    if (ttl) {
        EncodeKey ttl_key(KEY_TYPE_TTL_SORT, ttl, key);
        rocksdb::Slice encode_ttl_key = ttl_key.GetEncodeKey();
        uchar_t encode_ttl_value = type;
        
        bs.WriteData((uchar_t*)encode_ttl_key.data(), (uint32_t)encode_ttl_key.size());
        bs.WriteData(&encode_ttl_value, 1);
    }
}

int dump_string(const string& key, uint64_t ttl, const string& encode_value, string& serialized_value)
{
    EncodeKey meta_key(KEY_TYPE_META, key);
    rocksdb::Slice encode_key = meta_key.GetEncodeKey();
    
    ByteStream bs(&serialized_value);
    uint32_t ttl_cnt = ttl ? 1 : 0;
    bs.WriteVarUInt(1 + ttl_cnt);
    bs.WriteData((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    bs.WriteData((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    dump_ttl(key, ttl, KEY_TYPE_STRING, bs);
    
    return kExpireKeyExist;
}

int dump_complex_struct(int db_idx, uint8_t type, const string& key, uint64_t ttl, const string& encode_value,
                        uint64_t count, string& serialized_value, const rocksdb::Snapshot* snapshot)
{
    EncodeKey meta_key(KEY_TYPE_META, key);
    rocksdb::Slice encode_key = meta_key.GetEncodeKey();
    
    ByteStream bs(&serialized_value);
    uint32_t ttl_cnt = ttl ? 1 : 0;
    uint32_t total_cnt = (uint32_t)count + 1 + ttl_cnt;
    if (type == KEY_TYPE_ZSET_SCORE) {
        total_cnt += (uint32_t)count;
    }
    bs.WriteVarUInt(total_cnt);
    bs.WriteData((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    bs.WriteData((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    uint64_t seek_cnt = 0;
    EncodeKey key_prefix(type, key);
    rocksdb::Slice encode_prefix = key_prefix.GetEncodeKey();
    rocksdb::ReadOptions read_option = g_server.read_option;
    read_option.snapshot = snapshot;
    rocksdb::Iterator* it = g_server.db->NewIterator(read_option, g_server.cf_handles_map[db_idx]);
    for (it->Seek(encode_prefix); it->Valid() && seek_cnt <  count; it->Next(), seek_cnt++) {
        if (!it->key().starts_with(encode_prefix)) {
            break;
        }
        bs.WriteData((uchar_t*)it->key().data(), (uint32_t)it->key().size());
        bs.WriteData((uchar_t*)it->value().data(), (uint32_t)it->value().size());
    }
    
    if (type == KEY_TYPE_ZSET_SCORE) {
        if (seek_cnt != count) {
            log_message(kLogLevelError, "key=%s was corrupted\n", key.c_str());
            delete it;
            return kExpireDBError;
        }
        
        seek_cnt = 0;
        EncodeKey sort_key_prefix(KEY_TYPE_ZSET_SORT, key);
        rocksdb::Slice sort_encode_prefix = sort_key_prefix.GetEncodeKey();
        for (it->Seek(sort_encode_prefix); it->Valid() && seek_cnt <  count; it->Next(), seek_cnt++) {
            if (!it->key().starts_with(sort_encode_prefix)) {
                break;
            }
            bs.WriteData((uchar_t*)it->key().data(), (uint32_t)it->key().size());
            bs.WriteData((uchar_t*)it->value().data(), (uint32_t)it->value().size());
        }
    }
    
    delete it;
    if (seek_cnt == count) {
        // key_type in TTL value must the meta key type, so need minus 1
        dump_ttl(key, ttl, type - 1, bs);
        return kExpireKeyExist;
    } else {
        log_message(kLogLevelError, "key=%s was corrupted\n", key.c_str());
        return kExpireDBError;
    }
}

int dump_key(int db_idx, string& key, uint64_t& ttl, uint8_t& key_type, string& serialized_value, bool use_prefix = false)
{
    if (use_prefix) {
        EncodeKey key_prefix(KEY_TYPE_META, key);
        rocksdb::Slice encode_prefix = key_prefix.GetEncodeKey();
        ScanKeyGuard scan_key_guard(db_idx);
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, g_server.cf_handles_map[db_idx]);
        
        it->Seek(encode_prefix);
        if (it->Valid() && it->key().starts_with(encode_prefix)) {
            // rewrite the key to MigrateContext so after migrate complete, the key will be deleted
            key.clear();
            key.append(it->key().data() + 1, it->key().size() - 1);
            delete it;
        } else {
            delete it;
            return kExpireKeyNotExist;
        }
    }
    
    log_message(kLogLevelDebug, "dump real_key=%s\n", key.c_str());
    MetaData mdata;
    string encode_value;
    KeyLockGuard lock_guard(db_idx, key);
    int ret = expire_key_if_needed(db_idx, key, mdata, &encode_value);
    if (ret == kExpireKeyExist) {
        ttl = mdata.ttl;
        key_type = mdata.type;
        switch (mdata.type) {
            case KEY_TYPE_STRING:
                return dump_string(key, ttl, encode_value, serialized_value);
            case KEY_TYPE_HASH:
            case KEY_TYPE_LIST:
            case KEY_TYPE_SET:
            case KEY_TYPE_ZSET:
                return dump_complex_struct(db_idx, mdata.type + 1, key, ttl, encode_value, mdata.count, serialized_value);
            default:
                log_message(kLogLevelError, "no such data type: %d\n", mdata.type);
                return kExpireDBError;
        }
    }
    
    return ret;
}

int migrate_key(MigrateServer* server, MigrateContext* context)
{
    log_message(kLogLevelDebug, "send migrate key=%s\n", context->key.c_str());

    string serialized_value;
    int ret = dump_key(context->src_db_id, context->key, context->ttl, context->key_type, serialized_value, context->use_prefix);
    if (ret == kExpireDBError) {
        BaseConn::Send(context->from_handle, (char*)kIoErrorString.data(), (int)kIoErrorString.size());
        return CODE_ERROR;
    } else if (ret == kExpireKeyNotExist) {
        BaseConn::Send(context->from_handle, (char*)kNoKeyString.data(), (int)kNoKeyString.size());
        return CODE_ERROR;
    } else {
        if (server->cur_db_index != context->dst_db_id) {
            // migrate destination db is not the same with current db, send select db first
            vector<string> cmd_vec = {"select", to_string(context->dst_db_id)};
            string request;
            build_request(cmd_vec, request);
            BaseConn::Send(server->handle, (void*)request.data(), (int)request.size());
            
            context->wait_select_reply = true;
            server->cur_db_index = context->dst_db_id;
        }
        
        vector<string> restore_cmd_vec = {"RESTORE", context->key, to_string(context->ttl), serialized_value};
        string restore_req;
        build_request(restore_cmd_vec, restore_req);
        BaseConn::Send(server->handle, (char*)restore_req.data(), (int)restore_req.size());
        return CODE_OK;
    }
}

int get_migrate_conn_number()
{
    lock_guard<mutex> guard(g_migrate_mutex);
    return (int)g_migrate_server_map.size();
}

void migrate_conn_established(const string& addr)
{
    log_message(kLogLevelInfo, "migrate connection to %s established\n", addr.c_str());
    
    lock_guard<mutex> guard(g_migrate_mutex);
    auto it = g_migrate_server_map.find(addr);
    if (it != g_migrate_server_map.end()) {
        MigrateServer* server = it->second;
        server->is_connected = true;
        
        for (auto req_it = server->request_list.begin(); req_it != server->request_list.end(); ) {
            MigrateContext* context = *req_it;
            auto old_it = req_it;
            ++req_it;
            
            if (migrate_key(server, context) == CODE_ERROR) {
                server->request_list.erase(old_it);
                delete context;
            }
        }
    }
}

void migrate_conn_closed(const string& addr)
{
    log_message(kLogLevelInfo, "migrate connection to %s closed\n", addr.c_str());
    
    lock_guard<mutex> guard(g_migrate_mutex);
    auto it = g_migrate_server_map.find(addr);
    if (it != g_migrate_server_map.end()) {
        MigrateServer* server = it->second;

        for (auto req_it = server->request_list.begin(); req_it != server->request_list.end(); ++req_it) {
            MigrateContext* context = *req_it;
            BaseConn::Send(context->from_handle, (char*)kIoErrorString.data(), (int)kIoErrorString.size());
            delete context;
        }
        
        g_migrate_server_map.erase(it);
        delete server;
    }
}

void continue_migrate_command(const string& addr, const RedisReply& reply)
{
    lock_guard<mutex> guard(g_migrate_mutex);
    auto it = g_migrate_server_map.find(addr);
    if (it != g_migrate_server_map.end()) {
        MigrateServer* server = it->second;
        if (server->request_list.empty()) {
            log_message(kLogLevelError, "receive response from migration connection, but no request found\n");
            return;
        }
        
        MigrateContext* context = server->request_list.front();
        if (context->wait_select_reply) {
            context->wait_select_reply = false;
            return;
        }
        
        string respone = reply.GetStrValue();
        if (reply.GetType() == REDIS_TYPE_ERROR) {
            respone = "-IOERROR " + respone + "\r\n";
        } else {
            respone = "+" + respone + "\r\n";
        }
        
        BaseConn::Send(context->from_handle, (char*)respone.data(), (int)respone.size());
        
        if (reply.GetType() == REDIS_TYPE_STATUS) {
            KeyLockGuard key_lock_guard(context->src_db_id, context->key);
            
            delete_key(context->src_db_id, context->key, context->ttl, context->key_type);
            vector<string> cmd_vec = {"del", context->key};
            string command;
            build_request(cmd_vec, command);
            g_server.binlog.Store(context->src_db_id, command);
        }
        
        server->request_list.pop_front();
        delete context;
    }
}

// MIGRATE host port key destination_db timeout [use_prefix]
void migrate_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    bool use_prefix = false;
    if (cmd_size == 6) {
        ;
    } else if ((cmd_size == 7) && (!strcasecmp(cmd_vec[6].c_str(), "use_prefix"))) {
        use_prefix = true;
    } else {
        conn->SendError("syntax error");
        return;
    }
    
    long port, db_id, timeout;
    if ((get_long_from_string(cmd_vec[2], port) == CODE_ERROR) ||
        (get_long_from_string(cmd_vec[4], db_id) == CODE_ERROR) ||
        (get_long_from_string(cmd_vec[5], timeout) == CODE_ERROR) ) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    const string& key = cmd_vec[3];
    string addr = cmd_vec[1] + ":" + cmd_vec[2];
    
    // there are three scenarios for migrate:
    // 1. the connection to the destination is not exist
    // 2. the connection alread exist but is still connecting
    // 3. the connection already exist and is connected
    MigrateContext* context = new MigrateContext;
    context->from_handle = conn->GetHandle();
    context->timeout = get_tick_count() + timeout;
    context->key = key;
    context->src_db_id = conn->GetDBIndex();
    context->dst_db_id = (int)db_id;
    context->wait_select_reply = false;
    context->use_prefix = use_prefix;
    
    // first case
    lock_guard<mutex> guard(g_migrate_mutex);
    auto it = g_migrate_server_map.find(addr);
    if (it == g_migrate_server_map.end()) {
        MigrateServer* server = new MigrateServer;
        MigrateConn* mig_conn = new MigrateConn();
        server->handle = mig_conn->Connect(cmd_vec[1], port);
        if (server->handle == NETLIB_INVALID_HANDLE) {
            delete server;
            delete context;
            // mig_conn will delete itself when Connect failed, so do not need to be deleted here
            conn->SendRawResponse(kIoErrorString);
            return;
        }
        
        mig_conn->SetAddr(addr);
        server->is_connected = false;
        server->cur_db_index = 0;
        server->request_list.push_back(context);
        g_migrate_server_map[addr] = server;
        return;
    }
    
    // second case
    MigrateServer* server = it->second;
    if (!server->is_connected) {
        server->request_list.push_back(context);
        return;
    }
    
    // third case
    if (migrate_key(server, context) == CODE_OK) {
        server->request_list.push_back(context);
    } else {
        delete context;
    }
}

// RESTORE key ttl serialized-value
void restore_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long ttl;
    if (get_long_from_string(cmd_vec[2], ttl) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    map<string, string> kv_map;
    if (deserialize_map(cmd_vec[3], kv_map) == CODE_ERROR) {
        conn->SendError("deserialize error");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    rocksdb::WriteBatch batch;
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    for (auto it = kv_map.begin(); it != kv_map.end(); it++) {
        batch.Put(cf_handle, it->first, it->second);
    }
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireKeyExist) {
        delete_key(db_idx, cmd_vec[1], mdata.ttl, mdata.type);
    }
    
    g_server.key_count_vec[db_idx]++;
    if (ttl > 0) {
        g_server.ttl_key_count_vec[db_idx]++;
    }
    DB_BATCH_UPDATE(batch)
    if (conn->GetState() != CONN_STATE_CONNECTED) {
        return;
    }
    g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    conn->SendSimpleString("OK");
}

void dump_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    uint64_t ttl;
    uint8_t key_type;
    string serialized_value;
    string key = cmd_vec[1];
    int ret = dump_key(conn->GetDBIndex(), key, ttl, key_type, serialized_value);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kIoErrorString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        conn->SendBulkString(serialized_value);
    }
}
