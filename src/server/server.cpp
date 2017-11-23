//
//  server.cpp
//  kedis
//
//  Created by ziteng on 17/7/18.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "server.h"
#include "simple_log.h"
#include "base_listener.h"
#include "event_loop.h"
#include "client_conn.h"
#include "cmd_db.h"
#include "cmd_hash.h"
#include "cmd_keys.h"
#include "cmd_list.h"
#include "cmd_set.h"
#include "cmd_string.h"
#include "cmd_zset.h"
#include "hyperloglog.h"
#include "replication.h"
#include "migrate.h"
#include "slowlog.h"
#include "key_lock.h"
#include "config.h"
#include "encoding.h"
#include "db_util.h"
#include "expire_thread.h"

KedisServer g_server;
KedisStat g_stat;
ExpireThread g_expire_thread;
PurgeBinlogThread g_binlog_purge_thread;

void init_server_config()
{
    g_server.config_file = "./kedis.conf";
    g_server.port = 6379;
    g_server.daemonize = false;
    g_server.client_timeout = 0;
    g_server.pid_file = "kedis.pid";
    g_server.log_level = kLogLevelInfo;
    g_server.log_path = "log";
    g_server.io_thread_num = 16;
    g_server.db_name = "kdb";
    g_server.db_num = 16;
    g_server.binlog_dir = "binlog";
    g_server.binlog_capacity = 100000;
    g_server.master_port = 0;
    g_server.slave_read_only = false;
    g_server.repl_timeout = 60;
    g_server.require_pass = "";
    g_server.max_clients = 10000;
    g_server.slowlog_log_slower_than = 10; // milliseconds
    g_server.slowlog_max_len = 128;
    g_server.hll_sparse_max_bytes = 3000;
    
    g_server.db = NULL;
    g_server.master_handle = NETLIB_INVALID_HANDLE;
    g_server.master_link_status = "down";
    g_server.client_num = 0;
    g_server.repl_snapshot_count = 0;
    
    g_server.kedis_commands = {
        // DB commands
        {"AUTH", {auth_command, 2, false}},
        {"PING", {ping_command, 1, false}},
        {"ECHO", {echo_command, 2, false}},
        {"SELECT", {select_command, 2, false}},
        {"DBSIZE", {dbsize_command, 1, false}},
        {"INFO", {info_command, -1, false}},
        {"FLUSHDB", {flushdb_command, 1, false}},
        {"FLUSHALL", {flushall_command, 1, false}},
        {"DEBUG", {debug_command, -2, false}},
        {"COMMAND", {command_command, 1, false}},
        {"CONFIG", {config_command, -2, false}},
        
        // replication
        {"REPLCONF", {replconf_command, -3, false}},
        {"PSYNC", {psync_command, 3, false}},
        {"SLAVEOF", {slaveof_command, 3, false}},
        {"REPL_SNAPSHOT_COMPLETE", {repl_snapshot_complete_command, 4, false}},
        
        // migrate
        {"MIGRATE", {migrate_command, -6, true}},
        {"RESTORE", {restore_command, 4, true}},
        {"DUMP", {dump_command, 2, false}},
        
        // keys commands
        {"DEL", {del_command, -2, true}},
        {"EXISTS", {exists_command, 2, false}},
        {"TTL", {ttl_command, 2, false}},
        {"PTTL", {pttl_command, 2, false}},
        {"TYPE", {type_command, 2, false}},
        {"EXPIRE", {expire_command, 3, true}},
        {"EXPIREAT", {expireat_command, 3, true}},
        {"PEXPIRE", {pexpire_command, 3, true}},
        {"PEXPIREAT", {pexpireat_command, 3, true}},
        {"PERSIST", {persist_command, 2, true}},
        {"RANDOMKEY", {randomkey_command, 1, false}},
        {"KEYS", {keys_command, 2, false}},
        {"SCAN", {scan_command, -2, false}},
        
        // string commands
        {"GET", {get_command, 2, false}},
        {"GETSET", {getset_command, 3, true}},
        {"SET", {set_command, -3, true}},
        {"SETEX", {setex_command, 4, true}},
        {"SETNX", {setnx_command, 3, true}},
        {"PSETEX", {psetex_command, 4, true}},
        {"MSET", {mset_command, -3, true}},
        {"MSETNX", {msetnx_command, -3, true}},
        {"MGET", {mget_command, -2, false}},
        {"APPEND", {append_command, 3, true}},
        {"GETRANGE", {getrange_command, 4, false}},
        {"SETRANGE", {setrange_command, 4, true}},
        {"STRLEN", {strlen_command, 2, false}},
        {"INCR", {incr_command, 2, true}},
        {"DECR", {decr_command, 2, true}},
        {"INCRBY", {incrby_command, 3, true}},
        {"DECRBY", {decrby_command, 3, true}},
        {"INCRBYFLOAT", {incrbyfloat_command, 3, true}},
        {"SETBIT", {setbit_command, 4, true}},
        {"GETBIT", {getbit_command, 3, false}},
        {"BITCOUNT", {bitcount_command, -2, false}},
        {"BITPOS", {bitpos_command, -3, false}},
        
        // hash commands
        {"HDEL", {hdel_command, -3, true}},
        {"HEXISTS", {hexists_command, 3, false}},
        {"HGET", {hget_command, 3, false}},
        {"HGETALL", {hgetall_command, 2, false}},
        {"HKEYS", {hkeys_command, 2, false}},
        {"HVALS", {hvals_command, 2, false}},
        {"HINCRBY", {hincrby_command, 4, true}},
        {"HINCRBYFLOAT", {hincrbyfloat_command, 4, true}},
        {"HLEN", {hlen_command, 2, false}},
        {"HMGET", {hmget_command, -3, false}},
        {"HMSET", {hmset_command, -4, true}},
        {"HSCAN", {hscan_command, -3, false}},
        {"HSET", {hset_command, 4, true}},
        {"HSETNX", {hsetnx_command, 4, true}},
        {"HSTRLEN", {hstrlen_command, 3, false}},
        
        // list commands
        {"LINDEX", {lindex_command, 3, false}},
        {"LINSERT", {linsert_command, 5, true}},
        {"LLEN", {llen_command, 2, false}},
        {"LPUSH", {lpush_command, -3, true}},
        {"LPOP", {lpop_command, 2, true}},
        {"LRANGE", {lrange_command, 4, false}},
        {"RPUSH", {rpush_command, -3, true}},
        {"RPOP", {rpop_command, 2, true}},
        {"LPUSHX", {lpushx_command, 3, true}},
        {"RPUSHX", {rpushx_command, 3, true}},
        {"LREM", {lrem_command, 4, true}},
        {"LSET", {lset_command, 4, true}},
        {"LTRIM", {ltrim_command, 4, true}},
        
        // set commands
        {"SADD", {sadd_command, -3, true}},
        {"SCARD", {scard_command, 2, false}},
        {"SISMEMBER", {sismember_command, 3, false}},
        {"SMEMBERS", {smembers_command, 2, false}},
        {"SPOP", {spop_command, -2, true}},
        {"SRANDMEMBER", {srandmember_command, -2, false}},
        {"SREM", {srem_command, -3, true}},
        {"SSCAN", {sscan_command, -3, false}},
        
        // zset commands
        {"ZADD", {zadd_command, -4, true}},
        {"ZINCRBY", {zincrby_command, 4, true}},
        {"ZCARD", {zcard_command, 2, false}},
        {"ZCOUNT", {zcount_command, 4, false}},
        {"ZRANGE", {zrange_command, -4, false}},
        {"ZREVRANGE", {zrevrange_command, -4, false}},
        {"ZRANGEBYSCORE", {zrangebyscore_command, -4, false}},
        {"ZREVRANGEBYSCORE", {zrevrangebyscore_command, -4, false}},
        {"ZRANGEBYLEX", {zrangebylex_command, -4, false}},
        {"ZREVRANGEBYLEX", {zrevrangebylex_command, -4, false}},
        {"ZLEXCOUNT", {zlexcount_command, 4, false}},
        {"ZRANK", {zrank_command, 3, false}},
        {"ZREVRANK", {zrevrank_command, 3, false}},
        {"ZREM", {zrem_command, -3, true}},
        {"ZREMRANGEBYLEX", {zremrangebylex_command, 4, true}},
        {"ZREMRANGEBYRANK", {zremrangebyrank_command, 4, true}},
        {"ZREMRANGEBYSCORE", {zremrangebyscore_command, 4, true}},
        {"ZSCORE", {zscore_command, 3, false}},
        {"ZSCAN", {zscan_command, -3, false}},
        
        // hyperloglog
        {"PFADD", {pfadd_command, -2, true}},
        {"PFCOUNT", {pfcount_command, -2, false}},
        {"PFMERGE", {pfmerge_command, -3, true}},
        
        // slowlog
        {"SLOWLOG", {slowlog_command, -2, false}},
    };
}

void init_rocksdb_options()
{
    g_server.write_option.disableWAL = true;
    
    g_server.options.create_if_missing = true;
    
    // optimise RocksDB options, reference:
    // 1. RocksDB Tuning Guide (https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
    // 2. Optimization of RocksDB for Redis on Flash
    // 3. Fine-tuning RocksDB for NVMe SSD
    g_server.options.max_background_jobs = g_server.db_options.max_background_jobs = 8;
    g_server.options.compaction_readahead_size = g_server.db_options.compaction_readahead_size = 2 << 20;
    g_server.options.max_bytes_for_level_base = g_server.cf_options.max_bytes_for_level_base = 2 << 30;
    g_server.options.target_file_size_base = g_server.cf_options.target_file_size_base = 256 << 20;
    g_server.options.write_buffer_size = g_server.cf_options.write_buffer_size = 512 << 20;
}

void create_db()
{
    rocksdb::Status status;
    
    vector<string> cf_names;
    rocksdb::DB::ListColumnFamilies(g_server.db_options, g_server.db_name, &cf_names);
    if (cf_names.empty()) {
        status = rocksdb::DB::Open(g_server.options, g_server.db_name, &g_server.db);
        if (!status.ok()) {
            log_message(kLogLevelError, "Open DB faild: %s\n", status.ToString().c_str());
            exit(1);
        }
    } else {
        vector<rocksdb::ColumnFamilyDescriptor> column_families;
        for (auto name : cf_names) {
            log_message(kLogLevelInfo, "column family name=%s\n", name.c_str());
            column_families.push_back(rocksdb::ColumnFamilyDescriptor(name, g_server.cf_options));
        }
        
        vector<rocksdb::ColumnFamilyHandle*> cf_handles;
        status = rocksdb::DB::Open(g_server.db_options, g_server.db_name, column_families,
                                   &cf_handles, &g_server.db);
        if (!status.ok()) {
            log_message(kLogLevelError, "Open DB faild: %s\n", status.ToString().c_str());
            exit(1);
        }
        
        for (int i = 0; i < (int)cf_names.size(); i++) {
            string name = cf_names[i];
            if (name != rocksdb::kDefaultColumnFamilyName) {
                int db_index = atoi(name.c_str());
                g_server.cf_handles_map[db_index] = cf_handles[i];
            }
        }
    }
    
    for (int i = 0; i < g_server.db_num; i++) {
        auto it = g_server.cf_handles_map.find(i);
        if (it == g_server.cf_handles_map.end()) {
            rocksdb::ColumnFamilyHandle *cf_handle;
            string cf_name = to_string(i);
            g_server.db->CreateColumnFamily(g_server.cf_options, cf_name, &cf_handle);
            g_server.cf_handles_map[i] = cf_handle;
        }
    }
}

void close_db()
{
    for (auto it = g_server.cf_handles_map.begin(); it != g_server.cf_handles_map.end(); ++it) {
        delete it->second;  // delete column family handles
    }
    g_server.cf_handles_map.clear();
    
    if (g_server.db) {
        delete g_server.db;
        g_server.db = NULL;
    }
}

void drop_db(const string& db_path)
{
    string rm_cmd = "rm -fr " + db_path;
    system(rm_cmd.c_str());
}

void init_key_count()
{
    g_server.key_count_vec = new atomic<long> [g_server.db_num];
    g_server.ttl_key_count_vec = new atomic<long> [g_server.db_num];
    g_server.operating_count_vec = new atomic<long> [g_server.db_num];
    for (int i = 0; i < g_server.db_num; i++) {
        g_server.key_count_vec[i] = 0;
        g_server.ttl_key_count_vec[i] = 0;
        g_server.operating_count_vec[i] = 0;
    }
    
    // get key count from db
    rocksdb::Status s;
    EncodeKey prefix_key(KEY_TYPE_META, "");
    EncodeKey encode_key_count(KEY_TYPE_STATS, "KEY_COUNT");
    EncodeKey encode_ttl_key_count(KEY_TYPE_STATS, "TTL_KEY_COUNT");
    for (int i = 0; i < g_server.db_num; i++) {
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[i];
        string key_count;
        string ttl_key_count;
        s = g_server.db->Get(g_server.read_option, cf_handle, encode_key_count.GetEncodeKey(), &key_count);
        if (s.ok()) {
            g_server.key_count_vec[i] = atol(key_count.c_str());
        }
        
        s = g_server.db->Get(g_server.read_option, cf_handle, encode_ttl_key_count.GetEncodeKey(), &ttl_key_count);
        if (s.ok()) {
            g_server.ttl_key_count_vec[i] = atol(ttl_key_count.c_str());
        }
        
        if (g_server.key_count_vec[i] < MAX_KEY_ITERATOR_COUNT) {
            long key_num = 0;
            long ttl_num = 0;
            rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
            for (it->Seek(prefix_key.GetEncodeKey()); it->Valid(); it->Next()) {
                if (it->key()[0] != KEY_TYPE_META) {
                    break;
                }
                
                ByteStream stream((uchar_t*)it->value().data(), (uint32_t)it->value().size());
                try {
                    uint8_t key_type;
                    uint64_t ttl;
                    stream >> key_type;
                    stream >> ttl;
                    if (ttl) {
                        if (ttl <= get_tick_count()) {
                            string key(it->key().data() + 1, it->key().size() - 1);
                            delete_key(i, key, ttl, key_type);
                        } else {
                            ttl_num++;
                            key_num++;
                        }
                    } else {
                        key_num++;
                    }
                } catch (ParseException ex) {
                    log_message(kLogLevelError, "db=%d error in init_key_count()\n", i);
                    exit(1);
                }
            }
            delete it;
            
            g_server.key_count_vec[i] = key_num;
            g_server.ttl_key_count_vec[i] = ttl_num;
        }
    }
}

void persist_key_count()
{
    EncodeKey encode_key_count(KEY_TYPE_STATS, "KEY_COUNT");
    EncodeKey encode_ttl_key_count(KEY_TYPE_STATS, "TTL_KEY_COUNT");
    for (int i = 0; i < g_server.db_num; i++) {
        ScanKeyGuard scan_key_guard(i);
        
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[i];
        g_server.db->Put(g_server.write_option, cf_handle, encode_key_count.GetEncodeKey(), to_string(g_server.key_count_vec[i]));
        g_server.db->Put(g_server.write_option, cf_handle, encode_ttl_key_count.GetEncodeKey(),
                         to_string(g_server.ttl_key_count_vec[i]));
    }
}

void init_server()
{
    init_key_lock();
    
    create_db();
    init_key_count();
    g_server.binlog.Init();
    g_stat.Reset();
}

// Add a sample to the operations per second array of samples
void track_instantaneous_metric(int metric, long long current_reading)
{
    uint64_t current_tick = get_tick_count();
    uint64_t t = current_tick - g_stat.inst_metric[metric].last_sample_time;
    long ops = current_reading - g_stat.inst_metric[metric].last_sample_count;
    long ops_sec = t > 0 ? (ops * 1000/ t) : 0;
    
    g_stat.inst_metric[metric].samples[g_stat.inst_metric[metric].idx] = ops_sec;
    g_stat.inst_metric[metric].idx++;
    g_stat.inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    g_stat.inst_metric[metric].last_sample_time = current_tick;
    g_stat.inst_metric[metric].last_sample_count = current_reading;
}

// Return the mean of all the samples
long get_instantaneous_metric(int metric)
{
    long sum = 0;
    for (int j = 0; j < STATS_METRIC_SAMPLES; j++) {
        sum += g_stat.inst_metric[metric].samples[j];
    }
    return sum / STATS_METRIC_SAMPLES;
}

void server_cron(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    replication_cron();
    
    persist_key_count();
    
    track_instantaneous_metric(STATS_METRIC_COMMAND, g_stat.total_commands_processed);
    track_instantaneous_metric(STATS_METRIC_NET_INPUT, BaseConn::GetTotalNetInputBytes());
    track_instantaneous_metric(STATS_METRIC_NET_OUTPUT, BaseConn::GetTotalNetOutputBytes());
}

void shutdown_signal_handler(int sig_no)
{
    log_message(kLogLevelInfo, "kedis-server stopping...\n");
    
    destroy_thread_event_loops(g_server.io_thread_num);
    destroy_thread_base_conn(g_server.io_thread_num);
    g_expire_thread.StopThread();
    g_binlog_purge_thread.StopThread();
    
    persist_key_count();
    
    close_db();
    
    delete [] g_server.key_count_vec;
    delete [] g_server.ttl_key_count_vec;
    
    remove(g_server.pid_file.c_str());
    _exit(0);
}

int main(int argc, char* argv[])
{
    signal(SIGTERM, shutdown_signal_handler);
    signal(SIGINT, shutdown_signal_handler);
    
    init_server_config();
    init_rocksdb_options();
    parse_command_args(argc, argv, KEDIS_VERSION, g_server.config_file);
    
    load_config(g_server.config_file);
    
    if (g_server.daemonize) {
        run_as_daemon();
    }
    
    init_simple_log(g_server.log_level, g_server.log_path);
    log_message(kLogLevelInfo, "kedis-server starting...\n");
    
    init_server();
    
    init_thread_event_loops(g_server.io_thread_num);
    init_thread_base_conn(g_server.io_thread_num);
    
    if (g_server.bind_addrs.empty()) {
        log_message(kLogLevelInfo, "listen on port %d\n", g_server.port);
        if (start_listen<ClientConn>("0.0.0.0", g_server.port)) {
            log_message(kLogLevelError, "listen on 0.0.0.0:%d failed\n", g_server.port);
            shutdown_signal_handler(0);
        }
    } else {
        for (const string& addr: g_server.bind_addrs) {
            log_message(kLogLevelInfo, "listen on %s:%d\n", addr.c_str(), g_server.port);
            if (start_listen<ClientConn>(addr, g_server.port)) {
                log_message(kLogLevelError, "listen on %s:%d failed\n", addr.c_str(), g_server.port);
                shutdown_signal_handler(0);
            }
        }
    }
    
    write_pid(g_server.pid_file);
    
    g_expire_thread.StartThread();
    g_binlog_purge_thread.StartThread();
    
    get_main_event_loop()->AddTimer(server_cron, NULL, 1000);
    get_main_event_loop()->Start();
    
    return 0;
}
