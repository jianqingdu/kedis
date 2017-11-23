//
//  server.h
//  kedis
//
//  Created by jianqing.du on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __SERVER_H__
#define __SERVER_H__

#include "util.h"
#include "simple_log.h"
#include "client_conn.h"
#include "binlog.h"
#include "key_lock.h"
#include "rocksdb/db.h"

#ifndef KEDIS_VERSION
#define KEDIS_VERSION "X.X.X"
#endif


// Instantaneous metrics tracking.
#define STATS_METRIC_SAMPLES    10 // Number of samples per metric
#define STATS_METRIC_COMMAND    0  // Number of commands executed
#define STATS_METRIC_NET_INPUT  1  // Bytes read to network
#define STATS_METRIC_NET_OUTPUT 2  // Bytes written to network
#define STATS_METRIC_COUNT      3

#define MAX_KEY_ITERATOR_COUNT  100000

typedef void (*KedisCommandProc)(ClientConn* conn, const vector<string>& cmd_vec);

struct KedisCommand {
    KedisCommandProc    proc;
    int                 arity;
    bool                is_write;
};

// global state for kedis server
struct KedisServer {
    string  config_file;
    bool    daemonize;
    vector<string> bind_addrs;
    int     port;
    int     client_timeout; // max time (second) for idle client
    string  pid_file;
    LogLevel log_level;
    string  log_path;
    int     io_thread_num;
    string  db_name;
    int     db_num;  // total number of db
    string  binlog_dir;
    int     binlog_capacity;
    string  master_host;
    int     master_port;
    string  master_auth;
    bool    slave_read_only;
    int     repl_timeout;   // max time (second) for idle master or slave
    string  require_pass;   // if not empty, the client need AUTH before all other commands
    int     max_clients;    // max number of simultaneous clients
    int     slowlog_log_slower_than;
    int     slowlog_max_len;
    int     hll_sparse_max_bytes;
    
    rocksdb::DB* db;
    rocksdb::Options options; // define these 5 rocksdb options here, so optimise can be done in one place
    rocksdb::DBOptions db_options;
    rocksdb::ColumnFamilyOptions cf_options;
    rocksdb::ReadOptions read_option;
    rocksdb::WriteOptions write_option;
    
    map<int, rocksdb::ColumnFamilyHandle*> cf_handles_map;
    map<string, KedisCommand>   kedis_commands;
    atomic<long>* key_count_vec;
    atomic<long>* ttl_key_count_vec;
    atomic<long>* operating_count_vec; // how many thread are operating rocksdb, used for flushdb
    atomic<long> repl_snapshot_count;
    Binlog  binlog;
    
    net_handle_t    master_handle;
    string          master_link_status;
    mutex           slave_mutex;
    list<ClientConn*>   slaves;
    atomic<int>     client_num;
};

struct KedisStat {
    atomic<long> total_connections_received;
    atomic<long> rejected_connections;
    atomic<long> total_commands_processed;
    atomic<long> expired_keys;
    atomic<long> keyspace_hits;
    atomic<long> keyspace_missed;
    
    struct {
        uint64_t last_sample_time; // Timestamp of last sample in ms
        long last_sample_count; // Count in last sample
        long samples[STATS_METRIC_SAMPLES];
        int idx;
    } inst_metric[STATS_METRIC_COUNT];
    
    void Reset() {
        total_connections_received = 0;
        rejected_connections = 0;
        total_commands_processed = 0;
        expired_keys = 0;
        keyspace_hits = 0;
        keyspace_missed = 0;
        
        for (int j = 0; j < STATS_METRIC_COUNT; j++) {
            inst_metric[j].idx = 0;
            inst_metric[j].last_sample_time = get_tick_count();
            inst_metric[j].last_sample_count = 0;
            memset(inst_metric[j].samples, 0, sizeof(inst_metric[j].samples));
        }
    }
};

extern KedisServer g_server;
extern KedisStat g_stat;
extern PurgeBinlogThread g_binlog_purge_thread;

void create_db();
void close_db();
void drop_db(const string& db_path);

long get_instantaneous_metric(int metric);

// used to prevent flushdb when scan the keys
class ScanKeyGuard {
public:
    ScanKeyGuard(int db_idx) : db_idx_(db_idx) {
        lock_db(db_idx);
        g_server.operating_count_vec[db_idx_]++;
        unlock_db(db_idx);
    }
    
    ~ScanKeyGuard() {
        g_server.operating_count_vec[db_idx_]--;
    }
private:
    int db_idx_;
};

#endif /* __SERVER_H__ */
