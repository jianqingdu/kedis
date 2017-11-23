//
//  cmd_db.cpp
//  kedis
//
//  Created by ziteng on 17/7/27.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_db.h"
#include "db_util.h"
#include "encoding.h"
#include "migrate.h"
#include <sys/utsname.h>

/* Return zero if strings are the same, non-zero if they are not.
 * The comparison is performed in a way that prevents an attacker to obtain
 * information about the nature of the strings just monitoring the execution
 * time of the function.
 *
 * Note that limiting the comparison length to strings up to 512 bytes we
 * can avoid leaking any information about the password length and any
 * possible branch misprediction related leak.
 */
int time_independent_strcmp(const char *a, const char *b)
{
    char bufa[512], bufb[512];
    /* The above two strlen perform len(a) + len(b) operations where either
     * a or b are fixed (our password) length, and the difference is only
     * relative to the length of the user provided string, so no information
     * leak is possible in the following two lines of code. */
    unsigned int alen = (int)strlen(a);
    unsigned int blen = (int)strlen(b);
    int diff = 0;
    
    /* We can't compare strings longer than our static buffers.
     * Note that this will never pass the first test in practical circumstances
     * so there is no info leak. */
    if (alen > sizeof(bufa) || blen > sizeof(bufb)) {
        return 1;
    }
    
    memset(bufa, 0, sizeof(bufa));        /* Constant time. */
    memset(bufb, 0, sizeof(bufb));        /* Constant time. */
    /* Again the time of the following two copies is proportional to len(a) + len(b) so no info is leaked. */
    memcpy(bufa, a, alen);
    memcpy(bufb, b, blen);
    
    /* Always compare all the chars in the two buffers without conditional expressions. */
    for (uint32_t j = 0; j < sizeof(bufa); j++) {
        diff |= (bufa[j] ^ bufb[j]);
    }
    /* Length must be equal as well. */
    diff |= alen ^ blen;
    return diff; /* If zero strings are the same. */
}

void auth_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    if (g_server.require_pass.empty()) {
        conn->SendError("Client sent AUTH, but no password is set");
    } else if (!time_independent_strcmp(cmd_vec[1].c_str(), g_server.require_pass.c_str())) {
        conn->SetAuth(true);
        conn->SendSimpleString("OK");
    } else {
        conn->SetAuth(false);
        conn->SendError("invalid password");
    }
}

void ping_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    conn->SendSimpleString("PONG");
}

void echo_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    conn->SendBulkString(cmd_vec[1]);
}

void select_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long db_index;
    if (get_long_from_string(cmd_vec[1], db_index) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    if (db_index < 0 || db_index >= g_server.db_num) {
        conn->SendError("invalid DB index");
    } else {
        conn->SetDbIndex((int)db_index);
        conn->SendRawResponse(kOKString);
    }
}

void dbsize_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long key_num = g_server.key_count_vec[conn->GetDBIndex()];
    conn->SendInteger(key_num);
}

static string get_os_version()
{
    static int call_uname = 1;
    static struct utsname name;
    if (call_uname) {
        uname(&name);
        call_uname = 0;
    }
    
    string os = name.sysname;
    os += " "; os += name.release;
    os += " "; os += name.machine;
    return os;
}

static string get_gcc_version()
{
    char gcc_ver[128];
    snprintf(gcc_ver, sizeof(gcc_ver), "%d.%d.%d",
#ifdef __GNUC__
             __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__
#else
             0,0,0
#endif
             );
    return string(gcc_ver);
}

static uint64_t get_disk_usage()
{
    rocksdb::Range range;
    char a = 0, b = KEY_TYPE_STATS;
    range.start = rocksdb::Slice(&a, 1);
    range.limit = rocksdb::Slice(&b, 1);
    uint64_t total_bytes = 0;
    
    for (int i = 0; i < g_server.db_num; i++) {
        uint64_t size;
        g_server.db->GetApproximateSizes(g_server.cf_handles_map[i], &range, 1, &size);
        total_bytes += size;
    }
    
    return total_bytes;
}

void info_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size > 2) {
        conn->SendError("syntax error");
        return;
    }
    
    string section = (cmd_size == 1) ? "all" : cmd_vec[1];
    bool all_section = (cmd_size == 1) ? true : false;
    
    string info;
    if (all_section || !strcasecmp(section.c_str(), "server")) {
        info.append("# Server\r\n");
        
        string kedis_ver = KEDIS_VERSION;
        string build_time = string(__DATE__) + " " + string(__TIME__);
        info.append("kedis_version:" + kedis_ver + "\r\n");
        info.append("buid_time:" + build_time + "\r\n");
        info.append("os:" + get_os_version() + "\r\n");
        info.append("gcc_version:" + get_gcc_version() + "\r\n");
        info.append("process_id:" + to_string(getpid()) + "\r\n");
        info.append("port:" + to_string(g_server.port) + "\r\n");
        info.append("total_disk_usage:" + to_string(get_disk_usage()) + "\r\n");
        info.append("\r\n");
    }
    
    if (all_section || !strcasecmp(section.c_str(), "client")) {
        info.append("# Client\r\n");
        g_server.slave_mutex.lock();
        uint32_t client_num = g_server.client_num - (uint32_t)g_server.slaves.size();
        g_server.slave_mutex.unlock();
        info.append("connected_clients:" + to_string(client_num) + "\r\n");
        info.append("blocked_clients:0\r\n");
        info.append("\r\n");
    }
    
    if (all_section || !strcasecmp(section.c_str(), "stats")) {
        info.append("# Stats\r\n");
        info.append("total_connections_received:" + to_string(g_stat.total_connections_received) + "\r\n");
        info.append("total_commands_processed:" + to_string(g_stat.total_commands_processed) + "\r\n");
        info.append("instantaneous_ops_per_sec:" + to_string(get_instantaneous_metric(STATS_METRIC_COMMAND)) + "\r\n");
        info.append("total_net_input_bytes:" + to_string(BaseConn::GetTotalNetInputBytes()) + "\r\n");
        info.append("total_net_output_bytes:" + to_string(BaseConn::GetTotalNetOutputBytes()) + "\r\n");
        info.append("instantaneous_input_kbps:" + to_string(get_instantaneous_metric(STATS_METRIC_NET_INPUT)) + "\r\n");
        info.append("instantaneous_output_kbps:" + to_string(get_instantaneous_metric(STATS_METRIC_NET_OUTPUT)) + "\r\n");
        info.append("rejected_connections:" + to_string(g_stat.rejected_connections) + "\r\n");
        info.append("expired_keys:" + to_string(g_stat.expired_keys) + "\r\n");
        info.append("keyspace_hits:" + to_string(g_stat.keyspace_hits) + "\r\n");
        info.append("keyspace_misses:" + to_string(g_stat.keyspace_missed) + "\r\n");
        info.append("migrate_cached_sockets:" + to_string(get_migrate_conn_number()) + "\r\n");
        info.append("\r\n");
    }
    
    if (all_section || !strcasecmp(section.c_str(), "replication")) {
        info.append("# Replication\r\n");
        if (g_server.master_host.empty()) {
            info.append("role:master\r\n");
        } else {
            info.append("role:slave\r\n");
            info.append("master_host:" + g_server.master_host + "\r\n");
            info.append("master_port:" + to_string(g_server.master_port) + "\r\n");
            info.append("master_link_status:" + g_server.master_link_status + "\r\n");
        }
        
        g_server.slave_mutex.lock();
        int slave_cnt = (int)g_server.slaves.size();
        info.append("connected_slaves:" + to_string(slave_cnt) + "\r\n");
        int slave_id = 0;
        for (auto it = g_server.slaves.begin(); it != g_server.slaves.end(); it++) {
            ClientConn* conn = *it;
            char buf[128];
            string state = "online";
            if (conn->IsSendingSnapshot()) {
                state = "send_snapshot";
            }
            snprintf(buf, sizeof(buf), "slave%d:ip=%s,port=%d,state=%s,sync_seq=%llu\r\n", slave_id, conn->GetPeerIP(),
                     conn->GetSlavePort(), state.c_str(), (long long unsigned)conn->GetSyncSeq() - 1);
            info.append(buf);
            slave_id++;
        }
        g_server.slave_mutex.unlock();
        
        info.append("binlog_id:" + g_server.binlog.GetBinlogId() + "\r\n");
        info.append("binlog_seq:" + to_string(g_server.binlog.GetMaxSeq()) + "\r\n");
        info.append("\r\n");
    }
    
    if (all_section || !strcasecmp(section.c_str(), "keyspace")) {
        info.append("# Keyspace\r\n");
        
        for (int i = 0; i < g_server.db_num; i++) {
            long key_num = g_server.key_count_vec[i];
            if (key_num > 0) {
                long ttl_num = g_server.ttl_key_count_vec[i];
                char buf[128];
                snprintf(buf, sizeof(buf), "db%d:keys=%ld,expires=%ld\r\n", i, key_num, ttl_num);
                info.append(buf);
            }
        }

        info.append("\r\n");
    }
    
    conn->SendBulkString(info);
}

void generic_flushdb(int db_idx)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    
    g_server.db->DropColumnFamily(cf_handle);
    delete cf_handle;
    
    g_server.key_count_vec[db_idx] = 0;
    g_server.ttl_key_count_vec[db_idx] = 0;
    
    rocksdb::ColumnFamilyOptions cf_options;
    string cf_name = to_string(db_idx);
    g_server.db->CreateColumnFamily(cf_options, cf_name, &cf_handle);
    g_server.cf_handles_map[db_idx] = cf_handle;
}

void flushdb_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    // if the slave is in replication snapshot state, it may last a long time,
    // so just return an error
    if (g_server.repl_snapshot_count != 0) {
        conn->SendError("in snapshot replication");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    spinlock_db(db_idx);
    generic_flushdb(db_idx);
    g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    unlock_db(db_idx);
    
    conn->SendSimpleString("OK");
}

void flushall_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    if (g_server.repl_snapshot_count != 0) {
        conn->SendError("in snapshot replication");
        return;
    }
    
    spinlock_all();
    for (int db_idx = 0; db_idx < g_server.db_num; db_idx++) {
        generic_flushdb(db_idx);
    }
    
    g_server.binlog.Store(conn->GetDBIndex(), conn->GetCurReqCommand());
    unlock_all();
    
    conn->SendSimpleString("OK");
}

void debug_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (!strcasecmp(cmd_vec[1].c_str(), "sleep") && (cmd_size == 3)) {
        double dtime;
        if (get_double_from_string(cmd_vec[2], dtime) == CODE_ERROR) {
            conn->SendError("seconds is not a valid float");
            return;
        }
        
        long long utime = dtime * 1000000;
        struct timespec tv;
        
        tv.tv_sec = utime / 1000000;
        tv.tv_nsec = (utime % 1000000) * 1000;
        nanosleep(&tv, NULL);
        conn->SendRawResponse(kOKString);
    } else {
        conn->SendError("debug only support: debug sleep seconds");
    }
}

void command_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    // redis-cli will send COMMAND when start, this is just to prevent an error message of unknown command
    conn->SendRawResponse(kOKString);
}
