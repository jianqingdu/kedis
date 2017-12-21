//
//  config.cpp
//  kedis
//
//  Created by ziteng on 17/8/22.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "config.h"
#include "server.h"
#include "db_util.h"

static const char* log_level[] = {"error", "warn", "info", "debug"};

static int yesnotoi(const string& s)
{
    if (!strcasecmp(s.c_str(), "yes"))
        return 1;
    else if (!strcasecmp(s.c_str(),"no"))
        return 0;
    else
        return -1;
}

static void load_panic(const char* error_msg)
{
    init_simple_log(kLogLevelInfo, "error-log");
    log_message(kLogLevelError, "%s\n", error_msg);
    exit(1);
}

void load_config(const string& filename)
{
    string content;
    if (get_file_content(filename.c_str(), content)) {
        load_panic("get file content failed, the filename may not exist\n");
    }
    
    vector<string> lines = split(content, "\n");
    for (int i = 0; i < (int)lines.size(); i++) {
        string& line = lines[i];
        // skip empty or comment line
        if (line.empty() || (line[0] == '#')) {
            continue;
        }
        
        // replace all tab and carriage return charactor to space
        int line_len = (int)line.size();
        for (int j = 0; j < line_len; j++) {
            if (line[j] == '\t' || line[j] == '\r') {
                line[j] = ' ';
            }
        }
        
        vector<string> argv = split(line, " ");
        if (argv.empty()) {
            continue;
        }
        
        int argc = (int)argv.size();
        
        if (!strcasecmp("bind", argv[0].c_str()) && (argc >= 2)) {
            for (int j = 1; j < argc; j++) {
                g_server.bind_addrs.push_back(argv[j]);
            }
        } else if (!strcasecmp("port", argv[0].c_str()) && (argc == 2)) {
            g_server.port = atoi(argv[1].c_str());
            if (g_server.port < 0 || g_server.port > 0xFFFF) {
                load_panic("invalid port");
            }
        } else if (!strcasecmp("timeout", argv[0].c_str()) && (argc == 2)) {
            g_server.client_timeout = atoi(argv[1].c_str());
            if (g_server.client_timeout < 0) {
                load_panic("invalid client timeout");
            }
        } else if (!strcasecmp("daemonize", argv[0].c_str()) && (argc == 2)) {
            int r = yesnotoi(argv[1]);
            if (r == -1) {
                load_panic("must a yes or no");
            }
            g_server.daemonize = r;
        } else if (!strcasecmp("pidfile", argv[0].c_str()) && (argc == 2)) {
            g_server.pid_file = argv[1];
        } else if (!strcasecmp("loglevel", argv[0].c_str()) && (argc == 2)) {
            if (argv[1] == "debug")
                g_server.log_level = kLogLevelDebug;
            else if (argv[1] == "info")
                g_server.log_level = kLogLevelInfo;
            else if (argv[1] == "warning")
                g_server.log_level = kLogLevelWarning;
            else if (argv[1] == "error")
                g_server.log_level = kLogLevelError;
            else
                load_panic("no such log level");
        } else if (!strcasecmp("logpath", argv[0].c_str()) && (argc == 2)) {
            g_server.log_path = argv[1];
        } else if (!strcasecmp("io-thread-num", argv[0].c_str()) && (argc == 2)) {
            g_server.io_thread_num = atoi(argv[1].c_str());
            if (g_server.io_thread_num < 0) {
                load_panic("invalid io thread number");
            }
        } else if (!strcasecmp("databases", argv[0].c_str()) && (argc == 2)) {
            g_server.db_num = atoi(argv[1].c_str());
            if (g_server.db_num < 1) {
                load_panic("invalid number of databases");
            }
        } else if (!strcasecmp("db-name", argv[0].c_str()) && (argc == 2)) {
            g_server.db_name = argv[1];
        } else if (!strcasecmp("key-count-file", argv[0].c_str()) && (argc == 2)) {
            g_server.key_count_file = argv[1];
        } else if (!strcasecmp("binlog-dir", argv[0].c_str()) && (argc == 2)) {
            g_server.binlog_dir = argv[1];
        } else if (!strcasecmp("binlog-capacity", argv[0].c_str()) && (argc == 2)) {
            g_server.binlog_capacity = atoi(argv[1].c_str());
            if (g_server.binlog_capacity < 1) {
                load_panic("invalid binlog-capacity");
            }
        } else if (!strcasecmp("requirepass", argv[0].c_str()) && (argc == 2)) {
            g_server.require_pass = argv[1];
        } else if (!strcasecmp("maxclients", argv[0].c_str()) && (argc == 2)) {
            g_server.max_clients = atoi(argv[1].c_str());
            if (g_server.max_clients < 1) {
                load_panic("invalid max clients");
            }
        } else if (!strcasecmp("slaveof", argv[0].c_str()) && argc == 3) {
            g_server.master_host = argv[1];
            g_server.master_port = atoi(argv[2].c_str());
            if (g_server.master_port < 0 || g_server.master_port > 0xFFFF) {
                load_panic("invalid master port");
            }
        } else if (!strcasecmp("masterauth", argv[0].c_str()) && (argc == 2)) {
            g_server.master_auth = argv[1];
        } else if (!strcasecmp("slave-read-only", argv[0].c_str()) && (argc == 2)) {
            int r = yesnotoi(argv[1]);
            if (r == -1) {
                load_panic("must a yes or no");
            }
            g_server.slave_read_only = r;
        } else if (!strcasecmp("repl-timeout", argv[0].c_str()) && (argc == 2)) {
            g_server.repl_timeout = atoi(argv[1].c_str());
            if (g_server.repl_timeout < 0) {
                load_panic("invalid client timeout");
            }
        } else if (!strcasecmp("slowlog-log-slower-than", argv[0].c_str()) && (argc == 2)) {
            g_server.slowlog_log_slower_than = atoi(argv[1].c_str());
        } else if (!strcasecmp("slowlog-max-len", argv[0].c_str()) && (argc == 2)) {
            g_server.slowlog_max_len = atoi(argv[1].c_str());
        } else if (!strcasecmp("hll-sparse-max-bytes", argv[0].c_str()) && (argc == 2)) {
            g_server.hll_sparse_max_bytes = atoi(argv[1].c_str());
            if (g_server.hll_sparse_max_bytes < 0) {
                load_panic("invalid hll-sparse-max-bytes");
            }
        } else {
            string msg = "bad directive or wrong number of arguments: " + line;
            load_panic(msg.c_str());
        }
    }
}

int rewrite_config(const string& filename)
{
    string config_pattern_network =
    R"(
# Kedis configuration file example.
#
# Note that in order to read the configuration file, kedis must be
# started with the file path as first argument:
#
# ./kedis-server -c /path/to/kedis.conf
    
################################## NETWORK #####################################
    
# By default, if no "bind" configuration directive is specified, Kedis listens
# for connections from all the network interfaces available on the server.
# It is possible to listen to just one or multiple selected interfaces using
# the "bind" configuration directive, followed by one or more IP addresses.
#
%s bind %s

# Accept connections on the specified port, default is 6379.
port %d
    
# Close the connection after a client is idle for N seconds (0 to disable)
timeout %d
    )";
 
    string config_pattern_general =
    R"(
################################# GENERAL #####################################
    
# By default Kedis does not run as a daemon. Use 'yes' if you need it.
# Note that Kedis will write a pid file in ./redis.pid when daemonized.
daemonize %s

# set the pid file
pidfile %s

# Specify the server verbosity level.
# This can be one of:
# debug (a lot of information, useful for development/testing)
# info (many rarely useful info, but not a mess like the debug level)
# warning (moderately verbose, what you want in production probably)
# error (only very important / critical messages are logged)
loglevel %s
    
# Specify the log path name.
logpath %s
    
# Set the number of io threads. This number can not be changed after the server is started
io-thread-num %d
    
# Set the number of databases. The default database is DB 0, you can select
# a different one on a per-connection basis using SELECT <dbid> where
# dbid is a number between 0 and 'databases'-1
databases %d
    
# Set the name of database. The default name is kdb, all data files will be saved in this directory
db-name %s

# Set the name of key count file
key-count-file %s

# save binlog db file in this directory
binlog-dir %s
    
# the maximum number of binlog command can be saved
binlog-capacity %d
    
# Require clients to issue AUTH <PASSWORD> before processing any other
# commands.  This might be useful in environments in which you do not trust
# others with access to the host running kedis-server.
%s requirepass %s
    
# Set the max number of connected clients at the same time.
maxclients %d
    )";
    
    string config_pattern_replication =
    R"(
################################# REPLICATION #################################
    
# Master-Slave replication. Use slaveof to make a Kedis instance a copy of
# another Kedis server. A few things to understand ASAP about Kedis replication.
#
# 1) Kedis replication is asynchronous
# 2) Kedis slaves are able to perform a partial resynchronization with the
#    master if the replication link is lost for a relatively small amount of
#    time.
# 3) Replication is automatic and does not need user intervention. After a
#    network partition slaves automatically try to reconnect to masters
#    and resynchronize with them.
#
%s slaveof %s %s
    
# If the master is password protected (using the "requirepass" configuration
# directive below) it is possible to tell the slave to authenticate before
# starting the replication synchronization process, otherwise the master will
# refuse the slave request.
#
%s masterauth %s
    
# You can configure a slave instance to accept writes or not. Writing against
# a slave instance may be useful to store some ephemeral data (because data
# written on a slave will be easily deleted after resync with the master) but
# may also cause problems if clients are writing to it because of a misconfiguration.
#
slave-read-only %s
    
# The following option sets the replication timeout for:
#
# 1) Bulk transfer I/O during SYNC, from the point of view of slave.
# 2) Master timeout from the point of view of slaves (data, pings).
# 3) Slave timeout from the point of view of masters (REPLCONF ACK pings).
#
# It is important to make sure that this value is greater than the value
# specified for repl-ping-slave-period otherwise a timeout will be detected
# every time there is low traffic between the master and the slave.
#
repl-timeout %d
    )";
    
    string config_pattern_other =
    R"(
################################## SLOW LOG ###################################
    
# The Kedis Slow Log is a system to log queries that exceeded a specified
# execution time. The execution time does not include the I/O operations
# like talking with the client, sending the reply and so forth,
# but just the time needed to actually execute the command 
#
# You can configure the slow log with two parameters: one tells Kedis
# what is the execution time, in milliseconds, to exceed in order for the
# command to get logged, and the other parameter is the length of the
# slow log. When a new command is logged the oldest one is removed from the
# queue of logged commands.
    
# The following time is expressed in milliseconds, so 1000 is equivalent
# to one second. Note that a negative number disables the slow log, while
# a value of zero forces the logging of every command.
slowlog-log-slower-than %d
    
# There is no limit to this length. Just be aware that it will consume memory.
# You can reclaim memory used by the slow log with SLOWLOG RESET.
slowlog-max-len %d
    
############################### ADVANCED CONFIG ###############################
    
# HyperLogLog sparse representation bytes limit. The limit includes the
# 16 bytes header. When an HyperLogLog using the sparse representation crosses
# this limit, it is converted into the dense representation.
#
# A value greater than 16000 is totally useless, since at that point the
# dense representation is more memory efficient.
#
# The suggested value is ~ 3000 in order to have the benefits of
# the space efficient encoding without slowing down too much PFADD,
# which is O(N) with the sparse encoding. The value can be raised to
# ~ 10000 when CPU is not a concern, but space is, and the data set is
# composed of many HyperLogLogs with cardinality in the 0 - 15000 range.
hll-sparse-max-bytes %d
    
# config rewrite
    )";
    
    string tmp_file = g_server.config_file + "_tmp";
    log_message(kLogLevelDebug, "tmp_file=%s\n", tmp_file.c_str());
    FILE* fp = fopen(tmp_file.c_str(), "w");
    if (!fp) {
        log_message(kLogLevelWarning, "rewrite config file failed, file=%s, err=%s\n", tmp_file.c_str(), strerror(errno));
        return CODE_ERROR;
    }
    
    string addrs;
    for (const string& addr: g_server.bind_addrs) {
        addrs += addr + " ";
    }
    
    fprintf(fp, config_pattern_network.c_str(),
            addrs.empty()? "#": "", addrs.c_str(), g_server.port, g_server.client_timeout);
    
    fprintf(fp, config_pattern_general.c_str(),
            g_server.daemonize ? "yes" : "no", g_server.pid_file.c_str(), log_level[g_server.log_level],
            g_server.log_path.c_str(), g_server.io_thread_num, g_server.db_num, g_server.db_name.c_str(),
            g_server.key_count_file.c_str(), g_server.binlog_dir.c_str(), g_server.binlog_capacity,
            g_server.require_pass.empty() ? "#" : "",
            g_server.require_pass.empty() ? "<password>" : g_server.require_pass.c_str(), g_server.max_clients);

    fprintf(fp, config_pattern_replication.c_str(),
            g_server.master_host.empty() ? "#" : "",
            g_server.master_host.empty() ? "<masterhost>" : g_server.master_host.c_str(),
            g_server.master_host.empty() ? "<masterport>" : to_string(g_server.master_port).c_str(),
            g_server.master_auth.empty() ? "#" : "",
            g_server.master_auth.empty() ? "<master-password>" : g_server.master_auth.c_str(),
            g_server.slave_read_only ? "yes" : "no", g_server.repl_timeout);
    
    fprintf(fp, config_pattern_other.c_str(),
            g_server.slowlog_log_slower_than, g_server.slowlog_max_len,
            g_server.hll_sparse_max_bytes);
    fclose(fp);
    
    rename(tmp_file.c_str(), g_server.config_file.c_str());
    
    return CODE_OK;
}

void config_set_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    if (!strcasecmp(cmd_vec[2].c_str(), "timeout")) {
        g_server.client_timeout = atoi(cmd_vec[3].c_str());
    } else if (!strcasecmp(cmd_vec[2].c_str(), "loglevel")) {
        if (cmd_vec[3] == "debug") {
            g_server.log_level = kLogLevelDebug;
        } else if (cmd_vec[3] == "info") {
            g_server.log_level = kLogLevelInfo;
        } else if (cmd_vec[3] == "warning") {
            g_server.log_level = kLogLevelWarning;
        } else if (cmd_vec[3] == "error") {
            g_server.log_level = kLogLevelError;
        } else {
            conn->SendError("no such log level");
            return;
        }
        g_simple_log.SetLogLevel(g_server.log_level);
    } else if (!strcasecmp(cmd_vec[2].c_str(), "requirepass")) {
        g_server.require_pass = cmd_vec[3];
    } else if (!strcasecmp(cmd_vec[2].c_str(), "maxclients")) {
        int maxclients = atoi(cmd_vec[3].c_str());
        if (maxclients < 1) {
            conn->SendError("maxclients value must positive");
            return;
        }
        g_server.max_clients = maxclients;
    } else if (!strcasecmp(cmd_vec[2].c_str(), "masterauth")) {
        g_server.master_auth = cmd_vec[3];
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slave-read-only")) {
        int r = yesnotoi(cmd_vec[3]);
        if (r == -1) {
            conn->SendError("slave-read-only must be a yes or no");
            return;
        }
        g_server.slave_read_only = r;
    } else if (!strcasecmp(cmd_vec[2].c_str(), "repl-timeout")) {
        int repl_timeout = atoi(cmd_vec[3].c_str());
        if (g_server.repl_timeout < 0) {
            conn->SendError("repl-timeout must not negative");
            return;
        }
        g_server.repl_timeout = repl_timeout;
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slowlog-log-slower-than")) {
        g_server.slowlog_log_slower_than = atoi(cmd_vec[3].c_str());
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slowlog-max-len")) {
        g_server.slowlog_max_len = atoi(cmd_vec[3].c_str());
    } else if (!strcasecmp(cmd_vec[2].c_str(), "hll-sparse-max-bytes")) {
        g_server.hll_sparse_max_bytes = atoi(cmd_vec[3].c_str());
    } else {
        string err_msg = "Unsupported CONFIG parameter: " + cmd_vec[2];
        conn->SendError(err_msg);
        return;
    }
    
    conn->SendSimpleString("OK");
}

void config_get_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    vector<string> resp_vec;
    resp_vec.push_back(cmd_vec[2]);
    
    if (!strcasecmp(cmd_vec[2].c_str(), "timeout")) {
        resp_vec.push_back(to_string(g_server.client_timeout));
    } else if (!strcasecmp(cmd_vec[2].c_str(), "loglevel")) {
        resp_vec.push_back(log_level[g_server.log_level]);
    } else if (!strcasecmp(cmd_vec[2].c_str(), "requirepass")) {
        resp_vec.push_back(g_server.require_pass);
    } else if (!strcasecmp(cmd_vec[2].c_str(), "maxclients")) {
        resp_vec.push_back(to_string(g_server.max_clients));
    } else if (!strcasecmp(cmd_vec[2].c_str(), "masterauth")) {
        resp_vec.push_back(g_server.master_auth);
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slave-read-only")) {
        resp_vec.push_back(g_server.slave_read_only ? "yes" : "no");
    } else if (!strcasecmp(cmd_vec[2].c_str(), "repl-timeout")) {
        resp_vec.push_back(to_string(g_server.repl_timeout));
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slowlog-log-slower-than")) {
        resp_vec.push_back(to_string(g_server.slowlog_log_slower_than));
    } else if (!strcasecmp(cmd_vec[2].c_str(), "slowlog-max-len")) {
        resp_vec.push_back(to_string(g_server.slowlog_max_len));
    } else if (!strcasecmp(cmd_vec[2].c_str(), "hll-sparse-max-bytes")) {
        resp_vec.push_back(to_string(g_server.hll_sparse_max_bytes));
    } else {
        resp_vec.clear();
    }
    
    conn->SendArray(resp_vec);
}

void config_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (!strcasecmp(cmd_vec[1].c_str(), "set")) {
        if (cmd_size != 4) {
            conn->SendError("wrong number of arguments for CONFIG SET command");
        } else {
            config_set_command(conn, cmd_vec);
        }
    } else if (!strcasecmp(cmd_vec[1].c_str(), "get")) {
        if (cmd_size != 3) {
            conn->SendError("wrong number of arguments for CONFIG GET command");
        } else {
            config_get_command(conn, cmd_vec);
        }
    } else if (!strcasecmp(cmd_vec[1].c_str(),"resetstat")) {
        if (cmd_size != 2) {
            conn->SendError("wrong number of arguments for CONFIG RESETSTAT command");
        } else {
            g_stat.Reset();
            conn->SendSimpleString("OK");
            log_message(kLogLevelWarning, "CONFIG RESETSTAT executed with success\n");
        }
    } else if (!strcasecmp(cmd_vec[1].c_str(),"rewrite")) {
        if (cmd_size != 2) {
            conn->SendError("wrong number of arguments for CONFIG REWRITE command");
            return;
        }
        
        if (rewrite_config(g_server.config_file) == CODE_ERROR) {
            conn->SendError("Rewriting config file failed");
        } else {
            log_message(kLogLevelWarning, "CONFIG REWRITE executed with success\n");
            conn->SendSimpleString("OK");
        }
    } else {
        conn->SendError("CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
    }
}
