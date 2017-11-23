//
//  cmd_line_parser.h
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#ifndef __CMD_LINE_PARSER_H__
#define __CMD_LINE_PARSER_H__

#include "util.h"

struct Config {
    string  src_redis_host;
    int     src_redis_port;
    int     src_redis_db;   // select from which db, -1 means any db
    string  src_redis_password;
    string  dst_kedis_host;
    int     dst_kedis_port;
    int     dst_kedis_db;   // write to which db, -1 means do not need to select db
    string  dst_kedis_password;
    int     pipeline_cnt;
    string  rdb_file;
    string  aof_file;
    string  prefix;
    int     network_limit;  // limit network speed when receive RDB file
    bool    src_from_rdb;
    
    Config() {
        src_redis_host = "127.0.0.1";
        src_redis_port = 6375;
        src_redis_db = -1;
        src_redis_password = "";
        dst_kedis_host = "127.0.0.1";
        dst_kedis_port = 7400;
        dst_kedis_db = -1;
        dst_kedis_password = "";
        pipeline_cnt = 32;
        rdb_file = "dump.rdb";
        aof_file = "dump.aof";
        prefix = "";
        network_limit = 50 * 1024 * 1024;
        src_from_rdb = false;
    }
};

extern Config g_config;

void parse_cmd_line(int argc, char* argv[]);

#endif
