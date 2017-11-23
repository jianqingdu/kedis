//
//  cmd_line_parser.cpp
//  kedis
//
//  Created by ziteng on 17-11-20.
//  Copyright (c) 2017年 mgj. All rights reserved.
//

#include "cmd_line_parser.h"

#ifndef REDIS_TO_KEDIS_VERSION
#define REDIS_TO_KEDIS_VERSION "1.0.0" // major.minor.patch
#endif

Config g_config;

static void print_usage(const char* program)
{
    fprintf(stderr, "%s [OPTIONS]\n"
            "  --src_addr <host:port>   source redis server ip:port (default: 127.0.0.1:6375)\n"
            "  --dst_addr <host:port>   destination kedis server ip:port (default: 127.0.0.1:7400)\n"
            "  --src_db   <db_num>      source db number (default: -1)\n"
            "  --dst_db   <db_num>      destination kedis db number (default: -1)\n"
            "  --src_password <passwd>  source redis password (default: no password)\n"
            "  --dst_password <passwd>  destination kedis password (default: no password)\n"
            "  --pipeline <count>       how many pipeline command (default: 32)\n"
            "  --rdb_file <rdb_file>    rdb dump file (default: dump.rdb)\n"
            "  --aof_file <aof_file>    aof dump file (default: dump.aof)\n"
            "  --prefix   <prefix>      remove key prefix\n"
            "  --network_limit <nl>     limit network speed when receive RDB file, unit MB (default: 50MB)\n"
            "  --src_from_rdb           use RDB file as a data source\n"
            "  --version                show version\n"
            "  --help\n", program);
}

void parse_cmd_line(int argc, char* argv[])
{
    for (int i = 1; i < argc; ++i) {
        bool last_arg = (i == argc - 1);
        
        if (!strcmp(argv[i], "--help")) {
            print_usage(argv[0]);
            exit(0);
        } else if (!strcmp(argv[i], "--src_addr") && !last_arg) {
            string addr = argv[++i];
            if (!get_ip_port(addr, g_config.src_redis_host, g_config.src_redis_port)) {
                fprintf(stderr, "invalid src addr: %s\n", addr.c_str());
                exit(1);
            }
        } else if (!strcmp(argv[i], "--dst_addr") && !last_arg) {
            string addr = argv[++i];
            if (!get_ip_port(addr, g_config.dst_kedis_host, g_config.dst_kedis_port)) {
                fprintf(stderr, "invalid dst addr: %s\n", addr.c_str());
                exit(1);
            }
        } else if (!strcmp(argv[i], "--src_db") && !last_arg) {
            g_config.src_redis_db = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--dst_db") && !last_arg) {
            g_config.dst_kedis_db = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--src_password") && !last_arg) {
            g_config.src_redis_password = argv[++i];
        } else if (!strcmp(argv[i], "--dst_password") && !last_arg) {
            g_config.dst_kedis_password = argv[++i];
        } else if (!strcmp(argv[i], "--pipeline") && !last_arg) {
            g_config.pipeline_cnt = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--rdb_file") && !last_arg) {
            g_config.rdb_file = argv[++i];
        } else if (!strcmp(argv[i], "--aof_file") && !last_arg) {
            g_config.aof_file = argv[++i];
        } else if (!strcmp(argv[i], "--prefix") && !last_arg) {
            g_config.prefix = argv[++i];
        } else if (!strcmp(argv[i], "--network_limit") && !last_arg) {
            g_config.network_limit = atoi(argv[++i]) * 1024 * 1024;
        } else if (!strcmp(argv[i], "--src_from_rdb")) {
            g_config.src_from_rdb = true;
        } else if (!strcmp(argv[i], "--version")) {
            printf("redis_to_kedis Version: %s\n", REDIS_TO_KEDIS_VERSION);
            printf("redis_to_kedis Build: %s %s\n", __DATE__, __TIME__);
            exit(0);
        } else {
            print_usage(argv[0]);
            exit(1);
        }
    }
}
