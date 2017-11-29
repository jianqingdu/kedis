//
//  kedis_port.cpp
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "kedis_to_redis.h"
#include "util.h"
#include "base_conn.h"
#include "event_loop.h"
#include "simple_log.h"
#include "sync_task.h"
#include "src_kedis_conn.h"
#include "kedis_version.h"

Config g_config;

static void print_usage(const char* program)
{
    fprintf(stderr, "%s [OPTIONS]\n"
            "  --src_addr <host:port>   source kedis server ip:port (default: 127.0.0.1:6375)\n"
            "  --dst_addr <host:port>   destination redis server ip:port (default: 127.0.0.1:7400)\n"
            "  --src_db   <db_num>      source db number (default: -1)\n"
            "  --dst_db   <db_num>      destination db number (default: -1)\n"
            "  --src_password <passwd>  source kedis password (default: no password)\n"
            "  --dst_password <passwd>  destination redis password (default: no password)\n"
            "  --prefix   <prefix>      remove key prefix\n"
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
            if (!get_ip_port(addr, g_config.src_kedis_host, g_config.src_kedis_port)) {
                fprintf(stderr, "invalid src addr: %s\n", addr.c_str());
                exit(1);
            }
        } else if (!strcmp(argv[i], "--dst_addr") && !last_arg) {
            string addr = argv[++i];
            if (!get_ip_port(addr, g_config.dst_redis_host, g_config.dst_redis_port)) {
                fprintf(stderr, "invalid dst addr: %s\n", addr.c_str());
                exit(1);
            }
        } else if (!strcmp(argv[i], "--src_db") && !last_arg) {
            g_config.src_kedis_db = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--dst_db") && !last_arg) {
            g_config.dst_redis_db = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--src_password") && !last_arg) {
            g_config.src_kedis_password = argv[++i];
        } else if (!strcmp(argv[i], "--dst_password") && !last_arg) {
            g_config.dst_redis_password = argv[++i];
        } else if (!strcmp(argv[i], "--prefix") && !last_arg) {
            g_config.prefix = argv[++i];
        } else if (!strcmp(argv[i], "--version")) {
            printf("redis_port Version: %s\n", KEDIS_VERSION);
            printf("redis_port Build: %s %s\n", __DATE__, __TIME__);
            exit(0);
        } else {
            print_usage(argv[0]);
            exit(1);
        }
    }
}

int main(int argc, char* argv[])
{
    parse_cmd_line(argc, argv);
    
    int io_thread_num = 0; // all nonblock network io in main thread
    init_thread_event_loops(io_thread_num);
    init_thread_base_conn(io_thread_num);
    
    init_simple_log(kLogLevelDebug, "log");
    
    init_sync_task();
    
    SrcKedisConn* src_conn = new SrcKedisConn();
    src_conn->Connect(g_config.src_kedis_host, g_config.src_kedis_port);
    
    get_main_event_loop()->Start();
    
    return 0;
}
