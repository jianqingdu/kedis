//
//  kedis_port.h
//  kedis
//
//  Created by ziteng on 17/11/16.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __KEDIS_PORT_H__
#define __KEDIS_PORT_H__

#include "util.h"

struct Config {
    string  src_kedis_host;
    int     src_kedis_port;
    int     src_kedis_db;   // select from which db, -1 means any db
    string  src_kedis_password;
    string  dst_kedis_host;
    int     dst_kedis_port;
    int     dst_kedis_db;   // write to which db, -1 means do not need to select db
    string  dst_kedis_password;
    string  prefix;
    
    Config() {
        src_kedis_host = "127.0.0.1";
        src_kedis_port = 6375;
        src_kedis_db = -1;
        src_kedis_password = "";
        dst_kedis_host = "127.0.0.1";
        dst_kedis_port = 7400;
        dst_kedis_db = -1;
        dst_kedis_password = "";
        prefix = "";
    }
};

extern Config g_config;

#endif /* __KEDIS_PORT_H__ */
