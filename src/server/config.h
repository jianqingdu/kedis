//
//  config.h
//  kedis
//
//  Created by ziteng on 17/8/22.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "util.h"
#include "client_conn.h"

void load_config(const string& filename);

int rewrite_config(const string& filename);

void config_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __CONFIG_H__ */
