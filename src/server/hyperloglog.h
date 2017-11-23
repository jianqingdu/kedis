//
//  hyperloglog.h
//  kedis
//
//  Created by ziteng on 17/8/15.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __HYPERLOGLOG_H__
#define __HYPERLOGLOG_H__

#include "server.h"

void pfadd_command(ClientConn* conn, const vector<string>& cmd_vec);
void pfcount_command(ClientConn* conn, const vector<string>& cmd_vec);
void pfmerge_command(ClientConn* conn, const vector<string>& cmd_vec);

#endif /* __HYPERLOGLOG_H__ */
