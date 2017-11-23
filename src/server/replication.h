//
//  replication.h
//  kedis
//
//  Created by ziteng on 17/8/30.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __REPLICATION_H__
#define __REPLICATION_H__

#include "server.h"
#include "client_conn.h"

void replconf_command(ClientConn* conn, const vector<string>& cmd_vec);
void psync_command(ClientConn* conn, const vector<string>& cmd_vec);
void slaveof_command(ClientConn* conn, const vector<string>& cmd_vec);
void repl_snapshot_complete_command(ClientConn* conn, const vector<string>& cmd_vec);

void replication_cron();

void prepare_snapshot_replication();

class ReplicationSnapshot {
public:
    ReplicationSnapshot(const rocksdb::Snapshot* snapshot);
    ~ReplicationSnapshot();
    
    void SendTo(ClientConn* conn);
    bool IsCompelte();
private:
    const rocksdb::Snapshot*  snapshot_;
    rocksdb::Iterator*  iterator_;
    int                 cur_db_idx_;
};

#endif /* __REPLICATION_H__ */
