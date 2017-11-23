//
//  migrate_conn.h
//  kedis
//
//  Created by ziteng on 17/9/19.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __MIGRATE_CONN_H__
#define __MIGRATE_CONN_H__

#include "base_conn.h"
#include "redis_parser.h"

class MigrateConn : public BaseConn {
public:
    MigrateConn();
    virtual ~MigrateConn();
    
    virtual void Close();
    virtual void OnConfirm();
    virtual void OnRead();
    virtual void OnTimer(uint64_t curr_tick);
    
    void SetAddr(const string& addr) { addr_ = addr; }
private:
    string addr_;
};


#endif /* __MIGRATE_CONN_H__ */
