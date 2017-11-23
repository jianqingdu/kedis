//
//  expire_thread.h
//  kedis
//
//  Created by ziteng on 17/9/27.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __EXPIRE_THREAD_H__
#define __EXPIRE_THREAD_H__

#include "thread_pool.h"

class ExpireThread : public Thread {
public:
    ExpireThread() {}
    virtual ~ExpireThread() {}
    
    virtual void OnThreadRun(void);
};

#endif /* __EXPIRE_THREAD_H__ */
