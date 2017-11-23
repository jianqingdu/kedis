/*
 * io_thread_resource.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_IO_THREAD_RESOURCE_H__
#define __BASE_IO_THREAD_RESOURCE_H__

#include <assert.h>

// io thread resource management, EventLoop, PendingPktMgr, all will use this
template<typename T>
class IoThreadResource {
public:
    IoThreadResource();
    ~IoThreadResource();
    
    void Init(int io_thread_num);
    int GetThreadNum() { return io_thread_num_; }
    T* GetMainResource() { return main_thread_resource_; }
    T* GetIOResource(int handle);
    bool IsInited() { return inited_; }
private:
    T*      main_thread_resource_;
    T*      io_thread_resources_;
    int     io_thread_num_;
    bool    inited_;
};


template<typename T>
IoThreadResource<T>::IoThreadResource()
{
    inited_ = false;
    io_thread_num_ = 0;
    main_thread_resource_ = NULL;
    io_thread_resources_ = NULL;
}

template<typename T>
IoThreadResource<T>::~IoThreadResource()
{
    if (main_thread_resource_) {
        delete main_thread_resource_;
    }
    
    if (io_thread_resources_) {
        delete [] io_thread_resources_;
    }
}

template<typename T>
void IoThreadResource<T>::Init(int io_thread_num)
{
    io_thread_num_ = io_thread_num;
    main_thread_resource_ = new T;
    assert(main_thread_resource_);
    if (io_thread_num_ > 0) {
        io_thread_resources_ = new T[io_thread_num_];
    }
    
    inited_ = true;
}

template<typename T>
T* IoThreadResource<T>::GetIOResource(int handle)
{
    if (io_thread_num_ > 0) {
        return &io_thread_resources_[handle % io_thread_num_];
    } else {
        return main_thread_resource_;
    }
}

#endif
