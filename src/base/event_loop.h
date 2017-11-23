/*
 * event_loop.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_EVENT_LOOP_H__
#define __BASE_EVENT_LOOP_H__

#include "ostype.h"
#include "util.h"

enum {
	SOCKET_READ		= 0x1,
	SOCKET_WRITE    = 0x2,
	SOCKET_EXCEP    = 0x4,
	SOCKET_ALL      = 0x7,
    SOCKET_CONNECT_CB   = 0x8,
    SOCKET_ADD_CONN     = 0x10,
    SOCKET_DEL_CONN     = 0x20,
};

class BaseSocket;
typedef unordered_map<int, BaseSocket*> SocketMap;

class EventLoop
{
public:
    EventLoop();
	virtual ~EventLoop();

	void AddEvent(int fd, uint8_t socket_event, BaseSocket* pSocket);
	void RemoveEvent(int fd, uint8_t socket_event, BaseSocket* pSocket);

	void AddTimer(callback_t callback, void* user_data, uint64_t interval);
	void RemoveTimer(callback_t callback, void* user_data);

	void AddLoop(callback_t callback, void* user_data);

	void Start(uint32_t wait_timeout = 100);
    
    void Stop() { stop_ = true; Wakeup(); } // call by another thread
    void Wakeup();
    
    void SetThreadId(pthread_t tid) { thread_id_ = tid; }
    bool IsInLoopThread() { return pthread_self() == thread_id_; }
    pthread_t GetThreadId() { return thread_id_; }
    
    void OnTimer(); 
    
    BaseSocket* FindBaseSocket(net_handle_t handle);
private:
	void _CheckTimer();
	void _CheckLoop();
    void _ReadWakeupData();
    void _RegisterEventList();
    
	typedef struct {
		callback_t	callback;
		void*		user_data;
		uint64_t	interval;
		uint64_t	next_tick;
	} TimerItem;

    typedef struct {
        int         fd;
        uint8_t     socket_event;
        BaseSocket* base_socket;
    } RegisterEvent;
private:
    pthread_t           thread_id_;
    int                 event_fd_;
    bool                stop_;
    int                 wakeup_fds_[2];
	list<TimerItem*>	timer_list_;
	list<TimerItem*>	loop_list_;
    SocketMap           socket_map_;
    
    mutex               mutex_;
    list<RegisterEvent> register_event_list_;
};

void init_thread_event_loops(int io_thread_num);
void destroy_thread_event_loops(int io_thread_num);

EventLoop* get_main_event_loop();
EventLoop* get_io_event_loop(net_handle_t handle);

#endif
