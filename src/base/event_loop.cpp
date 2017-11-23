/*
 * event_loop.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/epoll.h>
#endif
#include "util.h"
#include "event_loop.h"
#include "base_socket.h"
#include "io_thread_resource.h"

IoThreadResource<EventLoop> g_event_loops;

void timer_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    if (callback_data) {
        EventLoop* el = (EventLoop*)callback_data;
        el->OnTimer();
    }
}

static void* event_loop_thread(void* arg)
{
    int event_loop_idx = (int)(long)arg;
    assert(event_loop_idx < g_event_loops.GetThreadNum());
    
    EventLoop* el = g_event_loops.GetIOResource(event_loop_idx);
    el->SetThreadId(pthread_self());
    el->AddTimer(timer_callback, el, 1000);
    el->Start();
    
    return NULL;
}

void init_thread_event_loops(int io_thread_num)
{
    if (g_event_loops.IsInited())
        return;
    
    signal(SIGPIPE, SIG_IGN);
    srand((unsigned)time(NULL));
    
    g_event_loops.Init(io_thread_num);
    
    g_event_loops.GetMainResource()->SetThreadId(pthread_self());
    g_event_loops.GetMainResource()->AddTimer(timer_callback, g_event_loops.GetMainResource(), 1000);
    
    for (long i = 0; i < io_thread_num; ++i) {
        pthread_t thread_id;
        pthread_create(&thread_id, NULL, event_loop_thread, (void*)i);
    }
}

void destroy_thread_event_loops(int io_thread_num)
{
    for (int i = 0; i < io_thread_num; i++) {
        EventLoop* el = g_event_loops.GetIOResource(i);
        el->Stop();
        pthread_join(el->GetThreadId(), NULL);
    }
    
    g_event_loops.GetMainResource()->Stop();
}

EventLoop* get_main_event_loop()
{
    return g_event_loops.GetMainResource();
}

EventLoop* get_io_event_loop(net_handle_t handle)
{
    return g_event_loops.GetIOResource(handle);
}

///////////////////////
EventLoop::EventLoop()
{
#ifdef __APPLE__
	event_fd_ = kqueue();
#else 
    event_fd_ = epoll_create(1024);
#endif
	assert(event_fd_ >= 0);
    
    wakeup_fds_[0] = wakeup_fds_[1] = -1;
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, wakeup_fds_) == 0);
    
    BaseSocket::SetNonblock(wakeup_fds_[0], true);
    BaseSocket::SetNonblock(wakeup_fds_[1], true);
    stop_ = false;
}

EventLoop::~EventLoop()
{
    if (event_fd_ != -1) {
        close(event_fd_);
    }
    
    if (wakeup_fds_[0] != -1) {
        close(wakeup_fds_[0]);
        close(wakeup_fds_[1]);
    }
}

void EventLoop::AddTimer(callback_t callback, void* user_data, uint64_t interval)
{
	list<TimerItem*>::iterator it;
	for (it = timer_list_.begin(); it != timer_list_.end(); it++) {
		TimerItem* pItem = *it;
		if (pItem->callback == callback && pItem->user_data == user_data) {
			pItem->interval = interval;
			pItem->next_tick = get_tick_count() + interval;
			return;
		}
	}

	TimerItem* pItem = new TimerItem;
	pItem->callback = callback;
	pItem->user_data = user_data;
	pItem->interval = interval;
	pItem->next_tick = get_tick_count() + interval;
	timer_list_.push_back(pItem);
}

void EventLoop::RemoveTimer(callback_t callback, void* user_data)
{
	list<TimerItem*>::iterator it;
	for (it = timer_list_.begin(); it != timer_list_.end(); it++) {
		TimerItem* pItem = *it;
		if (pItem->callback == callback && pItem->user_data == user_data) {
			timer_list_.erase(it);
			delete pItem;
			return;
		}
	}
}

void EventLoop::_CheckTimer()
{
	uint64_t curr_tick = get_tick_count();
	list<TimerItem*>::iterator it;

	for (it = timer_list_.begin(); it != timer_list_.end(); ) {
		TimerItem* pItem = *it;
		it++;		// iterator maybe deleted in the callback, so we should increment it before callback
		if (curr_tick >= pItem->next_tick) {
			pItem->next_tick += pItem->interval;
			pItem->callback(pItem->user_data, NETLIB_MSG_TIMER, 0, NULL);
		}
	}
}

void EventLoop::AddLoop(callback_t callback, void* user_data)
{
	TimerItem* pItem = new TimerItem;
	pItem->callback = callback;
	pItem->user_data = user_data;
	loop_list_.push_back(pItem);
}

void EventLoop::_CheckLoop()
{
	for (auto it = loop_list_.begin(); it != loop_list_.end(); it++) {
		TimerItem* pItem = *it;
		pItem->callback(pItem->user_data, NETLIB_MSG_LOOP, 0, NULL);
	}
}

void EventLoop::_ReadWakeupData()
{
    char buf[16];
    while (read(wakeup_fds_[0], buf, 16) > 0)
        ;
}

void EventLoop::_RegisterEventList()
{
    list<RegisterEvent> event_list;
    {
        lock_guard<mutex> mg(mutex_);
        if (register_event_list_.empty()) {
            return;
        }
        event_list.swap(register_event_list_);
    }
    
    for (auto it = event_list.begin(); it != event_list.end(); ++it) {
        AddEvent(it->fd, it->socket_event, it->base_socket);
    }
}

void EventLoop::Wakeup()
{
    char buf = 0;
    send(wakeup_fds_[1], &buf, 1, 0);
}

void EventLoop::OnTimer()
{
    uint64_t curr_tick = get_tick_count();
    SocketMap::iterator it_old;
    for (auto it = socket_map_.begin(); it != socket_map_.end(); ) {
        it_old = it;
        ++it;   // iterate to the next before OnTimer, cause OnTimer may close the connection and make iterator useless
        it_old->second->OnTimer(curr_tick);
    }
}

BaseSocket* EventLoop::FindBaseSocket(net_handle_t handle)
{
    SocketMap::iterator it = socket_map_.find(handle);
    if (it != socket_map_.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

void EventLoop::AddEvent(int fd, uint8_t socket_event, BaseSocket* pSocket)
{
    if (IsInLoopThread()) {
#ifdef __APPLE__
        struct kevent ke;
        void* udata = NULL;
        if (pSocket) {
            udata = reinterpret_cast<void*>(pSocket->GetHandle());
        }
        
        if ((socket_event & SOCKET_READ) != 0) {
            EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, udata);
            kevent(event_fd_, &ke, 1, NULL, 0, NULL);
        }

        if ((socket_event & SOCKET_WRITE) != 0) {
            EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, udata);
            kevent(event_fd_, &ke, 1, NULL, 0, NULL);
        }
        
#else
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLPRI | EPOLLERR | EPOLLHUP;
        ev.data.ptr = pSocket;
        if (epoll_ctl(event_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
            printf("epoll_ctl() failed, errno=%d\n", errno);
        }
#endif
        
        if ( ((socket_event & SOCKET_ADD_CONN) != 0) && pSocket) {
            //printf("add conn: %d\n", pSocket->GetHandle());
            socket_map_.insert(make_pair(pSocket->GetHandle(), pSocket));
        }
        
        if ( ((socket_event & SOCKET_CONNECT_CB) != 0) && pSocket) {
            pSocket->OnConnect();
        }
    } else {
        RegisterEvent re = {fd, socket_event, pSocket};
        {
            lock_guard<mutex> mg(mutex_);
            register_event_list_.push_back(re);
        }
        Wakeup();
    }
}

void EventLoop::RemoveEvent(int fd, uint8_t socket_event, BaseSocket* pSocket)
{
#ifdef __APPLE__
	struct kevent ke;
    void* udata = NULL;
    if (pSocket) {
        udata = reinterpret_cast<void*>(pSocket->GetHandle());
    }
    
	if ((socket_event & SOCKET_READ) != 0) {
		EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, udata);
		kevent(event_fd_, &ke, 1, NULL, 0, NULL);
	}

	if ((socket_event & SOCKET_WRITE) != 0) {
		EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, udata);
		kevent(event_fd_, &ke, 1, NULL, 0, NULL);
	}
#else
    if (epoll_ctl(event_fd_, EPOLL_CTL_DEL, fd, NULL) != 0) {
		printf("epoll_ctl failed, errno=%d\n", errno);
	}
#endif
    
    if ( ((socket_event & SOCKET_DEL_CONN) != 0) && pSocket) {
        //printf("delete conn: %d\n", pSocket->GetHandle());
        socket_map_.erase(pSocket->GetHandle());
    }
}

#ifdef __APPLE__
void EventLoop::Start(uint32_t wait_timeout)
{
	struct kevent events[1024];
	int nfds = 0;
	struct timespec timeout;

	timeout.tv_sec = wait_timeout / 1000;
	timeout.tv_nsec = (wait_timeout % 1000) * 1000000;

    AddEvent(wakeup_fds_[0], SOCKET_READ, NULL);
    
	while (!stop_) {
		nfds = kevent(event_fd_, NULL, 0, events, 1024, &timeout);

		for (int i = 0; i < nfds; i++) {
            if (!events[i].udata) {
                _ReadWakeupData();
                continue;
            }
            
            /*
             ** use handle instead of BaseSocket pointer **
             Because kevent will return two event(read and write) for the same socket,
             if in the process of the first event, the connection was deleted, then it will crash 
             when process the second event
             */
            net_handle_t handle = (int)reinterpret_cast<long>(events[i].udata);
            BaseSocket* pSocket = FindBaseSocket(handle);
            if (!pSocket)
                continue;
            
            pSocket->AddRef();
			if (events[i].filter == EVFILT_READ) {
				pSocket->OnRead();
			} else if (events[i].filter == EVFILT_WRITE) {
				pSocket->OnWrite();
			}
            pSocket->ReleaseRef();
		}

		_CheckTimer();
		_CheckLoop();
        
        _RegisterEventList();
	}
}

#else

void EventLoop::Start(uint32_t wait_timeout)
{
	struct epoll_event events[1024];
	int nfds = 0;
    
    AddEvent(wakeup_fds_[0], SOCKET_READ, NULL);
    
	while (!stop_) {
		nfds = epoll_wait(event_fd_, events, 1024, wait_timeout);
		for (int i = 0; i < nfds; i++) {
			BaseSocket* pSocket = (BaseSocket*)events[i].data.ptr;
            if (!pSocket) {
                _ReadWakeupData();
                continue;
            }

            pSocket->AddRef();
			if (events[i].events & EPOLLIN) {
				pSocket->OnRead();
			}

			if (events[i].events & EPOLLOUT) {
				pSocket->OnWrite();
			}

			if (events[i].events & (EPOLLPRI | EPOLLERR | EPOLLHUP)) {
				pSocket->OnClose();
			}
            pSocket->ReleaseRef();
		}

		_CheckTimer();
		_CheckLoop();

        _RegisterEventList();
	}
}
#endif
