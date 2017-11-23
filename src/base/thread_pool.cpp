/*
 * thread_pool.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#include <stdlib.h>
#include <assert.h>
#include "util.h"
#include "thread_pool.h"


ThreadNotify::ThreadNotify()
{
	pthread_mutex_init(&m_mutex, NULL);
	pthread_cond_init(&m_cond, NULL);
}

ThreadNotify::~ThreadNotify()
{
	pthread_mutex_destroy(&m_mutex);
	pthread_cond_destroy(&m_cond);
}

///////////
WorkerThread::WorkerThread()
{
    m_stop = false;
    m_task_cnt = 0;
}

WorkerThread::~WorkerThread()
{

}

void* WorkerThread::StartRoutine(void* arg)
{
	WorkerThread* pThread = (WorkerThread*)arg;

	pThread->Execute();

	return NULL;
}

void WorkerThread::Start()
{
	(void)pthread_create(&m_thread_id, NULL, StartRoutine, this);
}

void WorkerThread::Stop()
{
    m_stop = true;
    m_thread_notify.Signal();
}

void WorkerThread::Execute()
{
	while (!m_stop) {
		m_thread_notify.Lock();

		// put wait in while cause there can be spurious wake up (due to signal/ENITR)
		while (m_task_list.empty() && !m_stop) {
			m_thread_notify.Wait();
		}
        
        list<Task*> tmp_task_list;
        tmp_task_list.swap(m_task_list);
        
        m_thread_notify.Unlock();
        
		for (list<Task*>::iterator it = tmp_task_list.begin(); it != tmp_task_list.end(); ++it) {
            Task* task = *it;
            task->setThreadIdx(m_thread_idx);
            task->run();
            delete task;
            m_task_cnt--;
        }
	}
}

void WorkerThread::PushTask(Task* pTask)
{
	m_thread_notify.Lock();
	m_task_list.push_back(pTask);
	m_thread_notify.Signal();
	m_thread_notify.Unlock();
    
    m_task_cnt++;
}


//////////////
ThreadPool::ThreadPool()
{
	m_thread_num = 0;
	m_worker_threads = NULL;
}

ThreadPool::~ThreadPool()
{

}

int ThreadPool::Init(uint32_t thread_num)
{
    m_thread_num = thread_num;
	m_worker_threads = new WorkerThread [m_thread_num];
	assert(m_worker_threads);

	for (uint32_t i = 0; i < m_thread_num; ++i) {
		m_worker_threads[i].SetThreadIdx(i);
		m_worker_threads[i].Start();
	}

	return 0;
}

void ThreadPool::Destory()
{
    for (uint32_t i = 0; i < m_thread_num; ++i) {
        m_worker_threads[i].Stop();
    }
    
    for (uint32_t i = 0; i < m_thread_num; ++i) {
        pthread_join(m_worker_threads[i].GetThreadId(), NULL);
    }

    delete [] m_worker_threads;
}

void ThreadPool::AddTask(Task* pTask)
{
	/*
	 * select a random thread to push task
	 * we can also select a thread that has less task to do
	 * but that will scan the whole threads vector and use thread lock to get each task size
	 */
	uint32_t thread_idx = random() % m_thread_num;
	m_worker_threads[thread_idx].PushTask(pTask);
}

void ThreadPool::AddTask(Task* pTask, uint32_t thread_idx)
{
    if (thread_idx >= m_thread_num) {
        AddTask(pTask);
        return;
    }
    
    m_worker_threads[thread_idx].PushTask(pTask);
}

// not very precise because of without a mutex to synchronization
int ThreadPool::GetTotalTaskCnt()
{
    int total_task_cnt = 0;
    for (uint32_t i = 0; i < m_thread_num; ++i) {
        total_task_cnt += m_worker_threads[i].GetTaskCnt();
    }
    return total_task_cnt;
}
