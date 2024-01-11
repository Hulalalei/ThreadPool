#include "ThreadPoolPlus.hpp"


const int killsnum = 2;

Task::Task() {
    this->func = nullptr;
    this->arg = nullptr;
}

Task::Task(void (*func)(void *arg), void *arg) {
    this->func = func;
    this->arg = arg;
}

TaskQueue::TaskQueue(int capacity): queueCap(capacity) {
    this->currNum = queue.size();
}

// 类内先加载成员变量，然后才加载成员函数
ThreadPool::ThreadPool(int minNum, int maxNum, int capacity): isShut(false), kill(false), killNum(killsnum),
        maxThreadNum(maxNum), minThreadNum(minNum), taskQueue(new TaskQueue(capacity)), 
        workerMaps(new std::map<pthread_t, bool>, [](std::map<pthread_t, bool> *ptr) -> 
        void { 
            //std::cout << "begin to delete...\n";
            for (auto &t : *ptr) {
                //std::cout << "workerId = " << t.first << " has been destroyed...\n";
                int temp = pthread_join(t.first, nullptr);
                if (temp) {
                    throw std::runtime_error("recycle resource failed...\n");
                }
            }
            delete ptr; }) {


    // 初始化互斥量
    pthread_mutex_init(&this->mutexPool, nullptr);
    pthread_cond_init(&this->isFull, nullptr);
    pthread_cond_init(&this->isEmpty, nullptr);
    
    // 创建管理者线程
    pthread_create(&this->managerId, nullptr, this->manager, this);
    //pthread_detach(this->managerId);

    // 创建消费者线程
    for (size_t i = 0; i < this->minThreadNum; i++) {
        pthread_t tid = 0;
        pthread_create(&tid, nullptr, this->worker, this);
        //pthread_detach(tid);    /* 线程分离，内核回收资源，再调用pthread_join的话，会二次回收，从而报错 */
        this->addWorker(tid);
    }
}

auto ThreadPool::manager(void *arg) -> void* {
    auto ptr = static_cast<ThreadPool*> (arg);
    while (!ptr->isShut) {
        sleep(5);   /* 每隔5s检测一次 */
        /* 添加线程 */
        pthread_mutex_lock(&ptr->mutexPool);
        int liveNum = ptr->getLive();
        if (ptr->taskQueue->currNum > 2 * liveNum) {
            if ((*ptr->workerMaps).size() < ptr->maxThreadNum) {
                pthread_t tid = 0;
                pthread_create(&tid, nullptr, ptr->worker, ptr);
                ptr->addWorker(tid);
            } else {
                for (auto &temp : *ptr->workerMaps) {
                    if (!temp.second) {
                        ptr->kill = false;
                        temp.second = true;
                    }
                }
            }
        }
        pthread_mutex_unlock(&ptr->mutexPool);

        /* 销毁线程 */
        pthread_mutex_lock(&ptr->mutexPool);
        liveNum = ptr->getLive();
        if (liveNum > ptr->taskQueue->currNum && liveNum > ptr->minThreadNum) {
            int kNum = ptr->killNum;
            pthread_mutex_unlock(&ptr->mutexPool);
            while (kNum > 0 && liveNum > ptr->minThreadNum) {
                pthread_cond_signal(&ptr->isEmpty); /* 唤醒至少一个 */
                ptr->kill = true;
                kNum--;
                liveNum--;
            }
            sleep(2);   /* 给worker时间自杀 */
            ptr->kill = false;
        }
        pthread_mutex_unlock(&ptr->mutexPool);
    }
}

auto ThreadPool::worker(void *arg) -> void* {
    auto ptr = static_cast<ThreadPool*> (arg);
    
    while (!ptr->isShut) {
        while ((*ptr->workerMaps)[pthread_self()]) {
            pthread_mutex_lock(&ptr->mutexPool);
            while (ptr->taskQueue->currNum != 0) {
                Task *p = ptr->takeTask();
                pthread_mutex_unlock(&ptr->mutexPool);
                pthread_cond_broadcast(&ptr->isFull);   /* 唤醒 */
                p->func(p->arg);
                sleep(2);   /* 线程接收任务的频率 */

                pthread_mutex_lock(&ptr->mutexPool);
            }
            while (ptr->taskQueue->currNum == 0) {
                pthread_cond_wait(&ptr->isEmpty, &ptr->mutexPool);  /* 任务执行完后都阻塞在这 */
                if (ptr->kill) {
                    goto flag;
                }
            }
            pthread_mutex_unlock(&ptr->mutexPool);
        }
    flag:
        if (ptr->kill) {
            (*ptr->workerMaps)[pthread_self()] = false;
            //std::cerr << "has been blocked...\n";
        }
        pthread_mutex_unlock(&ptr->mutexPool);
        sleep(2);   /* 与manager同步 */
    }
    //std::cout << "worker end...\n";
}

auto ThreadPool::addTask(void (*func)(void *arg), void *arg) -> bool {
    pthread_mutex_lock(&this->mutexPool);
    while (this->taskQueue->currNum >= this->taskQueue->queueCap) {
        pthread_cond_wait(&this->isFull, &this->mutexPool);
    }
    this->taskQueue->queue.push(new Task(func, arg));
    this->taskQueue->currNum++;
    pthread_mutex_unlock(&this->mutexPool);
    pthread_cond_broadcast(&this->isEmpty);
    return true;
}

auto ThreadPool::takeTask() -> Task* {
    auto task = this->taskQueue->queue.front();
    this->taskQueue->queue.pop();
    this->taskQueue->currNum--;

    return task;
}

auto ThreadPool::addWorker(pthread_t tid) -> bool {
    auto flag = (*workerMaps).find(tid);   /* 保存在容器中 */
    if ((flag != (*workerMaps).end()) && flag->second) {
        throw std::runtime_error("已存在该线程");
    } else if (flag == (*workerMaps).end()) {
        (*this->workerMaps).insert(std::pair<pthread_t, bool>(tid, true));
    } else if ((flag != (*workerMaps).end()) && !flag->second) {
        (*this->workerMaps)[tid] = true;
    }
}

auto ThreadPool::getLive() -> int {
    int liveNum = 0;
    for (auto &temp : *this->workerMaps) {
        if (temp.second) 
            liveNum++;
    }

    return liveNum;
}

Task::~Task() {
    
}

TaskQueue::~TaskQueue() {

}

ThreadPool::~ThreadPool() {
    sleep(5);   /* 主线程睡眠，防止线程无法正常执行 */
    this->kill = true;
    for (size_t i = 0; i < this->getLive(); i++) {
        pthread_cond_broadcast(&this->isEmpty);
        sleep(3);   /* 保证子线程充分执行 */
    }
    
    this->isShut = true;
    sleep(2);   /* 让worker有时间退出 */
    pthread_join(this->managerId, nullptr);
    //std::cout << "managerId = " << managerId << " has been destroyed...\n";
    this->workerMaps.reset();
    
    pthread_mutex_destroy(&this->mutexPool);
    pthread_cond_destroy(&this->isEmpty);
    pthread_cond_destroy(&this->isFull);
    
    delete this->taskQueue;
    std::cout << "ThreadPool has been destroyed...\n";
}