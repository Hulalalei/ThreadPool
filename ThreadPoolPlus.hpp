#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <memory>
#include <queue>
#include <map>
#include <pthread.h>
#include <unistd.h>


// C++11多线程
class ThreadPool;
class TaskQueue;
class Task;


class ThreadPool {
public:
    explicit ThreadPool() = default;
    explicit ThreadPool(int maxNum, int minNum, int capacity);
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    auto addTask(void (*func)(void *arg), void *arg) -> bool;
    ~ThreadPool();
private:
    pthread_t managerId;
    std::unique_ptr<std::map<pthread_t, bool>, void(*)(std::map<pthread_t, bool>*)> workerMaps;
    static auto manager(void *arg) -> void*;
    static auto worker(void *arg) -> void*;

    auto takeTask() -> Task*;
    auto addWorker(pthread_t tid) -> bool;
    auto getLive() -> int;
    const int maxThreadNum;
    const int minThreadNum;
    bool isShut;
    bool kill;
    const int killNum;

    TaskQueue *taskQueue;

    pthread_mutex_t mutexPool;
    pthread_cond_t isFull, isEmpty;
};

class TaskQueue {
friend ThreadPool;
public:
    TaskQueue() = default;
    TaskQueue(int capacity);
    ~TaskQueue();
private:
    const int queueCap;
    int currNum;
    std::queue<Task*> queue;
};

class Task {
public:
    Task();
    Task(void (*func)(void *arg), void *arg);
    ~Task();
    void (*func)(void *arg);
    void *arg;
};