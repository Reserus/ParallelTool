#pragma once

#include "execution_driver.hpp"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadsDriver final : public IExecutionDriver {
    class ThreadPool {
    public:
        explicit ThreadPool(std::size_t thread_count);
        ~ThreadPool();

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;

        void submit(std::function<void()> task);

    private:
        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::mutex mutex_;
        std::condition_variable cv_;
        bool stop_ = false;
    };

private:
    bool is_acyclic(const TaskGraph& graph) const;

private:
    ThreadPool pool_;
public:
    explicit ThreadsDriver(std::size_t thread_count = std::thread::hardware_concurrency());
    ~ThreadsDriver() override;

    ThreadsDriver(const ThreadsDriver&) = delete;
    ThreadsDriver& operator=(const ThreadsDriver&) = delete;

    void run(TaskGraph& graph, TaskContext& context) override;

};