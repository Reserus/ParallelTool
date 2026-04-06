#include "threads_driver.hpp"

#include <atomic>
#include <exception>
#include <queue>
#include <stdexcept>
#include <vector>

ThreadsDriver::ThreadPool::ThreadPool(std::size_t thread_count)
    : stop_(false) {
    if (thread_count == 0) {
        thread_count = 4;
    }

    for (std::size_t i = 0; i < thread_count; ++i) {
        workers_.emplace_back([this]() {
            while (true) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cv_.wait(lock, [this]() {
                        return stop_ || !tasks_.empty();
                    });

                    if (stop_ && tasks_.empty()) {
                        return;
                    }

                    task = std::move(tasks_.front());
                    tasks_.pop();
                }

                task();
            }
        });
    }
}

ThreadsDriver::ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }

    cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadsDriver::ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_) {
            throw std::runtime_error("ThreadPool is stopped");
        }
        tasks_.push(std::move(task));
    }

    cv_.notify_one();
}

ThreadsDriver::ThreadsDriver(std::size_t thread_count)
    : pool_(thread_count) {
}

ThreadsDriver::~ThreadsDriver() = default;

bool ThreadsDriver::is_acyclic(const TaskGraph& graph) const {
    const std::size_t n = graph.size();
    std::vector<int> indegree(n, 0);

    for (std::size_t i = 0; i < n; ++i) {
        indegree[i] = static_cast<int>(graph.task(i).prev.size());
    }

    std::queue<TaskGraph::TaskId> q;
    for (std::size_t i = 0; i < n; ++i) {
        if (indegree[i] == 0) {
            q.push(i);
        }
    }

    std::size_t visited = 0;

    while (!q.empty()) {
        auto v = q.front();
        q.pop();
        ++visited;

        for (auto to : graph.task(v).next) {
            --indegree[to];
            if (indegree[to] == 0) {
                q.push(to);
            }
        }
    }

    return visited == n;
}

void ThreadsDriver::run(TaskGraph& graph, TaskContext& context) {
    if (graph.empty()) {
        return;
    }

    if (!is_acyclic(graph)) {
        throw std::runtime_error("ThreadsDriver: task graph contains a cycle");
    }

    std::vector<std::atomic<int>> remaining_deps(graph.size());
    for (std::size_t i = 0; i < graph.size(); ++i) {
        remaining_deps[i].store(static_cast<int>(graph.task(i).prev.size()));
    }

    std::atomic<std::size_t> completed{0};
    std::mutex done_mutex;
    std::condition_variable done_cv;

    std::mutex error_mutex;
    std::exception_ptr first_exception = nullptr;

    std::function<void(TaskGraph::TaskId)> schedule_task;

    schedule_task = [&](TaskGraph::TaskId id) {
        pool_.submit([&, id]() {
            try {
                graph.task(id).func(context);
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (!first_exception) {
                    first_exception = std::current_exception();
                }
            }

            for (auto nxt : graph.task(id).next) {
                if (--remaining_deps[nxt] == 0) {
                    schedule_task(nxt);
                }
            }

            if (++completed == graph.size()) {
                std::lock_guard<std::mutex> lock(done_mutex);
                done_cv.notify_one();
            }
        });
    };

    for (std::size_t i = 0; i < graph.size(); ++i) {
        if (remaining_deps[i] == 0) {
            schedule_task(i);
        }
    }

    {
        std::unique_lock<std::mutex> lock(done_mutex);
        done_cv.wait(lock, [&]() {
            return completed == graph.size();
        });
    }

    if (first_exception) {
        std::rethrow_exception(first_exception);
    }
}