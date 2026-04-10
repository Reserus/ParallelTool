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

    enum class TaskState {
        Pending,
        Queued,
        Running,
        Finished,
        Failed,
        Skipped
    };

    std::vector<std::atomic<int>> remaining_deps(graph.size());
    for (std::size_t i = 0; i < graph.size(); ++i) {
        remaining_deps[i].store(static_cast<int>(graph.task(i).prev.size()));
    }

    std::vector<std::atomic<TaskState>> states(graph.size());
    for (auto& state : states) {
        state.store(TaskState::Pending);
    }

    std::atomic<std::size_t> settled{0};
    std::atomic<bool> failed{false};

    std::mutex done_mutex;
    std::condition_variable done_cv;

    std::mutex error_mutex;
    std::exception_ptr first_exception = nullptr;

    auto mark_settled = [&]() {
        const std::size_t value = settled.fetch_add(1) + 1;
        if (value == graph.size()) {
            std::lock_guard<std::mutex> lock(done_mutex);
            done_cv.notify_one();
        }
    };

    std::function<void(TaskGraph::TaskId)> cancel_subtree;
    std::function<void(TaskGraph::TaskId)> schedule_task;

    cancel_subtree = [&](TaskGraph::TaskId id) {
        for (auto nxt : graph.task(id).next) {
            TaskState current = states[nxt].load();

            while (true) {
                if (current == TaskState::Pending || current == TaskState::Queued) {
                    if (states[nxt].compare_exchange_weak(current, TaskState::Skipped)) {
                        mark_settled();
                        cancel_subtree(nxt);
                        break;
                    }
                    continue;
                }

                // Уже в работе или уже завершена/пропущена/упала
                break;
            }
        }
    };

    schedule_task = [&](TaskGraph::TaskId id) {
        TaskState expected = TaskState::Pending;
        if (!states[id].compare_exchange_strong(expected, TaskState::Queued)) {
            return;
        }

        try {
            pool_.submit([&, id]() {
                TaskState expected_state = TaskState::Queued;

                // Если граф уже сломан до старта этой задачи
                if (failed.load()) {
                    if (states[id].compare_exchange_strong(expected_state, TaskState::Skipped)) {
                        mark_settled();
                    }
                    return;
                }

                if (!states[id].compare_exchange_strong(expected_state, TaskState::Running)) {
                    return;
                }

                try {
                    graph.task(id).func(context);
                    states[id].store(TaskState::Finished);

                    if (!failed.load()) {
                        for (auto nxt : graph.task(id).next) {
                            if (--remaining_deps[nxt] == 0) {
                                schedule_task(nxt);
                            }
                        }
                    }
                } catch (...) {
                    states[id].store(TaskState::Failed);
                    failed.store(true);

                    {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        if (!first_exception) {
                            first_exception = std::current_exception();
                        }
                    }

                    cancel_subtree(id);
                }

                mark_settled();
            });
        } catch (...) {
            states[id].store(TaskState::Failed);
            failed.store(true);

            {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (!first_exception) {
                    first_exception = std::current_exception();
                }
            }

            cancel_subtree(id);
            mark_settled();
        }
    };

    for (std::size_t i = 0; i < graph.size(); ++i) {
        if (remaining_deps[i] == 0) {
            schedule_task(i);
        }
    }

    {
        std::unique_lock<std::mutex> lock(done_mutex);
        done_cv.wait(lock, [&]() {
            return settled.load() == graph.size();
        });
    }

    if (first_exception) {
        std::rethrow_exception(first_exception);
    }
}