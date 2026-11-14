#include "threads_driver.hpp"

#include <atomic>
#include <exception>
#include <queue>
#include <stdexcept>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

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

void ThreadsDriver::validate_graph(
    const TaskGraph& graph,
    const TaskContext& context,
    std::vector<std::vector<TaskGraph::TaskId>>& next,
    std::vector<std::vector<TaskGraph::TaskId>>& prev) const {
    const std::size_t n = graph.size();
    next.assign(n, {});
    prev.assign(n, {});

    std::unordered_map<std::string, TaskGraph::TaskId> producers;
    for (std::size_t i = 0; i < n; ++i) {
        const auto& node = graph.task(i);
        std::unordered_set<std::string> outputs;
        outputs.insert(node.outputs.begin(), node.outputs.end());
        for (const auto& key : outputs) {
            if (!producers.emplace(key, i).second) {
                throw std::runtime_error("ThreadsDriver: duplicate producer for key: " + key);
            }
            if (context.contains(key)) {
                throw std::runtime_error("ThreadsDriver: task output overwrites initial key: " + key);
            }
        }

        for (const auto& access : node.access_spec.read_writes) {
            if (access.first == access.second) {
                throw std::runtime_error("ThreadsDriver: read/write input and output must differ: " + access.first);
            }
        }
    }

    auto add_edge = [&](TaskGraph::TaskId from, TaskGraph::TaskId to) {
        if (from == to) {
            throw std::runtime_error("ThreadsDriver: task depends on itself");
        }
        if (std::find(next[from].begin(), next[from].end(), to) == next[from].end()) {
            next[from].push_back(to);
            prev[to].push_back(from);
        }
    };

    for (std::size_t i = 0; i < n; ++i) {
        for (auto to : graph.task(i).next) {
            add_edge(i, to);
        }
    }

    for (std::size_t i = 0; i < n; ++i) {
        const auto& node = graph.task(i);
        std::unordered_set<std::string> inputs(node.inputs.begin(), node.inputs.end());
        for (const auto& access : node.access_spec.read_writes) {
            inputs.insert(access.first);
        }
        for (const auto& key : inputs) {
            const auto producer = producers.find(key);
            if (producer == producers.end()) {
                if (!context.contains(key)) {
                    throw std::runtime_error("ThreadsDriver: missing input: " + key);
                }
            } else {
                add_edge(producer->second, i);
            }
        }
    }

    std::vector<int> indegree(n, 0);

    for (std::size_t i = 0; i < n; ++i) {
        indegree[i] = static_cast<int>(prev[i].size());
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

        for (auto to : next[v]) {
            --indegree[to];
            if (indegree[to] == 0) {
                q.push(to);
            }
        }
    }

    if (visited != n) {
        throw std::runtime_error("ThreadsDriver: task graph contains a cycle");
    }
}

void ThreadsDriver::run(TaskGraph& graph, TaskContext& context) {
    if (graph.empty()) {
        return;
    }

    std::vector<std::vector<TaskGraph::TaskId>> next;
    std::vector<std::vector<TaskGraph::TaskId>> prev;
    validate_graph(graph, context, next, prev);

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
        remaining_deps[i].store(static_cast<int>(prev[i].size()));
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
        for (auto nxt : next[id]) {
            TaskState current = states[nxt].load();

            while (true) {
                if (current == TaskState::Pending || current == TaskState::Queued) {
                    if (states[nxt].compare_exchange_weak(current, TaskState::Skipped)) {
                        if (current == TaskState::Pending) {
                            mark_settled();
                        }
                        cancel_subtree(nxt);
                        break;
                    }
                    continue;
                }

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

                if (failed.load()) {
                    states[id].compare_exchange_strong(expected_state, TaskState::Skipped);
                    mark_settled();
                    return;
                }

                if (!states[id].compare_exchange_strong(expected_state, TaskState::Running)) {
                    mark_settled();
                    return;
                }

                try {
                    if (graph.task(id).access_func) {
                        TaskAccess access(context, graph.task(id).access_spec);
                        graph.task(id).access_func(access);
                        access.commit();
                    } else {
                        graph.task(id).func(context);
                    }
                    states[id].store(TaskState::Finished);

                    if (!failed.load()) {
                        for (auto nxt : next[id]) {
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

                    for (std::size_t i = 0; i < graph.size(); ++i) {
                        TaskState current = states[i].load();
                        while (current == TaskState::Pending || current == TaskState::Queued) {
                            if (states[i].compare_exchange_weak(current, TaskState::Skipped)) {
                                if (current == TaskState::Pending) {
                                    mark_settled();
                                }
                                break;
                            }
                        }
                    }
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

            for (std::size_t i = 0; i < graph.size(); ++i) {
                TaskState current = states[i].load();
            while (current == TaskState::Pending || current == TaskState::Queued) {
                if (states[i].compare_exchange_weak(current, TaskState::Skipped)) {
                    if (current == TaskState::Pending) {
                        mark_settled();
                    }
                    break;
                    }
                }
            }
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
