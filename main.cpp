#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
    
public:
    explicit ThreadPool(size_t thread_count = std::thread::hardware_concurrency())
        : stop_(false) {
        if (thread_count == 0) {
            thread_count = 4;
        }

        for (size_t i = 0; i < thread_count; ++i) {
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

    ~ThreadPool() {
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

    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }


};

class TaskGraph {
    using TaskId = size_t;
private:
    struct TaskNode {
        std::function<void()> func;
        std::vector<TaskId> next;
        int dependency_count;
    };

    std::vector<TaskNode> tasks_;

    friend class GraphExecutor;
public:


    TaskId add_task(std::function<void()> func) {
        tasks_.push_back(TaskNode{std::move(func), {}, 0});
        return tasks_.size() - 1;
    }

    void add_dependency(TaskId before, TaskId after) {
        tasks_[before].next.push_back(after);
        tasks_[after].dependency_count++;
    }

    size_t size() const {
        return tasks_.size();
    }


};

class GraphExecutor {
private:
    ThreadPool pool_;
    
public:
    explicit GraphExecutor(size_t thread_count = std::thread::hardware_concurrency())
        : pool_(thread_count) {}

    void run(TaskGraph& graph) {
        if (graph.tasks_.empty()) {
            return;
        }

        std::vector<std::atomic<int>> remaining_deps(graph.size());
        for (size_t i = 0; i < graph.size(); ++i) {
            remaining_deps[i].store(graph.tasks_[i].dependency_count);
        }

        std::atomic<size_t> completed = 0;
        std::mutex done_mutex;
        std::condition_variable done_cv;

        std::function<void(TaskGraph::TaskId)> schedule_task;

        schedule_task = [&](TaskGraph::TaskId id) {
            pool_.submit([&, id]() {
                graph.tasks_[id].func();

                for (TaskGraph::TaskId nxt : graph.tasks_[id].next) {
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

        for (size_t i = 0; i < graph.size(); ++i) {
            if (remaining_deps[i] == 0) {
                schedule_task(i);
            }
        }

        std::unique_lock<std::mutex> lock(done_mutex);
        done_cv.wait(lock, [&]() {
            return completed == graph.size();
        });
    }


};

int main() {
    TaskGraph graph;

    auto t1 = graph.add_task([]() {
        std::cout << "Task 1\n";
    });

    auto t2 = graph.add_task([]() {
        std::cout << "Task 2\n";
    });

    auto t3 = graph.add_task([]() {
        std::cout << "Task 3 (depends on 1 and 2)\n";
    });

    auto t4 = graph.add_task([]() {
        std::cout << "Task 4 (depends on 3)\n";
    });

    graph.add_dependency(t1, t3);
    graph.add_dependency(t2, t3);
    graph.add_dependency(t3, t4);

    GraphExecutor executor(4);
    executor.run(graph);

    std::cout << "All tasks completed\n";
    return 0;
}