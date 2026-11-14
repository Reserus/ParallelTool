#include "executor.hpp"
#include "threads_driver.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

template <class F>
void expect_throw(F&& function) {
    bool threw = false;
    try {
        function();
    } catch (const std::exception&) {
        threw = true;
    }
    assert(threw);
}

void run_graph(TaskGraph& graph, TaskContext& context) {
    ThreadsDriver driver(4);
    Executor(driver).run(graph, context);
}

void test_chain_and_diamond() {
    TaskGraph graph;
    TaskContext context;
    graph.add_task("producer", TaskAccessSpec{{}, {"a"}, {}}, [](TaskAccess& a) {
        a.write<int>("a") = 2;
    });
    graph.add_task("middle", TaskAccessSpec{{"a"}, {"b"}, {}}, [](TaskAccess& a) {
        a.write<int>("b") = a.read<int>("a") + 1;
    });
    graph.add_task("left", TaskAccessSpec{{"b"}, {"left"}, {}}, [](TaskAccess& a) {
        a.write<int>("left") = a.read<int>("b");
    });
    graph.add_task("right", TaskAccessSpec{{"b"}, {"right"}, {}}, [](TaskAccess& a) {
        a.write<int>("right") = a.read<int>("b") * 2;
    });
    run_graph(graph, context);
    assert(context.get_copy<int>("left") == 3);
    assert(context.get_copy<int>("right") == 6);
    assert(graph.task(1).prev.empty());
    assert(graph.task(2).prev.empty());
    TaskContext second_context;
    run_graph(graph, second_context);
    assert(second_context.get_copy<int>("right") == 6);
}

void test_declared_before_producer_and_initial_input() {
    TaskGraph graph;
    TaskContext context;
    context.set("input", 4);
    graph.add_task("consumer", TaskAccessSpec{{"generated", "input"}, {"result"}, {}}, [](TaskAccess& a) {
        a.write<int>("result") = a.read<int>("generated") + a.read<int>("input");
    });
    graph.add_task("producer", TaskAccessSpec{{}, {"generated"}, {}}, [](TaskAccess& a) {
        a.write<int>("generated") = 5;
    });
    run_graph(graph, context);
    assert(context.get_copy<int>("result") == 9);
}

void test_graph_validation() {
    {
        TaskGraph graph;
        TaskContext context;
        graph.add_task("one", TaskAccessSpec{{}, {"x"}, {}}, [](TaskAccess& a) { a.write<int>("x") = 1; });
        graph.add_task("two", TaskAccessSpec{{}, {"x"}, {}}, [](TaskAccess& a) { a.write<int>("x") = 2; });
        expect_throw([&] { run_graph(graph, context); });
    }
    {
        TaskGraph graph;
        TaskContext context;
        context.set("x", 0);
        graph.add_task("writer", TaskAccessSpec{{}, {"x"}, {}}, [](TaskAccess& a) { a.write<int>("x") = 1; });
        expect_throw([&] { run_graph(graph, context); });
    }
    {
        TaskGraph graph;
        TaskContext context;
        graph.add_task("missing", TaskAccessSpec{{"x"}, {"y"}, {}}, [](TaskAccess&) {});
        expect_throw([&] { run_graph(graph, context); });
    }
    {
        TaskGraph graph;
        TaskContext context;
        const auto a = graph.add_task("a", TaskAccessSpec{{"b"}, {"a_out"}, {}}, [](TaskAccess& x) {
            x.write<int>("a_out") = x.read<int>("b");
        });
        const auto b = graph.add_task("b", TaskAccessSpec{{"a_out"}, {"b"}, {}}, [](TaskAccess& x) {
            x.write<int>("b") = x.read<int>("a_out");
        });
        graph.add_dependency(a, b);
        expect_throw([&] { run_graph(graph, context); });
    }
    {
        TaskGraph graph;
        TaskContext context;
        graph.add_task("bad_rw", TaskAccessSpec{{}, {}, {{"x", "x"}}}, [](TaskAccess&) {});
        expect_throw([&] { run_graph(graph, context); });
    }
    {
        TaskGraph graph;
        TaskContext context;
        context.set("x", 1);
        graph.add_task("undeclared", TaskAccessSpec{{"x"}, {"y"}, {}}, [](TaskAccess& a) {
            a.write<int>("not_y") = a.read<int>("x");
        });
        expect_throw([&] { run_graph(graph, context); });
    }
}

void test_access_rollback_and_missing_output() {
    {
        TaskGraph graph;
        TaskContext context;
        context.set("source", std::vector<int>{1, 2});
        graph.add_task("fail", TaskAccessSpec{{}, {}, {{"source", "copy"}}}, [](TaskAccess& a) {
            a.read_write<std::vector<int>>("source", "copy").push_back(3);
            throw std::runtime_error("failure");
        });
        expect_throw([&] { run_graph(graph, context); });
        assert(!context.contains("copy"));
        const std::vector<int> expected{1, 2};
        assert(context.get_copy<std::vector<int>>("source") == expected);
    }
    {
        TaskGraph graph;
        TaskContext context;
        graph.add_task("missing_output", TaskAccessSpec{{}, {"output"}, {}}, [](TaskAccess&) {});
        expect_throw([&] { run_graph(graph, context); });
        assert(!context.contains("output"));
    }
    {
        TaskGraph graph;
        TaskContext context;
        std::atomic<bool> completed{false};
        graph.add_task("fail", {}, {}, [](TaskContext&) { throw std::runtime_error("failure"); });
        graph.add_task("slow", {}, {}, [&](TaskContext&) {
            std::this_thread::sleep_for(100ms);
            completed = true;
        });
        expect_throw([&] { run_graph(graph, context); });
        assert(completed.load());
    }
}

int main() {
    test_chain_and_diamond();
    test_declared_before_producer_and_initial_input();
    test_graph_validation();
    test_access_rollback_and_missing_output();
}
