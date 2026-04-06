cmake_minimum_required(VERSION 3.16)

project(ParallelTools LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(parallel_tools
    src/threads_driver.cpp
    src/mpi_driver.cpp
)

target_include_directories(parallel_tools
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(parallel_tools PRIVATE
    -Wall -Wextra -Wpedantic
)

add_executable(example_basic
    examples/basic_example.cpp
)

target_link_libraries(example_basic
    PRIVATE
        parallel_tools
)