cmake_minimum_required(VERSION 3.16)

project(ParallelTools LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(MPI REQUIRED)

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

target_link_libraries(parallel_tools
    PUBLIC
        MPI::MPI_CXX
)

add_executable(example_basic
    examples/basic_example.cpp
)

add_executable(example_failure
    examples/failure_example.cpp
)

add_executable(example_root_fail
    examples/root_fail_example.cpp
)

add_executable(example_serialization
    examples/serialization_example.cpp
)

add_executable(example_mpi_transport
    examples/mpi_value_transport_example.cpp
)

target_link_libraries(example_basic
    PRIVATE
        parallel_tools
)

target_link_libraries(example_failure
    PRIVATE
        parallel_tools
)

target_link_libraries(example_root_fail
    PRIVATE
        parallel_tools
)

target_link_libraries(example_serialization
    PRIVATE
        parallel_tools
)

target_link_libraries(example_mpi_transport
    PRIVATE
        parallel_tools
)