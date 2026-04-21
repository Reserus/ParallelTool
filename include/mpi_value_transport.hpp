#pragma once

#include "task_context.hpp"
#include "task_value_serialization.hpp"

#include <mpi.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace mpi_transport {

inline void send_buffer(const std::vector<std::uint8_t>& buffer,
                        int dest,
                        int tag,
                        MPI_Comm comm = MPI_COMM_WORLD) {
    const std::uint64_t size = static_cast<std::uint64_t>(buffer.size());

    int rc = MPI_Send(&size, 1, MPI_UINT64_T, dest, tag, comm);
    if (rc != MPI_SUCCESS) {
        throw std::runtime_error("send_buffer: failed to send buffer size");
    }

    if (size > 0) {
        rc = MPI_Send(buffer.data(),
                      static_cast<int>(size),
                      MPI_UINT8_T,
                      dest,
                      tag + 1,
                      comm);
        if (rc != MPI_SUCCESS) {
            throw std::runtime_error("send_buffer: failed to send buffer data");
        }
    }
}

inline std::vector<std::uint8_t> recv_buffer(int source,
                                             int tag,
                                             MPI_Comm comm = MPI_COMM_WORLD) {
    std::uint64_t size = 0;

    int rc = MPI_Recv(&size,
                      1,
                      MPI_UINT64_T,
                      source,
                      tag,
                      comm,
                      MPI_STATUS_IGNORE);
    if (rc != MPI_SUCCESS) {
        throw std::runtime_error("recv_buffer: failed to receive buffer size");
    }

    std::vector<std::uint8_t> buffer(static_cast<std::size_t>(size));

    if (size > 0) {
        rc = MPI_Recv(buffer.data(),
                      static_cast<int>(size),
                      MPI_UINT8_T,
                      source,
                      tag + 1,
                      comm,
                      MPI_STATUS_IGNORE);
        if (rc != MPI_SUCCESS) {
            throw std::runtime_error("recv_buffer: failed to receive buffer data");
        }
    }

    return buffer;
}

inline void send_value(const TaskContext::Value& value,
                       int dest,
                       int tag,
                       MPI_Comm comm = MPI_COMM_WORLD) {
    const auto buffer = task_serialization::serialize_value(value);
    send_buffer(buffer, dest, tag, comm);
}

inline TaskContext::Value recv_value(int source,
                                     int tag,
                                     MPI_Comm comm = MPI_COMM_WORLD) {
    const auto buffer = recv_buffer(source, tag, comm);
    return task_serialization::deserialize_value(buffer);
}

}