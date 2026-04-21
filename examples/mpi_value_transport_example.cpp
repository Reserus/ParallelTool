#include "mpi_value_transport.hpp"

#include <mpi.h>

#include <iostream>
#include <vector>

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int world_size = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_size != 2) {
        if (rank == 0) {
            std::cout << "Run with exactly 2 processes\n";
        }
        MPI_Finalize();
        return 0;
    }

    try {
        if (rank == 0) {
            TaskContext::Value value = std::vector<int>{10, 20, 30, 40};
            mpi_transport::send_value(value, 1, 100);
            std::cout << "[rank 0] value sent\n";
        } else if (rank == 1) {
            TaskContext::Value value = mpi_transport::recv_value(0, 100);

            if (auto* vec = std::get_if<std::vector<int>>(&value)) {
                std::cout << "[rank 1] received vector<int>: ";
                for (int x : *vec) {
                    std::cout << x << " ";
                }
                std::cout << "\n";
            } else {
                std::cout << "[rank 1] wrong type received\n";
            }
        }
    } catch (const std::exception& e) {
        std::cout << "[rank " << rank << "] error: " << e.what() << "\n";
    }

    MPI_Finalize();
    return 0;
}