/* 
 * GridTools
 * 
 * Copyright (c) 2014-2020, ETH Zurich
 * All rights reserved.
 * 
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
#include <ghex/threads/none/primitives.hpp>
#include <ghex/allocator/cuda_allocator.hpp>
#include <vector>
#include <iomanip>
#include <utility>

#include <gtest/gtest.h>


#ifdef GHEX_TEST_USE_UCX
#include <ghex/transport_layer/ucx/context.hpp>
using transport = gridtools::ghex::tl::ucx_tag;
#else
#include <ghex/transport_layer/mpi/context.hpp>
using transport = gridtools::ghex::tl::mpi_tag;
#endif

using threading = gridtools::ghex::threads::none::primitives;
using context_type = gridtools::ghex::tl::context<transport, threading>;

using cpu_msg_buffer = gridtools::ghex::tl::message_buffer<std::allocator<unsigned char>>;
using gpu_msg_buffer = gridtools::ghex::tl::message_buffer<gridtools::ghex::allocator::cuda::allocator<unsigned char>>;

#define SIZE 40

int rank;

template <typename M>
void init_msg(M& msg) {
    int* data = msg.template data<int>();
    for (size_t i = 0; i < msg.size()/sizeof(int); ++i) {
        data[i] = static_cast<int>(i);
    }
}

void init_msg(std::vector<unsigned char>& msg) {
    int c = 0;
    for (size_t i = 0; i < msg.size(); i += 4) {
        *(reinterpret_cast<int*>(&msg[i])) = c++;
    }
}

template <typename M>
bool check_msg(M const& msg) {
    bool ok = true;
    if (rank > 1)
        return ok;

    const int* data = msg.template data<int>();
    for (size_t i = 0; i < msg.size()/sizeof(int); ++i) {
        if ( data[i] != static_cast<int>(i) )
            ok = false;
    }
    return ok;
}

bool check_msg(std::vector<unsigned char> const& msg) {
    bool ok = true;
    if (rank > 1)
        return ok;

    int c = 0;
    for (size_t i = 0; i < msg.size(); i += 4) {
        int value = *(reinterpret_cast<int const*>(&msg[i]));
        if ( value != c++ )
            ok = false;
    }
    return ok;
}

template<typename Context>
auto test_unidirectional_host_device(Context& context) {
    auto token = context.get_token();
    EXPECT_TRUE(token.id() == 0);

    auto comm = context.get_communicator(token);

    cpu_msg_buffer msg(SIZE);
    init_msg(msg);

    if ( rank == 0 ) {
        comm.send(msg, 1, 1).get();
    } else if (rank == 1) {
        gpu_msg_buffer rmsg(SIZE);
        auto fut = comm.recv(rmsg, 0, 1);
        while (!fut.ready()) { }
        GT_CUDA_CHECK(cudaMemcpy(msg.data(), rmsg.data(), SIZE, cudaMemcpyDeviceToHost));
    }
    return std::move(msg);
}

template<typename Context>
auto test_unidirectional_device_host(Context& context) {
    auto token = context.get_token();
    EXPECT_TRUE(token.id() == 0);

    auto comm = context.get_communicator(token);

    cpu_msg_buffer msg(SIZE);
    init_msg(msg);

    if ( rank == 0 ) {
        gpu_msg_buffer smsg(SIZE);
        GT_CUDA_CHECK(cudaMemcpy(smsg.data(), msg.data(), SIZE, cudaMemcpyHostToDevice));
        comm.send(smsg, 1, 1).get();
    } else if (rank == 1) {
        auto fut = comm.recv(msg, 0, 1);
        while (!fut.ready()) { }
    }
    return std::move(msg);
}

template<typename Context>
auto test_unidirectional_device_device(Context& context) {
    auto token = context.get_token();
    EXPECT_TRUE(token.id() == 0);

    auto comm = context.get_communicator(token);

    cpu_msg_buffer msg(SIZE);
    init_msg(msg);

    if ( rank == 0 ) {
        gpu_msg_buffer smsg(SIZE);
        GT_CUDA_CHECK(cudaMemcpy(smsg.data(), msg.data(), SIZE, cudaMemcpyHostToDevice));
        comm.send(smsg, 1, 1).get();
    } else if (rank == 1) {
        gpu_msg_buffer rmsg(SIZE);
        auto fut = comm.recv(rmsg, 0, 1);
        while (!fut.ready()) { }
        GT_CUDA_CHECK(cudaMemcpy(msg.data(), rmsg.data(), SIZE, cudaMemcpyDeviceToHost));
    }
    return std::move(msg);
}

TEST(low_level_cuda, basic_unidirectional_HD) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;
    EXPECT_TRUE( check_msg( basic_unidirectional_host_device(context) ) );
}

TEST(low_level_cuda, basic_unidirectional_DH) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;
    EXPECT_TRUE( check_msg( basic_unidirectional_device_host(context) ) );
}

TEST(low_level_cuda, basic_unidirectional_DD) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;
    EXPECT_TRUE( check_msg( basic_unidirectional_device_device(context) ) );
}
