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

#include <ghex/structured/pattern.hpp>
#include <ghex/communication_object_2.hpp>
#ifndef GHEX_TEST_USE_UCX
#include <ghex/transport_layer/mpi/context.hpp>
#else
#include <ghex/transport_layer/ucx/context.hpp>
#endif
#include <ghex/threads/atomic/primitives.hpp>
#include <ghex/threads/std_thread/primitives.hpp>
#include <array>
#include <iostream>
#include <iomanip>
#include <thread>
#include <future>
#include <gtest/gtest.h>
#include <gridtools/common/array.hpp>
#ifdef __CUDACC__
#include <gridtools/common/cuda_util.hpp>
#include <gridtools/common/host_device.hpp>
// stupid kernel to test whether cuda is working
#include <stdio.h>
__global__ void print_kernel() {
    printf("Hello from block %d, thread %d\n", blockIdx.x, threadIdx.x);
}
#endif

#ifndef GHEX_TEST_USE_UCX
using transport = gridtools::ghex::tl::mpi_tag;
using threading = gridtools::ghex::threads::std_thread::primitives;
#else
using transport = gridtools::ghex::tl::ucx_tag;
using threading = gridtools::ghex::threads::std_thread::primitives;
#endif

// domain setup:
// - 2-dimensional
// - s x s size blocks
// - concatenated along the x-direction
// - each rank owns one block
// - periodic in x and y direction
// - buffer size b on all sides
//
// +---+---+
// | 0 | 1 | ...
// +---+---+

namespace ghex = gridtools::ghex;

template<typename T, class Container>
void fill_field(int s, int b, T offset, Container& field)
{
    for (int y=-b; y<s+b; ++y)
        for (int x=-b; x<s+b; ++x)
        {
            const std::size_t location = (std::size_t)(y+b)*(std::size_t)(s+2*b) + (std::size_t)(x+b);
            if (y<0 || y>=s || x<0 || x>=s)
                field[location] = T(-1);
            else
                field[location] = offset++;
        }
}

template<typename T, class Container>
void check_field(int s, int b, T offset_left, T offset, T offset_right, Container& field)
{
    for (int y=-b; y<s+b; ++y)
    {
        for (int x=-b; x<0; ++x)
        {
            const std::size_t location = (std::size_t)(y+b)*(std::size_t)(s+2*b) + (std::size_t)(x+b);
            const std::size_t x_left = s+x;
            const std::size_t y_left = (y<0 ? s+y : (y>=s ? y-s : y));
            EXPECT_EQ( field[location], y_left*s + x_left + offset_left );
        }
        for (int x=0; x<s; ++x)
        {
            const std::size_t location = (std::size_t)(y+b)*(std::size_t)(s+2*b) + (std::size_t)(x+b);
            const std::size_t y_middle = (y<0 ? s+y : (y>=s ? y-s : y));
            EXPECT_EQ( field[location], y_middle*s + x + offset );
        }
        for (int x=s; x<s+b; ++x)
        {
            const std::size_t location = (std::size_t)(y+b)*(std::size_t)(s+2*b) + (std::size_t)(x+b);
            const std::size_t x_right = x-s;
            const std::size_t y_right = (y<0 ? s+y : (y>=s ? y-s : y));
            EXPECT_EQ( field[location], y_right*s + x_right + offset_right );
        }
    }
}

template<class Container>
void print_field(int s, int b, Container& field)
{
    for (int x=-b; x<s+b; ++x)
    {
        if (x==0 || x==s)
        {
            for (int y=-b; y<s+b; ++y)
            {
                if (y==0 || y==s) 
                    std::cout << "+";
                std::cout << "----";
            }
            std::cout << "\n";
        }
        for (int y=-b; y<s+b; ++y)
        {
            if (y==0 || y==s) std::cout << "|";
            const std::size_t location = (std::size_t)(y+b)*(std::size_t)(s+2*b) + (std::size_t)(x+b);
            std::cout << std::setw(4) << field[location];
        }
        std::cout << "\n";
    }
    std::cout << std::endl;
}

TEST(simple_domain, exchange)
{
    auto context_ptr = ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;

    using T = double;
    const int s = 10;
    const int b = 2;
    const int num_cells = (s+2*b)*(s+2*b);
    const std::array<int, 2> offset = {b,b};
    const std::array<int, 2> extent = {s+2*b, s+2*b};
    const int rank       = context.rank();
    const int left_rank  = (context.rank()+context.size()-1)%context.size();
    const int right_rank = (context.rank()+1)%context.size();

    // make one field on the cpu
    std::vector<T> raw_field_cpu(num_cells);
    fill_field(s, b, rank*s*s, raw_field_cpu);
    //print_field(s, b, raw_field_cpu);

    // halos
    const std::array<int, 4> halos{b,b,b,b};
    // periodicity
    const std::array<bool, 2> periodicity{true,true};
    // total domain
    const std::array<int, 2> g_first = {0,0};
    const std::array<int, 2> g_last  = {s*context.size()-1, s-1};
    // local domain
    const std::array<int, 2> l_first = {s*context.rank(), 0};
    const std::array<int, 2> l_last  = {s*context.rank()+s-1, s-1};

    using domain_descriptor_type = ghex::structured::domain_descriptor<int,2>;

    auto pattern = ghex::make_pattern<ghex::structured::grid>(
        context,
        domain_descriptor_type::halo_generator_type(g_first, g_last, halos, periodicity),
        std::vector<domain_descriptor_type>{ domain_descriptor_type{ context.rank(), l_first, l_last } });
    
    using pattern_type = decltype(pattern);
    auto comm = context.get_communicator(context.get_token());
    auto co = ghex::make_communication_object<pattern_type>(comm);

#ifdef __CUDACC__
    const int mem_size = num_cells*sizeof(T);
    // allocate memory on gpu
    T* gpu_ptr;
    GT_CUDA_CHECK(cudaMalloc((void**)&gpu_ptr, mem_size);
    // transfer memory to the gpu
    GT_CUDA_CHECK(cudaMemcpy(gpu_ptr, raw_field_cpu.data(), mem_size, cudaMemcpyHostToDevice));
    // wrap in ghex field
    auto field = ghex::wrap_field<ghex::gpu, 1, 0>(context.rank(), gpu_ptr, offset, extent);
#else
    // wrap in ghex field
    auto field = ghex::wrap_field<ghex::cpu, 1, 0>(context.rank(), raw_field_cpu.data(), offset, extent);
#endif

    co.exchange( pattern(field) ).wait();
    
#ifdef __CUDACC__
    // transfer memory back to host
    GT_CUDA_CHECK(cudaMemcpy(raw_field_cpu.data(), gpu_ptr, mem_size, cudaMemcpyDeviceToHost));
    // free cuda memory
    cudaFree(gpu_ptr);
#endif

    check_field(s, b, left_rank*s*s, rank*s*s, right_rank*s*s, raw_field_cpu);
    
    for (int r=0; r<context.size(); ++r)
    {
        comm.barrier();
        if (comm.rank() == r)
            print_field(s, b, raw_field_cpu);
    }
    comm.barrier();

}

TEST(simple_domain, manual_exchange)
{
    auto context_ptr = ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;

    using T = double;
    const int s = 10;
    const int b = 2;
    const int num_cells = (s+2*b)*(s+2*b);
    const std::array<int, 2> offset = {b,b};
    const std::array<int, 2> extent = {s+2*b, s+2*b};
    const int rank       = context.rank();
    const int left_rank  = (context.rank()+context.size()-1)%context.size();
    const int right_rank = (context.rank()+1)%context.size();

    // make one field on the cpu
    std::vector<T> raw_field_cpu(num_cells);
    fill_field(s, b, rank*s*s, raw_field_cpu);
    //print_field(s, b, raw_field_cpu);

    auto comm = context.get_communicator(context.get_token());

#ifdef __CUDACC__
    const int mem_size = num_cells*sizeof(T);
    // allocate memory on gpu
    T* gpu_ptr;
    GT_CUDA_CHECK(cudaMalloc((void**)&gpu_ptr, mem_size);
    // transfer memory to the gpu
    GT_CUDA_CHECK(cudaMemcpy(gpu_ptr, raw_field_cpu.data(), mem_size, cudaMemcpyHostToDevice));
    using msg_buffer = ghex::tl::message_buffer<gridtools::ghex::allocator::cuda::allocator<unsigned char>>;
#else
    using msg_buffer = ghex::tl::message_buffer<std::allocator<unsigned char>>;
#endif
    // make buffers for sending
    msg_buffer left_top_send(b*b*sizeof(T));
    msg_buffer right_top_send(b*b*sizeof(T));
    msg_buffer left_bottom_send(b*b*sizeof(T));
    msg_buffer right_bottom_send(b*b*sizeof(T));
    msg_buffer left_send(b*s*sizeof(T));
    msg_buffer right_send(b*s*sizeof(T));
    msg_buffer top_send(b*s*sizeof(T));
    msg_buffer bottom_send(b*s*sizeof(T));
    // make buffers for receiving
    msg_buffer left_top_recv(b*b*sizeof(T));
    msg_buffer right_top_recv(b*b*sizeof(T));
    msg_buffer left_bottom_recv(b*b*sizeof(T));
    msg_buffer right_bottom_recv(b*b*sizeof(T));
    msg_buffer left_recv(b*s*sizeof(T));
    msg_buffer right_recv(b*s*sizeof(T));
    msg_buffer top_recv(b*s*sizeof(T));
    msg_buffer bottom_recv(b*s*sizeof(T));

    auto fut_01 = comm.recv(left_top_recv, left_rank, 0);
    auto fut_02 = comm.send(right_bottom_send, right_rank, 0);

    auto fut_03 = comm.recv(left_bottom_recv, left_rank, 1);
    auto fut_04 = comm.send(right_top_send, right_rank, 1);

    auto fut_05 = comm.recv(left_recv, left_rank, 2);
    auto fut_06 = comm.send(right_send, right_rank, 2);

    auto fut_07 = comm.recv(top_recv, rank, 3);
    auto fut_08 = comm.send(bottom_send, rank, 3);

    auto fut_09 = comm.recv(bottom_recv, rank, 4);
    auto fut_10 = comm.send(top_send, rank, 4);

    auto fut_11 = comm.recv(right_top_recv, right_rank, 5);
    auto fut_12 = comm.send(left_bottom_send, left_rank, 5);

    auto fut_13 = comm.recv(right_bottom_recv, right_rank, 6);
    auto fut_14 = comm.send(left_top_send, left_rank, 6);

    auto fut_15 = comm.recv(right_recv, right_rank, 7);
    auto fut_16 = comm.send(left_send, left_rank, 7);

    fut_01.wait();
    fut_02.wait();
    fut_03.wait();
    fut_04.wait();
    fut_05.wait();
    fut_06.wait();
    fut_07.wait();
    fut_08.wait();
    fut_09.wait();
    fut_10.wait();
    fut_11.wait();
    fut_12.wait();
    fut_13.wait();
    fut_14.wait();
    fut_15.wait();
    fut_16.wait();
    
#ifdef __CUDACC__
    // transfer memory back to host
    GT_CUDA_CHECK(cudaMemcpy(raw_field_cpu.data(), gpu_ptr, mem_size, cudaMemcpyDeviceToHost));
    // free cuda memory
    cudaFree(gpu_ptr);
#endif

    /*check_field(s, b, left_rank*s*s, rank*s*s, right_rank*s*s, raw_field_cpu);
    
    for (int r=0; r<context.size(); ++r)
    {
        comm.barrier();
        if (comm.rank() == r)
            print_field(s, b, raw_field_cpu);
    }
    comm.barrier();*/
}
