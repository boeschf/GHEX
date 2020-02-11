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
//using threading = gridtools::ghex::threads::atomic::primitives;
#else
using transport = gridtools::ghex::tl::ucx_tag;
using threading = gridtools::ghex::threads::std_thread::primitives;
//using threading = gridtools::ghex::threads::atomic::primitives;
#endif
//using context_type = gridtools::ghex::tl::context<transport, threading>;



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
            if (y<0 || y>=b || x<0 || x>=b)
                field[location] = T(-1);
            else
                field[location] = offset++;
        }
}


TEST(simple_domain, exchange)
{
    auto context_ptr = ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
    auto& context = *context_ptr;

    using T = double;
    const int s = 10;
    const int b = 1;
    const int num_cells = (s+2*b)*(s+2*b);
    const int mem_size = num_cells*sizeof(T);
    const std::array<int, 2> offset = {b,b};
    const std::array<int, 2> extent = {s+2*b, s+2*b};
    const int rank       = context.rank();
    const int left_rank  = (context.rank()+context.size()-1)%context.size();
    const int right_rank = (context.rank()+1)%context.size();

    // make one field on the cpu
    std::vector<T> raw_field_cpu(num_cells);
    fill_field(s, b, rank*s*s, raw_field_cpu);

    // halos
    const std::array<bool, 4> halos{1,1,1,1};
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
    auto co = ghex::make_communication_object<pattern_type>(context.get_communicator(context.get_token()));

#ifdef __CUDACC__
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


}
