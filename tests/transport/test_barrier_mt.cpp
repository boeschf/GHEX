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
#include <ghex/threads/std_thread/primitives.hpp>
#include <ghex/threads/atomic/primitives.hpp>
#ifdef _OPENMP
#include <ghex/threads/omp/primitives.hpp>
#endif
#include <iostream>
#include <iomanip>
#include <atomic>
#include <gtest/gtest.h>

#ifdef GHEX_TEST_USE_UCX
#include <ghex/transport_layer/ucx/context.hpp>
using transport = gridtools::ghex::tl::ucx_tag;
#elif GHEX_TEST_USE_LIBFABRIC
#include <ghex/transport_layer/libfabric/context.hpp>
using transport = gridtools::ghex::tl::libfabric_tag;
#else
#include <ghex/transport_layer/mpi/context.hpp>
using transport = gridtools::ghex::tl::mpi_tag;
#endif

using threading = gridtools::ghex::threads::std_thread::primitives;
using threading2 = gridtools::ghex::threads::atomic::primitives;
#ifdef _OPENMP
using threading3 = gridtools::ghex::threads::omp::primitives;
#endif

std::atomic<int> barrier_count{0};

void prepare_test() { barrier_count = 0; }

template <typename Context>
auto thread_func(Context&& context, int nthreads) {
    return [&context, nthreads]() {
        auto token = context.get_token();
        auto comm = context.get_communicator(token);

        for(int i=0; i<10; i++)  {
            barrier_count++;
            comm.barrier();
            EXPECT_EQ(barrier_count, (i+1)*nthreads);
            comm.barrier();
        }
    };
}


TEST(transport, barrier_mt_std) {
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(4, MPI_COMM_WORLD);
    auto& context = *context_ptr;

    std::vector<std::thread> threads;
    threads.reserve(4);
    prepare_test();
    for (int i=0; i<4; ++i)
        threads.push_back(std::thread{thread_func(context, 4)});
    for (auto& t : threads)
        t.join();
}

TEST(transport, barrier_mt_std_atomic) {
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading2>::create(4, MPI_COMM_WORLD);
    auto& context = *context_ptr;

    std::vector<std::thread> threads;
    threads.reserve(4);
    prepare_test();
    for (int i=0; i<4; ++i)
        threads.push_back(std::thread{thread_func(context, 4)});
    for (auto& t : threads)
        t.join();
}

#ifdef _OPENMP
TEST(transport, barrier_mt_omp) {
    int num_threads = 1;
    omp_set_num_threads(4);
    prepare_test();
#pragma omp parallel
#pragma omp master
    num_threads = omp_get_num_threads();

    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading3>::create(num_threads, MPI_COMM_WORLD);
    auto& context = *context_ptr;
#pragma omp parallel
    {
        thread_func(context, num_threads)();
    }
}
#else
TEST(transport, DISABLED_barrier_mt_omp) {}
#endif

#ifdef _OPENMP
TEST(transport, barrier_mt_omp_atomic) {
    int num_threads = 1;
    omp_set_num_threads(4);
    prepare_test();
#pragma omp parallel
#pragma omp master
    num_threads = omp_get_num_threads();

    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading2>::create(num_threads, MPI_COMM_WORLD);
    auto& context = *context_ptr;
#pragma omp parallel
    {
        thread_func(context, num_threads)();
    }
}
#else
TEST(transport, DISABLED_barrier_mt_omp_atomic) {}
#endif
