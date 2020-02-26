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
#include <ghex/threads/none/primitives.hpp>
#include <iostream>
#include <iomanip>

#include <gtest/gtest.h>

#include <ghex/transport_layer/mpi/context.hpp>
#include <ghex/transport_layer/mpi/bulk_exchange.hpp>
using transport = gridtools::ghex::tl::mpi_tag;

using threading_std = gridtools::ghex::threads::std_thread::primitives;
using threading_atc = gridtools::ghex::threads::atomic::primitives;
using threading_non = gridtools::ghex::threads::none::primitives;
using threading = threading_atc;

//const std::size_t size = 10*1024;

TEST(rma_mpi, window_active_group) {
    
    const int num_threads = 3;
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(num_threads, MPI_COMM_WORLD);
    auto& context = *context_ptr;


    auto func = [&context]() {
        auto token = context.get_token();
        auto comm  = context.get_communicator(token);

        const auto rank = comm.rank();
        const auto size = comm.size();
        const auto left_rank = ((rank+size)-1) % size;
        const auto right_rank = (rank+1) % size;
        const auto tag = token.id();

        const auto buffer_size = 1;
        auto send_msg = comm.make_message<>(buffer_size);
        auto recv_msg = comm.make_message<>(buffer_size);

        auto bulk_ex = gridtools::ghex::tl::mpi::bulk_exchange<threading>(comm);
        bulk_ex.sync();

        for (int i=0; i<10; ++i) {
            bulk_ex.start_epoch();
            bulk_ex.send();
            bulk_ex.end_epoch();
        }

        auto h = bulk_ex.register_send(send_msg, right_rank, tag);
        bulk_ex.register_recv(recv_msg, left_rank, tag);
        bulk_ex.sync();

        for (int i=0; i<10; ++i) {
            bulk_ex.start_epoch();
            bulk_ex.send(h);
            bulk_ex.end_epoch();
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i=0; i<num_threads; ++i)
        threads.push_back(std::thread{func});

    for (auto& t : threads)
        t.join();
}
