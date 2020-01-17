/*
 * GridTools
 *
 * Copyright (c) 2014-2019, ETH Zurich
 * All rights reserved.
 *
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 */
#ifndef INCLUDED_TL_MPI_CONTEXT_HPP
#define INCLUDED_TL_MPI_CONTEXT_HPP

#include "../context.hpp"
#include "./communicator.hpp"

namespace gridtools {

namespace ghex {

namespace tl {

template<typename ThreadPrimitives>
struct transport_context<mpi_tag, ThreadPrimitives> {
    using communicator_type = mpi::communicator<ThreadPrimitives>;
    using parallel_context_type = parallel_context<ThreadPrimitives>;
    using thread_token = typename parallel_context_type::thread_token;

    parallel_context<ThreadPrimitives>& m_parallel_context;

    template<typename ... Args>
    transport_context(parallel_context<ThreadPrimitives>& pc, Args&&...)
        : m_parallel_context(pc)
    {}

    communicator_type get_serial_communicator(){
        return { (MPI_Comm)(m_parallel_context.world()), this };
    }

    communicator_type get_communicator(const thread_token& t){
        return { (MPI_Comm)(m_parallel_context.world()), this, t.id() };
    }

};

template<class ThreadPrimitives>
struct context_factory<mpi_tag, ThreadPrimitives> {
    static std::unique_ptr<context<mpi_tag, ThreadPrimitives>> create(int num_threads, MPI_Comm mpi_comm){
        return std::make_unique<context<mpi_tag, ThreadPrimitives>>(num_threads, mpi_comm);
    }
};

}

}

}

#endif /* INCLUDED_TL_MPI_CONTEXT_HPP */

