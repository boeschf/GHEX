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
#ifndef INCLUDED_GHEX_TL_LIBFABRIC_CONTEXT_HPP
#define INCLUDED_GHEX_TL_LIBFABRIC_CONTEXT_HPP

#include "ghex/transport_layer/communicator.hpp"
#include "ghex/transport_layer/libfabric/communicator.hpp"
#include "ghex/transport_layer/libfabric/controller.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace libfabric {
            // cppcheck-suppress ConfigurationNotChecked
            static hpx::debug::enable_print<false> ctx_deb("CONTEXT");

            struct transport_context
            {
                using tag                    = libfabric_tag;
                using communicator_type      = tl::communicator<libfabric::communicator>;
                using shared_state_type      = typename communicator_type::shared_state_type;
                using state_type             = typename communicator_type::state_type;
                using state_ptr              = std::unique_ptr<state_type>;
                using state_vector           = std::vector<state_ptr>;
                using controller_type        = ghex::tl::libfabric::controller;

                const mpi::rank_topology& m_rank_topology;
                MPI_Comm                  m_comm;
                int                       m_rank;
                int                       m_size;
                controller_type          *m_controller;
                state_vector              m_states;
                shared_state_type         m_shared_state;
                state_type                m_state;
                std::mutex                m_mutex;

                // --------------------------------------------------
                // create a singleton shared_ptr to a libfabric controller that
                // can be shared between ghex context objects
                static controller_type *init_libfabric_controller(MPI_Comm comm, int rank, int size)
                {
                    static std::mutex m_init_mutex;
                    std::lock_guard<std::mutex> lock(m_init_mutex);
                    static std::unique_ptr<controller_type> instance(new controller_type(
                            GHEX_LIBFABRIC_PROVIDER,
                            GHEX_LIBFABRIC_DOMAIN,
                            comm, rank, size
                        ));
                    instance->startup();
                    return instance.get();
                }

                // --------------------------------------------------
                transport_context(const mpi::rank_topology& t)
                  : m_rank_topology{t}
                  , m_states()
                  , m_comm{t.mpi_comm()}
                  , m_rank{ [](MPI_Comm c){ int r; GHEX_CHECK_MPI_RESULT(MPI_Comm_rank(c,&r)); return r; }(m_comm) }
                  , m_size{ [](MPI_Comm c){ int s; GHEX_CHECK_MPI_RESULT(MPI_Comm_size(c,&s)); return s; }(m_comm) }
                  , m_controller(init_libfabric_controller(m_comm, m_rank, m_size))
                  , m_shared_state{m_rank_topology, m_controller}
                  , m_state{m_controller->ep_active_, m_controller->fabric_domain_}
                {
                }

                communicator_type get_serial_communicator()
                {
                    return {&m_shared_state, &m_state};
                }

                communicator_type get_communicator()
                {
                    std::lock_guard<std::mutex> lock(m_mutex); // we need to guard only the insertion in the vector,
                                                               // but this is not a performance critical section
                    m_states.push_back(std::make_unique<state_type>(m_controller->ep_active_, m_controller->fabric_domain_));
                    return {&m_shared_state, m_states[m_states.size()-1].get()};
                }

                controller_type *get_libfabric_controller()
                {
                    return m_controller;
                }
            };

        } // namespace libfabric

        template<>
        struct context_factory<libfabric_tag>
        {
            using context_type = context<libfabric::transport_context>;
            static std::unique_ptr<context_type> create(MPI_Comm mpi_comm)
            {
                auto new_comm = detail::clone_mpi_comm(mpi_comm);
                return std::unique_ptr<context_type>{
                    // pass comm twice as the context constructor needs it
                    // and passes it on to the libfabric::transport_context
                    // constructor as another arg
                    new context_type{new_comm}};
            }
        };

        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_LIBFABRIC_CONTEXT_HPP */
