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
#ifndef INCLUDED_GHEX_TL_MPI_COMMUNICATOR_STATE_HPP
#define INCLUDED_GHEX_TL_MPI_COMMUNICATOR_STATE_HPP

#include <vector>
#include <atomic>
#include "./error.hpp"
#include "./future.hpp"
#include "../callback_utils.hpp"

namespace gridtools {

    namespace ghex {

        namespace tl {

            namespace mpi {

                /** @brief common data which is shared by all communicators. This class is thread safe.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                struct shared_communicator_state {
                    using thread_primitives_type = ThreadPrimitives;
                    using transport_context_type = transport_context<mpi_tag, ThreadPrimitives>;
                    using rank_type = int;
                    using tag_type = int;

                    MPI_Comm m_comm;
                    transport_context_type* m_context;
                    thread_primitives_type* m_thread_primitives;
                    rank_type m_rank;
                    rank_type m_size;
                    MPI_Win m_win;
                    MPI_Group m_group;
                    MPI_Group m_access_group;
                    MPI_Group m_exposure_group;
                    std::vector<rank_type> m_access_ranks;
                    std::vector<rank_type> m_exposure_ranks;
                    volatile bool m_epoch = false;
                    //std::atomic<int> m_counter;

                    shared_communicator_state(MPI_Comm comm, transport_context_type* tc, thread_primitives_type* tp)
                    : m_comm{comm}
                    , m_context{tc}
                    , m_thread_primitives{tp}
                    , m_rank{ [](MPI_Comm c){ int r; GHEX_CHECK_MPI_RESULT(MPI_Comm_rank(c,&r)); return r; }(comm) }
                    , m_size{ [](MPI_Comm c){ int s; GHEX_CHECK_MPI_RESULT(MPI_Comm_size(c,&s)); return s; }(comm) }
                    //, m_counter{0}
                    {
                        GHEX_CHECK_MPI_RESULT(MPI_Win_create_dynamic(MPI_INFO_NULL, m_comm, &m_win));
                        GHEX_CHECK_MPI_RESULT(MPI_Win_get_group(m_win, &m_group));
                        GHEX_CHECK_MPI_RESULT(MPI_Win_get_group(m_win, &m_access_group));
                        GHEX_CHECK_MPI_RESULT(MPI_Win_get_group(m_win, &m_exposure_group));
                        /*m_access_ranks.push_back(m_rank);
                        m_exposure_ranks.push_back(m_rank);
                        GHEX_CHECK_MPI_RESULT(MPI_Group_incl(m_group, m_access_ranks.size(), m_access_ranks.data(), &(m_access_group)));
                        GHEX_CHECK_MPI_RESULT(MPI_Group_incl(m_group, m_exposure_ranks.size(), m_exposure_ranks.data(), &(m_exposure_group)));
                        m_access_ranks.clear();
                        m_exposure_ranks.clear();*/
                    }

                    ~shared_communicator_state()
                    {
                        MPI_Win_free(&m_win);
                        MPI_Group_free(&m_group);
                        MPI_Group_free(&m_access_group);
                        MPI_Group_free(&m_exposure_group);
                    }

                    rank_type rank() const noexcept { return m_rank; }
                    rank_type size() const noexcept { return m_size; }
                };

                /** @brief communicator per-thread data.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                struct communicator_state {
                    using shared_state_type = shared_communicator_state<ThreadPrimitives>;
                    using thread_primitives_type = ThreadPrimitives;
                    using thread_token = typename thread_primitives_type::token;
                    using rank_type = typename shared_state_type::rank_type;
                    using tag_type = typename shared_state_type::tag_type;
                    template<typename T>
                    using future = future_t<T>;
                    using queue_type = ::gridtools::ghex::tl::cb::callback_queue<future<void>, rank_type, tag_type>;
                    using progress_status = gridtools::ghex::tl::cb::progress_status;

                    thread_token* m_token_ptr;
                    queue_type m_send_queue;
                    queue_type m_recv_queue;
                    int  m_progressed_sends = 0;
                    int  m_progressed_recvs = 0;

                    communicator_state(thread_token* t)
                    : m_token_ptr{t}
                    {}

                    progress_status progress() {
                        m_progressed_sends += m_send_queue.progress();
                        m_progressed_recvs += m_recv_queue.progress();
                        return {
                            std::exchange(m_progressed_sends,0),
                            std::exchange(m_progressed_recvs,0),
                            std::exchange(m_recv_queue.m_progressed_cancels,0)};
                    }
                };

            } // namespace mpi

        } // namespace tl

    } // namespace ghex

} //namespace gridtools

#endif /* INCLUDED_GHEX_TL_MPI_COMMUNICATOR_STATE_HPP */
