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
#ifndef INCLUDED_GHEX_TL_MPI_BULK_EXCHANGE_HPP
#define INCLUDED_GHEX_TL_MPI_BULK_EXCHANGE_HPP

#include <vector>
#include <memory>
#include <set>
#include "./communicator.hpp"
#include "../communicator.hpp"

namespace gridtools {

    namespace ghex {

        namespace tl {

            namespace mpi {
                
                template<typename ThreadPrimitives>
                class bulk_exchange {
                public:
                    using communicator_type = ::gridtools::ghex::tl::communicator<::gridtools::ghex::tl::mpi::communicator<ThreadPrimitives>>;
                    using thread_primitives_type = ThreadPrimitives;
                    using thread_token = typename thread_primitives_type::token;
                    using rank_type = typename communicator_type::rank_type;
                    using tag_type = typename communicator_type::tag_type;
                    template<typename T>
                    using future = typename communicator_type::template future<T>;
                    using request = typename communicator_type::request;
                    
                    struct bulk_send_handle {
                        std::size_t m_index = 0u;
                        bulk_send_handle() noexcept = default;
                        bulk_send_handle(std::size_t i) noexcept : m_index{i} {}
                    };

                private:
                    struct put_send_t {
                        const void* m_buffer;
                        std::size_t m_size;
                        rank_type m_rank;
                        future<void> m_future;
                        std::unique_ptr<MPI_Aint> m_recv_address;
                        bool m_synced = false;

                        put_send_t(const void* p, std::size_t s, rank_type r) noexcept
                        : m_buffer{p}
                        , m_size{s}
                        , m_rank{r}
                        , m_recv_address{ std::make_unique<MPI_Aint>(0) }
                        {}
                    };
                    struct put_recv_t {
                        void* m_buffer;
                        future<void> m_future;
                        std::unique_ptr<MPI_Aint> m_recv_address;
                        bool m_synced = false;

                        put_recv_t(void* p) noexcept
                        : m_buffer{p}
                        , m_recv_address{ std::make_unique<MPI_Aint>(0) }
                        {}
                    };

                private:
                    communicator_type m_comm;
                    thread_primitives_type* m_tp;
                    thread_token m_token;
                    window_t* m_window;
                    std::size_t m_index;
                    std::vector<put_send_t> m_put_sends;
                    std::vector<put_recv_t> m_put_recvs;

                public:
                    bulk_exchange(communicator_type comm)
                    : m_comm{comm}
                    , m_tp{m_comm.m_shared_state->m_thread_primitives}
                    , m_token{*(m_comm.m_state->m_token_ptr)} 
                    {
                        m_tp->barrier(m_token);
                        m_tp->single(m_token, [this]() {
                            m_comm.m_shared_state->m_windows.push_back(std::make_unique<window_t>(m_comm.m_shared_state->m_comm));
                            m_window = m_comm.m_shared_state->m_windows.back().get();
                            GHEX_CHECK_MPI_RESULT(MPI_Win_create_dynamic(MPI_INFO_NULL, m_window->m_comm, &(m_window->m_win)));
                            GHEX_CHECK_MPI_RESULT(MPI_Win_get_group(m_window->m_win, &(m_window->m_group)));
                            auto r = m_comm.rank();
                            GHEX_CHECK_MPI_RESULT(MPI_Group_incl(m_window->m_group, 1, &r, &(m_window->m_access_group)));
                            GHEX_CHECK_MPI_RESULT(MPI_Group_incl(m_window->m_group, 1, &r, &(m_window->m_exposure_group)));
                        });
                        m_tp->barrier(m_token);
                        m_window = m_comm.m_shared_state->m_windows.back().get();
                        m_index = m_comm.m_shared_state->m_windows.size()-1;
                        m_tp->barrier(m_token);
                    }

                    bulk_exchange(const bulk_exchange&) = delete;
                    bulk_exchange(bulk_exchange&&) = default;

                    ~bulk_exchange() {
                        for (auto& item : m_put_recvs)
                            MPI_Win_detach(m_window->m_win, item.m_buffer);
                    }

                public:
                    template<typename Message>
                    bulk_send_handle register_send(const Message& msg, rank_type dst, tag_type tag) {
                        using value_type = typename Message::value_type;
                        m_tp->critical([this, dst]() { m_window->m_access_ranks.insert(dst); });
                        
                        m_put_sends.emplace_back( msg.data(), msg.size()*sizeof(value_type), dst );
                        auto& item = m_put_sends.back();
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Irecv(item.m_recv_address.get(), 1, MPI_AINT,
                                                        dst, tag, m_comm.m_shared_state->m_comm, &req.get()));
                        req.m_kind = request_kind::recv;
                        item.m_future = std::move(req);
                        return { m_put_sends.size() - 1u };
                    }

                    template<typename Message>
                    void register_recv(Message& msg, rank_type src, tag_type tag) {
                        using value_type = typename Message::value_type;
                        GHEX_CHECK_MPI_RESULT(MPI_Win_attach(m_window->m_win, msg.data(), msg.size()*sizeof(value_type)));
                        m_tp->critical([this, src]() { m_window->m_exposure_ranks.insert(src); });

                        m_put_recvs.emplace_back(msg.data());
                        auto& item = m_put_recvs.back();
                        GHEX_CHECK_MPI_RESULT(MPI_Get_address(msg.data(), item.m_recv_address.get()));
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Isend(item.m_recv_address.get(), 1, MPI_AINT,
                                                        src, tag, m_window->m_comm, &req.get()));
                        req.m_kind = request_kind::send;
                        item.m_future = std::move(req);
                        //return { m_put_recvs.size() - 1u };
                    }
                
                    void sync() {
                        for (auto& item : m_put_sends)
                            if (!item.m_synced) {
                                item.m_future.wait();
                                item.m_synced = true;
                            }

                        for (auto& item : m_put_recvs)
                            if (!item.m_synced) {
                                item.m_future.wait();
                                item.m_synced = true;
                            }
                        //m_tp->barrier(m_token)
                        m_comm.barrier();
                        m_tp->single(m_token, [this]() {
                            if (m_window->m_access_ranks.size())
                            {
                                GHEX_CHECK_MPI_RESULT(MPI_Group_free(&(m_window->m_access_group)));
                                std::vector<rank_type> access_ranks(m_window->m_access_ranks.begin(), m_window->m_access_ranks.end());
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Group_incl(m_window->m_group, access_ranks.size(), access_ranks.data(), &(m_window->m_access_group)));
                            }
                            if (m_window->m_exposure_ranks.size())
                            {
                                GHEX_CHECK_MPI_RESULT(MPI_Group_free(&(m_window->m_exposure_group)));
                                std::vector<rank_type> exposure_ranks(m_window->m_exposure_ranks.begin(), m_window->m_exposure_ranks.end());
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Group_incl(m_window->m_group, exposure_ranks.size(), exposure_ranks.data(), &(m_window->m_exposure_group)));
                            }
                        }); 
                        //m_tp->barrier(token);
                        m_comm.barrier();
                    }

                    void start_epoch() {
                        m_tp->single(m_token, [this]() {
                            GHEX_CHECK_MPI_RESULT(MPI_Win_post(m_window->m_exposure_group, 0, m_window->m_win));
                            GHEX_CHECK_MPI_RESULT(MPI_Win_start(m_window->m_access_group, 0, m_window->m_win));
                            m_window->m_epoch = true;
                        });
                    }

                    void end_epoch() {
                        m_tp->barrier(m_token);
                        m_tp->single(m_token, [this]() {
                            GHEX_CHECK_MPI_RESULT(MPI_Win_complete(m_window->m_win));
                            GHEX_CHECK_MPI_RESULT(MPI_Win_wait(m_window->m_win));
                            m_window->m_epoch = false;
                        });
                        m_tp->barrier(m_token);
                    }

                    void send() { for (std::size_t i = 0u; i<m_put_sends.size(); ++i) send(i); }

                    void send(bulk_send_handle h) { send(h.m_index); }

                    void send(std::size_t i) {
                        while (!m_window->m_epoch) {}
                        auto& item = m_put_sends[i];
                        GHEX_CHECK_MPI_RESULT(MPI_Put(item.m_buffer, item.m_size, MPI_BYTE, item.m_rank, 
                                                      *(item.m_recv_address), item.m_size, MPI_BYTE, m_window->m_win));
                    }
                };

            } // namespace mpi

        } // namespace tl

    } // namespace ghex

} //namespace gridtools

#endif /* INCLUDED_GHEX_TL_MPI_BULK_EXCHANGE_HPP */

