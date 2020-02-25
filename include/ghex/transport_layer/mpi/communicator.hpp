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
#ifndef INCLUDED_GHEX_TL_MPI_COMMUNICATOR_HPP
#define INCLUDED_GHEX_TL_MPI_COMMUNICATOR_HPP

#include <boost/optional.hpp>
#include <atomic>
#include "../shared_message_buffer.hpp"
#include "../tags.hpp"
#include "./future.hpp"
#include "./request_cb.hpp"
#include "../context.hpp"
#include "./communicator_state.hpp"

namespace gridtools {
    
    namespace ghex {

        namespace tl {
            
            template<typename ThreadPrimitives>
            struct transport_context<mpi_tag, ThreadPrimitives>;

            namespace mpi {

                /** @brief A communicator for MPI point-to-point communication.
                  * This class is lightweight and copying/moving instances is safe and cheap.
                  * Communicators can be created through the context, and are thread-compatible.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                class communicator {
                  public: // member types
                    using thread_primitives_type = ThreadPrimitives;
                    using shared_state_type = shared_communicator_state<ThreadPrimitives>;
                    using transport_context_type = typename shared_state_type::transport_context_type;
                    using thread_token = typename thread_primitives_type::token;
                    using state_type = communicator_state<ThreadPrimitives>;
                    using rank_type = typename state_type::rank_type;
                    using tag_type = typename state_type::tag_type;
                    using request = request_t;
                    using status = status_t;
                    template<typename T>
                    using future = typename state_type::template future<T>;
                    using address_type    = rank_type;
                    using request_cb_type = request_cb<ThreadPrimitives>;
                    using message_type    = typename request_cb_type::message_type;
                    using progress_status = typename state_type::progress_status;

                  private: // members
                    shared_state_type* m_shared_state;
                    state_type* m_state;

                  public: // ctors
                    communicator(shared_state_type* shared_state, state_type* state)
                    : m_shared_state{shared_state}
                    , m_state{state}
                    {}
                    communicator(const communicator&) = default;
                    communicator(communicator&&) = default;
                    communicator& operator=(const communicator&) = default;
                    communicator& operator=(communicator&&) = default;

                  public: // member functions
                    rank_type rank() const noexcept { return m_shared_state->rank(); }
                    rank_type size() const noexcept { return m_shared_state->size(); }
                    address_type address() const noexcept { return rank(); }

                    /** @brief send a message. The message must be kept alive by the caller until the communication is
                     * finished.
                     * @tparam Message a meassage type
                     * @param msg an l-value reference to the message to be sent
                     * @param dst the destination rank
                     * @param tag the communication tag
                     * @return a future to test/wait for completion */
                    template<typename Message>
                    [[nodiscard]] future<void> send(const Message& msg, rank_type dst, tag_type tag) {
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Isend(reinterpret_cast<const void*>(msg.data()),
                                                        sizeof(typename Message::value_type) * msg.size(), MPI_BYTE,
                                                        dst, tag, m_shared_state->m_comm, &req.get()));
                        req.m_kind = request_kind::send;
                        return req;
                    }

                    /** @brief receive a message. The message must be kept alive by the caller until the communication is
                     * finished.
                     * @tparam Message a meassage type
                     * @param msg an l-value reference to the message to be sent
                     * @param src the source rank
                     * @param tag the communication tag
                     * @return a future to test/wait for completion */
                    template<typename Message>
                    [[nodiscard]] future<void> recv(Message& msg, rank_type src, tag_type tag) {
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Irecv(reinterpret_cast<void*>(msg.data()),
                                                        sizeof(typename Message::value_type) * msg.size(), MPI_BYTE,
                                                        src, tag, m_shared_state->m_comm, &req.get()));
                        req.m_kind = request_kind::recv;
                        return req;
                    }

                    template<typename Message>
                    void register_send(const Message& msg, rank_type dst, tag_type tag) {
                        if (m_state->m_sync_count != m_state->m_sync_send) {
                            m_state->m_put_sends.clear();
                            m_state->m_sync_send = m_state->m_sync_count;
                        }

                        using value_type = typename Message::value_type;
                        m_shared_state->m_thread_primitives->critical(
                            [this, dst]() { m_shared_state->m_access_ranks.insert(dst); });
                        
                        m_state->m_put_sends.emplace_back( msg.data(), msg.size()*sizeof(value_type), dst );
                        auto& item = m_state->m_put_sends.back();
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Irecv(item.m_recv_address.get(), 1, MPI_AINT,
                                                        dst, tag, m_shared_state->m_comm, &req.get()));
                        req.m_kind = request_kind::recv;
                        item.m_future = std::move(req);
                    }

                    template<typename Message>
                    void register_recv(Message& msg, rank_type src, tag_type tag) {
                        if (m_state->m_sync_count != m_state->m_sync_recv) {
                            for (auto& item : m_state->m_put_recvs)
                                GHEX_CHECK_MPI_RESULT(MPI_Win_detach(m_shared_state->m_win, item.m_buffer));
                            m_state->m_put_recvs.clear();
                            m_state->m_sync_recv = m_state->m_sync_count;
                        }

                        using value_type = typename Message::value_type;
                        GHEX_CHECK_MPI_RESULT(MPI_Win_attach(m_shared_state->m_win, msg.data(), msg.size()*sizeof(value_type)));
                        m_shared_state->m_thread_primitives->critical(
                            [this, src]() { m_shared_state->m_exposure_ranks.insert(src); });

                        m_state->m_put_recvs.emplace_back(msg.data());
                        auto& item = m_state->m_put_recvs.back();
                        GHEX_CHECK_MPI_RESULT(MPI_Get_address(msg.data(), item.m_recv_address.get()));
                        request req;
                        GHEX_CHECK_MPI_RESULT(MPI_Isend(item.m_recv_address.get(), 1, MPI_AINT,
                                                        src, tag, m_shared_state->m_comm, &req.get()));
                        req.m_kind = request_kind::send;
                        item.m_future = std::move(req);
                    }

                    void sync_register() {

                        for (auto& item : m_state->m_put_sends)
                            item.m_future.wait();
                        for (auto& item : m_state->m_put_recvs)
                            item.m_future.wait();
                        ++m_state->m_sync_count;

                        auto& token = *(m_state->m_token_ptr);
                        auto& tp = *(m_shared_state->m_thread_primitives);
                        auto& st = *m_shared_state;
                        //tp.barrier(token);
                        barrier();
                        tp.single(token, [this,&st]() {
                            if (st.m_access_ranks.size())
                            {
                                std::cout << rank() << " access ranks: ";
                                for (auto r : st.m_access_ranks) std::cout << r << " ";
                                std::cout << std::endl;
                                MPI_Group_free(&(st.m_access_group));
                                std::vector<rank_type> access_ranks(st.m_access_ranks.begin(), st.m_access_ranks.end());
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Group_incl(st.m_group, access_ranks.size(), access_ranks.data(), &(st.m_access_group)));
                                st.m_access_ranks.clear();
                            }
                            if (st.m_exposure_ranks.size())
                            {
                                std::cout << rank() << " exposure ranks: ";
                                for (auto r : st.m_exposure_ranks) std::cout << r << " ";
                                std::cout << std::endl;
                                MPI_Group_free(&(st.m_exposure_group));
                                std::vector<rank_type> exposure_ranks(st.m_exposure_ranks.begin(), st.m_exposure_ranks.end());
                                GHEX_CHECK_MPI_RESULT(
                                    MPI_Group_incl(st.m_group, exposure_ranks.size(), exposure_ranks.data(), &(st.m_exposure_group)));
                                st.m_exposure_ranks.clear();
                            }
                        }); 
                        //tp.barrier(token);
                        barrier();
                    }

                    void post_bulk() {
                        auto& token = *(m_state->m_token_ptr);
                        auto& tp = *(m_shared_state->m_thread_primitives);
                        auto& st = *m_shared_state;
                        /*tp.single(token, [this,&st]() {   
                            MPI_Win_post(st.m_exposure_group, 0, st.m_win);
                            MPI_Win_start(st.m_access_group, 0, st.m_win);
                            //while ( st.m_counter.load() > 0 ) {}
                            while ( st.m_epoch ) {}
                            st.m_counter = st.m_thread_primitives->size();
                            st.m_epoch = true; } );
                        while (!st.m_epoch) {}*/

                        //if (++st.m_counter == 1)
                        //tp.master(token, [this,&st]()
                        tp.single(token, [this,&st]()
                        //if (token.id() == 0)
                        {
                            MPI_Win_post(st.m_exposure_group, 0, st.m_win);
                            MPI_Win_start(st.m_access_group, 0, st.m_win);
                            st.m_epoch = true;
                        });
                        //tp.barrier(token);
                        while (!st.m_epoch) {}
                        
                        for (auto& item : m_state->m_put_sends)
                            GHEX_CHECK_MPI_RESULT(
                                MPI_Put(item.m_buffer, item.m_size, MPI_BYTE, item.m_rank,
                                        *(item.m_recv_address), item.m_size, MPI_BYTE, st.m_win));
                    }

                    void wait_bulk() {
                        auto& token = *(m_state->m_token_ptr);
                        auto& tp = *(m_shared_state->m_thread_primitives);
                        auto& st = *m_shared_state;
                        //if (--st.m_counter == 0)
                        tp.barrier(token);
                        //st.m_epoch = false;
                        //tp.barrier(token);
                        tp.single(token, [this,&st]()
                        //if (token.id() == 0)
                        {
                            MPI_Win_complete(st.m_win);

                            MPI_Win_wait(st.m_win);
                            st.m_epoch = false;

                        });
                        tp.barrier(token);
                    }


                    /** @brief Function to poll the transport layer and check for completion of operations with an
                      * associated callback. When an operation completes, the corresponfing call-back is invoked
                      * with the message, rank and tag associated with this communication.
                      * @return non-zero if any communication was progressed, zero otherwise. */
                    progress_status progress() { return m_state->progress(); }

                   /** @brief send a message and get notified with a callback when the communication has finished.
                     * The ownership of the message is transferred to this communicator and it is safe to destroy the
                     * message at the caller's site. 
                     * Note, that the communicator has to be progressed explicitely in order to guarantee completion.
                     * @tparam CallBack a callback type with the signature void(message_type, rank_type, tag_type)
                     * @param msg r-value reference to any_message instance
                     * @param dst the destination rank
                     * @param tag the communication tag
                     * @param callback a callback instance
                     * @return a request to test (but not wait) for completion */
                    template<typename CallBack>
                    request_cb_type send(message_type&& msg, rank_type dst, tag_type tag, CallBack&& callback)
                    {
                        auto fut = send(msg, dst, tag);
                        if (fut.ready())
                        {
                            callback(std::move(msg), dst, tag);
                            ++(m_state->m_progressed_sends);
                            return {};
                        }
                        else
                        {
                            return { &m_state->m_send_queue,
                                m_state->m_send_queue.enqueue(std::move(msg), dst, tag, std::move(fut), 
                                        std::forward<CallBack>(callback))};
                        }
                    }
                    
                   /** @brief receive a message and get notified with a callback when the communication has finished.
                     * The ownership of the message is transferred to this communicator and it is safe to destroy the
                     * message at the caller's site. 
                     * Note, that the communicator has to be progressed explicitely in order to guarantee completion.
                     * @tparam CallBack a callback type with the signature void(message_type, rank_type, tag_type)
                     * @param msg r-value reference to any_message instance
                     * @param src the source rank
                     * @param tag the communication tag
                     * @param callback a callback instance
                     * @return a request to test (but not wait) for completion */
                    template<typename CallBack>
                    request_cb_type recv(message_type&& msg, rank_type src, tag_type tag, CallBack&& callback)
                    {
                        auto fut = recv(msg, src, tag);
                        if (fut.ready())
                        {
                            callback(std::move(msg), src, tag);
                            ++(m_state->m_progressed_recvs);
                            return {};
                        }
                        else
                        {
                            return { &m_state->m_recv_queue,
                                m_state->m_recv_queue.enqueue(std::move(msg), src, tag, std::move(fut), 
                                        std::forward<CallBack>(callback))};
                        }
                    }

                    void barrier() {
                        if (auto token_ptr = m_state->m_token_ptr) {
                            auto& tp = *(m_shared_state->m_thread_primitives);
                            auto& token = *token_ptr;
                            tp.single(token, [this]() { MPI_Barrier(m_shared_state->m_comm); } );
                            progress(); // progress once more to set progress counters to zero
                            tp.barrier(token);
                        }
                        else
                            MPI_Barrier(m_shared_state->m_comm);
                    }
                };

            } // namespace mpi

        } // namespace tl

    } // namespace ghex

} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_MPI_COMMUNICATOR_HPP */

