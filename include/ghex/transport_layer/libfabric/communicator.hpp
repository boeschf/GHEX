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
#ifndef INCLUDED_GHEX_TL_LIBFABRIC_COMMUNICATOR_CONTEXT_HPP
#define INCLUDED_GHEX_TL_LIBFABRIC_COMMUNICATOR_CONTEXT_HPP

#include <atomic>
//
#include <ghex/transport_layer/shared_message_buffer.hpp>
#include <ghex/transport_layer/libfabric/controller.hpp>
#include <ghex/transport_layer/libfabric/future.hpp>
#include <ghex/transport_layer/libfabric/sender.hpp>

namespace gridtools {
    namespace ghex {

        // cppcheck-suppress ConfigurationNotChecked
        static hpx::debug::enable_print<false> com_deb("COMMUNI");

        namespace tl {
            namespace libfabric {            

                using region_provider    = libfabric_region_provider;
                using region_type        = rma::detail::memory_region_impl<region_provider>;

                struct status_t {};

                /** @brief common data which is shared by all communicators. This class is thread safe.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                struct shared_communicator_state {
                    using thread_primitives_type = ThreadPrimitives;
                    using transport_context_type = transport_context<libfabric_tag, ThreadPrimitives>;
                    using rank_type = int;
                    using tag_type = std::uint64_t;
                    using controller_type = ghex::tl::libfabric::controller;

                    controller_type        *m_controller;
                    transport_context_type *m_context;
                    thread_primitives_type *m_thread_primitives;

                    shared_communicator_state(controller_type *control, transport_context_type* tc, thread_primitives_type* tp)
                    : m_controller{control}
                    , m_context{tc}
                    , m_thread_primitives{tp}
                    {}

                    rank_type rank() const noexcept { return m_controller->m_rank_; }
                    rank_type size() const noexcept { return m_controller->m_size_; }
                };

                /** @brief communicator per-thread data.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                struct communicator_state {
                    using shared_state_type      = shared_communicator_state<ThreadPrimitives>;
                    using thread_primitives_type = ThreadPrimitives;
                    using thread_token           = typename thread_primitives_type::token;
                    using rank_type              = typename shared_state_type::rank_type;
                    using tag_type               = typename shared_state_type::tag_type;
                    template<typename T> using future = future_t<T>;
                    using progress_status        = ghex::tl::cb::progress_status;
                    using controller_shared      = std::shared_ptr<ghex::tl::libfabric::controller>;

                    thread_token* m_token_ptr;
                    int  m_progressed_sends = 0;
                    int  m_progressed_recvs = 0;
                    int m_progressed_cancels = 0;

                    communicator_state(thread_token* t)
                    : m_token_ptr{t}
                    {}

                    progress_status progress() {
                        return {
                            std::exchange(m_progressed_sends,0),
                            std::exchange(m_progressed_recvs,0),
                            std::exchange(m_progressed_cancels,0)};
                    }
                };

                /** @brief completion handle returned from callback based communications
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                struct request_cb : public gridtools::ghex::tl::libfabric::request_t
                {
                    using shared_state_type = shared_communicator_state<ThreadPrimitives>;
                    using state_type        = communicator_state<ThreadPrimitives>;
                    using any_message_type  = ::gridtools::ghex::tl::libfabric::any_libfabric_message;
                    using tag_type          = typename shared_state_type::tag_type;
                    using rank_type         = int;
                    template <typename T>
                    using future            = future_t<T>;
                    using completion_type   = ::gridtools::ghex::tl::cb::request;
                    using queue_type = ::gridtools::ghex::tl::cb::callback_queue<future<void>, rank_type, tag_type>;

                    using gridtools::ghex::tl::libfabric::request_t::request_t;
                    using gridtools::ghex::tl::libfabric::request_t::m_controller;
                    using gridtools::ghex::tl::libfabric::request_t::m_kind;
                    using gridtools::ghex::tl::libfabric::request_t::m_lf_ctxt;

                    request_cb(const gridtools::ghex::tl::libfabric::request_t &r) {
                        m_controller = r.m_controller;
                        m_kind       = r.m_kind;
                        m_lf_ctxt    = r.m_lf_ctxt;
                    }
                };

                /** @brief A communicator for MPI point-to-point communication.
                  * This class is lightweight and copying/moving instances is safe and cheap.
                  * Communicators can be created through the context, and are thread-compatible.
                  * @tparam ThreadPrimitives The thread primitives type */
                template<typename ThreadPrimitives>
                class communicator {
                  public: // member types
                    using thread_primitives_type = ThreadPrimitives;
                    using shared_state_type      = shared_communicator_state<ThreadPrimitives>;
                    using transport_context_type = transport_context<libfabric_tag, ThreadPrimitives>;
                    using thread_token           = typename thread_primitives_type::token;
                    using state_type             = communicator_state<ThreadPrimitives>;
                    using rank_type              = int;
                    using tag_type               = typename shared_state_type::tag_type;
                    using request                = request_t;

                    template<typename T> using future = future_t<T>;
                    template<typename T> using allocator_type  = ghex::tl::libfabric::rma::memory_region_allocator<T>;
                    using lf_allocator_type  = allocator_type<unsigned char>;

                    using address_type              = rank_type;
                    using request_cb_type           = request_cb<ThreadPrimitives>;
                    using message_type              = typename request_cb_type::any_message_type;
                    using any_message_type          = typename request_cb_type::any_message_type;
                    using libfabric_msg_type        = message_buffer<lf_allocator_type>;
                    using libfabric_sharedmsg_type  = shared_message_buffer<lf_allocator_type>;

                    using progress_status = ghex::tl::cb::progress_status;

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

                    inline std::uint64_t make_tag64(std::uint32_t tag) {
                        return (std::uint64_t(m_shared_state->m_context->ctag_) << 32) |
                                (std::uint64_t(tag & 0xFFFFFFFF));
                    }

                    /** @brief send a message. The message must be kept alive by the caller until the communication is
                     * finished.
                     * @tparam Message a message type
                     * @param msg an l-value reference to the message to be sent
                     * @param dst the destination rank
                     * @param tag the communication tag
                     * @return a future to test/wait for completion */
                    template<typename Message>
                    [[nodiscard]] future<void> send(const Message& msg, rank_type dst, tag_type tag)
                    {
                        [[maybe_unused]] auto scp = com_deb.scope(this, __func__, "(future)");

                        std::uint64_t stag = make_tag64(tag);

                        // get main libfabric controller
                        auto controller = m_shared_state->m_controller;
                        // get a sender
                        ghex::tl::libfabric::sender *sndr = controller->get_sender(dst);

                        // create a request
                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
                        request req{controller, request_kind::recv, std::move(result)};

                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send (future)")
                            , "thisrank", hpx::debug::dec<>(rank())
                            , "rank", hpx::debug::dec<>(dst)
                            , "tag", hpx::debug::hex<16>(std::uint64_t(tag))
                            , "cxt", hpx::debug::hex<8>(m_shared_state->m_context)
                            , "stag", hpx::debug::hex<16>(stag)
                            , "sndr", hpx::debug::ptr(sndr)
                            , "addr", hpx::debug::ptr(msg.data())
                            , "size", hpx::debug::hex<6>(msg.size())));

                        sndr->init_message_data(msg, stag);

                        // async send, with callback to set the future ready when transfer is complete
                        sndr->send_tagged_msg([p=req.m_lf_ctxt]() {
                            p->m_ready = true;
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send (future)"), "F(set)"));
                        });

                        // future constructor will be called with request as param
                        return req;
                    }

                    /** @brief send a message and get notified with a callback when the communication has finished.
                      * The ownership of the message is transferred to this communicator and it is safe to destroy the
                      * message at the caller's site.
                      * Note, that the communicator has to be progressed explicitely in order to guarantee completion.
                      * @tparam CallBack a callback type with the signature void(any_message, rank_type, tag_type)
                      * @param msg r-value reference to any_message instance
                      * @param dst the destination rank
                      * @param tag the communication tag
                      * @param callback a callback instance
                      * @return a request to test (but not wait) for completion */
                    template<typename CallBack>
                    request_cb_type send(any_message_type&& msg, rank_type dst, tag_type tag, CallBack&& callback)
                    {
                        [[maybe_unused]] auto scp = com_deb.scope(this, __func__, "(callback)");

                        std::uint64_t stag = make_tag64(tag);

                        // get main libfabric controller
                        auto controller = m_shared_state->m_controller;
                        // get a sender
                        ghex::tl::libfabric::sender *sndr = controller->get_sender(dst);

                        // create a request
                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
                        request req{controller, request_kind::recv, std::move(result)};

                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send (callback)")
                         , "thisrank", hpx::debug::dec<>(rank())
                         , "rank", hpx::debug::dec<>(dst)
                         , "tag", hpx::debug::hex<16>(std::uint64_t(tag))
                         , "cxt", hpx::debug::hex<8>(m_shared_state->m_context)
                         , "stag", hpx::debug::hex<16>(stag)
                         , "sndr", hpx::debug::ptr(sndr)
                         , "addr", hpx::debug::ptr(msg.data())
                         , "size", hpx::debug::hex<6>(msg.size())));

                        // so that we can move the message int the callback,
                        // first extract any rma info we need
                        sndr->init_message_data(msg, stag);

                        // now move message into callback
                        auto lambda = [
                             p        = req.m_lf_ctxt,
                             msg      = std::move(msg),
                             callback = std::forward<CallBack>(callback),
                             dst, tag
                             ]() mutable
                        {
                            p->m_ready = true;
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send (callback)")
                                 , "F(set)", hpx::debug::dec<>(dst)));
                            callback(std::move(msg), dst, tag);
                        };

                        // perform a send with the callback for when transfer is complete
                        sndr->send_tagged_msg(std::move(lambda));
                        return req;
                    }

                    /** @brief receive a message. The message must be kept alive by the caller until the communication is
                     * finished.
                     * @tparam Message a message type
                     * @param msg an l-value reference to the message to be sent
                     * @param src the source rank
                     * @param tag the communication tag
                     * @return a future to test/wait for completion */
                    template<typename Message>
                    [[nodiscard]] future<void> recv(Message &msg, rank_type src, tag_type tag)
                    {
                        [[maybe_unused]] auto scp = com_deb.scope(this, __func__, "(future)");
                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("map contents")
                                            , m_shared_state->m_controller->memory_pool_->region_alloc_pointer_map_.debug_map()));

                        std::uint64_t stag = make_tag64(tag);

                        // main libfabric controller
                        auto controller = m_shared_state->m_controller;

                        // create a request
                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
                        request req{controller, request_kind::recv, std::move(result)};

                        // get a receiver object (tagged, with a callback)
                        ghex::tl::libfabric::receiver *rcv =
                                controller->get_expected_receiver(src);

                        // to support cancellation, we pass the context pointer (receiver)
                        // into the future shared state
                        req.m_lf_ctxt->m_lf_context = rcv;

                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (future)")
                            , "thisrank", hpx::debug::dec<>(rank())
                            , "rank", hpx::debug::dec<>(src)
                            , "tag", hpx::debug::hex<16>(std::uint64_t(tag))
                            , "cxt", hpx::debug::hex<8>(m_shared_state->m_context)
                            , "stag", hpx::debug::hex<16>(stag)
                            , "recv", hpx::debug::ptr(rcv)
                            , "addr", hpx::debug::ptr(msg.data())
                            , "size", hpx::debug::hex<6>(msg.size())));

                        rcv->init_message_data(msg, stag);

                        auto lambda = [p=req.m_lf_ctxt](){
                            p->m_ready = true;
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (future)"), "F(set)"));
                        };

                        rcv->receive_tagged_msg(std::move(lambda));
                        return req;
                    }

//                    template<typename Msg, typename CallBack>
//                    request_cb_type recv(Msg &&msg, rank_type src, tag_type tag, CallBack&& callback)
//                    {
//                        [[maybe_unused]] auto scp = com_deb.scope(this, __func__, "(callback)");
//                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("map contents")
//                                            , GHEX_DP_LAZY(m_shared_state->m_controller->memory_pool_->region_alloc_pointer_map_.debug_map(), com_deb));

//                        std::uint64_t stag = make_tag64(tag);

//                        // main libfabric controller
//                        auto controller = m_shared_state->m_controller;

//                        // create a request
//                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
//                        request req{controller, request_kind::recv, std::move(result)};

//                        // get a receiver object (tagged, with a callback)
//                        ghex::tl::libfabric::receiver *rcv =
//                                controller->get_expected_receiver(src);

//                        // to support cancellation, we pass the context pointer (receiver)
//                        // into the future shared state
//                        req.m_lf_ctxt->m_lf_context = rcv;

//                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (callback)")
//                            , "thisrank", hpx::debug::dec<>(rank())
//                            , "rank", hpx::debug::dec<>(src)
//                            , "tag", hpx::debug::hex<16>(std::uint64_t(tag))
//                            , "cxt", hpx::debug::hex<8>(m_shared_state->m_context)
//                            , "stag", hpx::debug::hex<16>(stag)
//                            , "recv", hpx::debug::ptr(rcv)
//                            , "addr", hpx::debug::ptr(msg.data())
//                            , "size", hpx::debug::hex<6>(msg.size()));

//                        // so that we can move the message int the callback,
//                        // first extract any rma info we need
//                        rcv->init_message_data(msg, stag);

//                        // now move message into callback
//                        auto lambda = [
//                             p        = req.m_lf_ctxt,
//                             msg      = std::forward<Msg>(msg),
//                             callback = std::forward<CallBack>(callback),
//                             src, tag
//                             ]() mutable
//                        {
//                            p->m_ready = true;
//                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (callback)")
//                                 , "F(set)", hpx::debug::dec<>(src));
//                            callback(std::move(msg), src, tag);
//                        };

//                        // perform a send with the callback for when transfer is complete
//                        rcv->receive_tagged_msg(std::move(lambda));
//                        return req;
//                    }

                    template<typename CallBack>
                    request_cb_type recv(any_libfabric_message &&msg, rank_type src, tag_type tag, CallBack&& callback)
                    {
                        [[maybe_unused]] auto scp = com_deb.scope(this, __func__, "(callback)");
                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("map contents")
                                            , m_shared_state->m_controller->memory_pool_->region_alloc_pointer_map_.debug_map()));

                        std::uint64_t stag = make_tag64(tag);

                        // main libfabric controller
                        auto controller = m_shared_state->m_controller;

                        // create a request
                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
                        request req{controller, request_kind::recv, std::move(result)};

                        // get a receiver object (tagged, with a callback)
                        ghex::tl::libfabric::receiver *rcv =
                                controller->get_expected_receiver(src);

                        // to support cancellation, we pass the context pointer (receiver)
                        // into the future shared state
                        req.m_lf_ctxt->m_lf_context = rcv;

                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (callback)")
                            , "thisrank", hpx::debug::dec<>(rank())
                            , "rank", hpx::debug::dec<>(src)
                            , "tag", hpx::debug::hex<16>(std::uint64_t(tag))
                            , "cxt", hpx::debug::hex<8>(m_shared_state->m_context)
                            , "stag", hpx::debug::hex<16>(stag)
                            , "recv", hpx::debug::ptr(rcv)
                            , "addr", hpx::debug::ptr(msg.data())
                            , "size", hpx::debug::hex<6>(msg.size())));

                        // so that we can move the message int the callback,
                        // first extract any rma info we need
                        rcv->init_message_data(msg, stag);

                        // now move message into callback
                        auto lambda = [
                             p        = req.m_lf_ctxt,
                             msg      = std::move(msg), // std::forward<Msg>(msg)
                             callback = std::forward<CallBack>(callback),
                             src, tag
                             ]() mutable
                        {
                            p->m_ready = true;
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Recv (callback)")
                                 , "F(set)", hpx::debug::dec<>(src)));
                            callback(std::move(msg), src, tag);
                        };

                        // perform a send with the callback for when transfer is complete
                        rcv->receive_tagged_msg(std::move(lambda));
                        return req;
                    }

//                    template<typename CallBack>
//                    request_cb_type recv(libfabric_sharedmsg_type &shared_msg, rank_type src, tag_type tag, CallBack&& callback)
//                    {
//                        return recv(shared_msg.get(), src, tag, std::forward<CallBack>(callback));
//                    }

                    /** @brief Function to poll the transport layer and check for completion of operations with an
                      * associated callback. When an operation completes, the corresponfing call-back is invoked
                      * with the message, rank and tag associated with this communication.
                      * @return non-zero if any communication was progressed, zero otherwise. */
                    progress_status progress() {
                        return m_shared_state->m_controller->poll_for_work_completions();
                    }

                    void barrier() {
                        if (auto token_ptr = m_state->m_token_ptr) {
                            auto& tp = *(m_shared_state->m_thread_primitives);
                            auto& token = *token_ptr;
                            tp.single(token, [this]() { MPI_Barrier(m_shared_state->m_controller->m_comm_); } );
                            progress(); // progress once more to set progress counters to zero
                            tp.barrier(token);
                        }
                        else
                            MPI_Barrier(m_shared_state->m_controller->m_comm_);
                    }
                };

            } // namespace libfabric
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_LIBFABRIC_COMMUNICATOR_CONTEXT_HPP */

