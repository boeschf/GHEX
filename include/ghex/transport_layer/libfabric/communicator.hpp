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

                namespace detail {

                    // -----------------------------------------------------------------
                    // an MPI error handling type that we can use to intercept
                    // MPI errors if we enable the error handler
                    static MPI_Errhandler hpx_mpi_errhandler = 0;

                    // extract MPI error message
                    std::string error_message(int code)
                    {
                        int N = 1023;
                        std::unique_ptr<char[]> err_buff(new char[std::size_t(N) + 1]);
                        err_buff[0] = '\0';

                        MPI_Error_string(code, err_buff.get(), &N);

                        return err_buff.get();
                    }

                    // function that converts an MPI error into an exception
                    void hpx_MPI_Handler(MPI_Comm*, int* errorcode, ...)
                    {
                        std::string temp = std::string("hpx_MPI_Handler : ") +
                                detail::error_message(*errorcode);
                        std::cout << temp << std::endl;
                        throw(std::runtime_error(temp));
                    }

                    // set an error handler for communicators that will be called
                    // on any error instead of the default behavior of program termination
                    void set_error_handler()
                    {
                        MPI_Comm_create_errhandler(
                            detail::hpx_MPI_Handler, &detail::hpx_mpi_errhandler);
                        MPI_Comm_set_errhandler(MPI_COMM_WORLD, detail::hpx_mpi_errhandler);
                    }
                }

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

                    rank_type rank() const noexcept { return m_context->m_rank; }
                    rank_type size() const noexcept { return m_context->m_size; }
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

                    thread_token        *m_token_ptr;
                    struct fid_ep       *endpoint_;
                    struct fid_domain   *fabric_domain_;
                    //
                    int  m_progressed_sends = 0;
                    int  m_progressed_recvs = 0;
                    int m_progressed_cancels = 0;
                    //

                    // We maintain a list of senders that are being used
                    using sender_list = std::stack<sender*>;
                    std::atomic<unsigned int> senders_in_use_;

                    communicator_state(thread_token* t, struct fid_ep *endpoint, struct fid_domain *domain)
                    : m_token_ptr{t}
                    , endpoint_{endpoint}
                    , fabric_domain_{domain}
                    {
                        // only init the sender list once per thread
                        static thread_local std::atomic_flag initialized = ATOMIC_FLAG_INIT;
                        if (initialized.test_and_set()) return;
                        //
                        for (std::size_t i = 0; i < 16; ++i)
                        {
                            add_sender_to_pool();
                        }
                    }

                    ~communicator_state()
                    {
                        // clear senders list
                        int i = 0;
                        bool ok = true;
                        sender* snd = nullptr;
                        while (!senders()->empty()) {
                            snd = senders()->top();
                            std::cout << "Deleting sender " << i++ << " " << senders()->size() << " " << snd << std::endl;
                            delete snd;
                            senders()->pop();
                            std::cout << "Deleted sender " << senders()->size() << " " << snd << std::endl;
                        }
                    }

                    // --------------------------------------------------
                    static sender_list *senders() {
                        static thread_local std::unique_ptr<sender_list> instance(new sender_list());
                        return instance.get();
                    }

                    // --------------------------------------------------------------------
                    // return a sender object
                    // --------------------------------------------------------------------
                    void add_sender_to_pool() {
                        sender *snd = new sender(endpoint_, fabric_domain_);
                        // after a sender has been used, it's postprocess handler
                        // is called, this returns it to the free list
                        snd->postprocess_handler_ = [this](sender* s)
                            {
            //                    --senders_in_use_;
                                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("senders in use")
                                              , "(-- stack sender)"
                                              , hpx::debug::ptr(s)
                                              /*, hpx::debug::dec<>(senders_in_use_)*/));
std::cout << hpx::debug::hex<8>(std::this_thread::get_id()) << " Pushing sender (PPH) " << s << std::endl;
                                senders()->push(s);
std::cout << hpx::debug::hex<8>(std::this_thread::get_id()) << " Pushed sender " << senders()->size() << " " << s << std::endl;
                                // trigger_pending_work();
                            };
                        // put the new sender on the free list
std::cout << hpx::debug::hex<8>(std::this_thread::get_id()) << " Pushing sender " << snd << std::endl;
                        senders()->push(snd);
std::cout << hpx::debug::hex<8>(std::this_thread::get_id()) << " Pushed sender " << senders()->size() << " " << snd << std::endl;
                    }

                    sender* get_sender(int rank)
                    {
                        sender* snd = nullptr;
                        // pop an sender from the free list, if that fails, create a new one
                        if (senders()->empty())
                        {
                            add_sender_to_pool();
                        }
                        snd = senders()->top();
                        senders()->pop();
                        std::cout << "Popped sender " << " " << snd << std::endl;

            //            ++senders_in_use_;

                        // debug info only

//                        if (com_deb.is_enabled()) {
//                            libfabric::locality addr;
//                            addr.set_fi_address(fi_addr_t(rank));
//                            std::size_t addrlen = libfabric::locality_defs::array_size;
//                            int ret = fi_av_lookup(av_, fi_addr_t(rank),
//                                                   addr.fabric_data_writable(), &addrlen);
//                            if ((ret == 0) && (addrlen==libfabric::locality_defs::array_size)) {
//                                GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("get_sender")
//                                    , hpx::debug::ptr(snd)
//                                    , "from", iplocality(here_)
//                                    , "to" , iplocality(addr)
//                                    , "dest rank", hpx::debug::hex<4>(rank)
//                                    , "fi_addr (rank)" , hpx::debug::hex<4>(fi_addr_t(rank))
//                                    , "senders in use (get_sender)"
//                                    /*, hpx::debug::dec<>(senders_in_use_)*/));
//                            }
//                            else {
//                                throw std::runtime_error(
//                                    "get_sender : address vector traversal lookup failure");
//                            }
//                        }

                        // set the sender destination address/offset in AV table
                        snd->dst_addr_ = fi_addr_t(rank);
                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("message_region_owned_ = false")));

                        return snd;
                    }

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

                /** @brief A communicator for point-to-point communication.
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
                    {
                        detail::set_error_handler();
                    }
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
                        ghex::tl::libfabric::sender *sndr = m_state->get_sender(dst);

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

                    template<typename CallBack>
                    request_cb_type send(any_libfabric_message& msg, rank_type dst, tag_type tag, CallBack&& callback) {
std::cout << "Using reference send function " << std::endl;
                        GHEX_CHECK_CALLBACK_F(message_type,rank_type,tag_type)
                        using V = typename std::remove_reference_t<any_libfabric_message>::value_type;
                        return send(message_type{libfabric_ref_message<V>{msg.data(),msg.size(), msg.m_holder.m_region}}, dst, tag, std::forward<CallBack>(callback));
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
                        ghex::tl::libfabric::sender *sndr = m_state->get_sender(dst);

                        // create a request
                        std::shared_ptr<context_info> result(new context_info{false, nullptr});
                        request req{controller, request_kind::recv, std::move(result)};

                        GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send any"), "(callback)"
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
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send any"),"(callback lambda)"
                                 , "F(set)", hpx::debug::dec<>(dst)));
                            callback(std::move(msg), dst, tag);
                            GHEX_DP_LAZY(com_deb, com_deb.debug(hpx::debug::str<>("Send any"),"(callback lambda)"
                                 , "done", hpx::debug::dec<>(dst)));
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
                            tp.single(token, [this]() { MPI_Barrier(m_shared_state->m_context->m_comm); } );
                            progress(); // progress once more to set progress counters to zero
                            tp.barrier(token);
                        }
                        else
                            MPI_Barrier(m_shared_state->m_context->m_comm);
                    }
                };

            } // namespace libfabric
        } // namespace tl
    } // namespace ghex
} // namespace gridtools


#endif /* INCLUDED_GHEX_TL_LIBFABRIC_COMMUNICATOR_CONTEXT_HPP */

