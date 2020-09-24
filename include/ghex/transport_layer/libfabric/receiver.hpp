#ifndef GHEX_LIBFABRIC_RECEIVER_HPP
#define GHEX_LIBFABRIC_RECEIVER_HPP

#include <ghex/transport_layer/libfabric/rma/detail/memory_region_impl.hpp>
#include <ghex/transport_layer/libfabric/rma/detail/memory_region_allocator.hpp>
#include <ghex/transport_layer/libfabric/rma/memory_pool.hpp>
#include <ghex/transport_layer/libfabric/rma/atomic_count.hpp>
//
#include <ghex/transport_layer/libfabric/performance_counter.hpp>
//
#include <ghex/transport_layer/libfabric/libfabric_region_provider.hpp>
#include <ghex/transport_layer/libfabric/rma_base.hpp>
#include <ghex/transport_layer/libfabric/controller.hpp>
#include <ghex/transport_layer/libfabric/unique_function.hpp>
#include <ghex/transport_layer/libfabric/performance_counter.hpp>
#include <ghex/transport_layer/callback_utils.hpp>
//
#include <boost/container/small_vector.hpp>
//
#include <cstdint>

namespace gridtools { namespace ghex {
    // cppcheck-suppress ConfigurationNotChecked
    static hpx::debug::enable_print<false> recv_deb("RECEIVE");
}}

namespace gridtools {
namespace ghex {
namespace tl {
namespace libfabric
{

    struct receiver : public rma_base
    {
        friend class controller;

        using region_provider    = libfabric_region_provider;
        using region_type        = rma::detail::memory_region_impl<region_provider>;
        using memory_pool_type   = rma::memory_pool<region_provider>;
        using libfabric_msg_type = message_buffer<rma::memory_region_allocator<unsigned char>>;
        using any_msg_type       = gridtools::ghex::tl::libfabric::any_libfabric_message;

        // internal handler type
        using postprocess_receiver_fn = unique_function<void(receiver*)>;

    private:
        fid_ep                            *endpoint_;
        libfabric_region_holder            message_holder_;
        region_type                       *message_region_;
        rma::memory_pool<region_provider> *memory_pool_;

        // used by all receivers
        postprocess_receiver_fn            postprocess_handler_;

        // needed by tagged receivers
        fi_addr_t                         src_addr_;
        uint64_t                          tag_;
        unique_function<void(void)>       user_cb_;

        // shared performance counters used by all receivers
        static performance_counter<unsigned int> messages_handled_;
        static performance_counter<unsigned int> receives_pre_posted_;

    public:
        // --------------------------------------------------------------------
        // for vector storage, we need a move constructor
        receiver(receiver &&other) = default;

        // --------------------------------------------------------------------
        // construct receive object
        receiver(fid_ep* endpoint,
                 rma::memory_pool<region_provider>& memory_pool,
                 postprocess_receiver_fn &&handler)
            : rma_base(ctx_receiver)
            , endpoint_(endpoint)
            , message_region_(nullptr)
//            , message_region_temp_(false)
            , memory_pool_(&memory_pool)
        {
//            recv_deb.trace(hpx::debug::str<>("created receiver")
//                , hpx::debug::ptr(this));

            // called when the receiver completes - during cleanup
            postprocess_handler_ = std::move(handler);
        }

        // --------------------------------------------------------------------
        // destruct receive object
        ~receiver()
        {
        }

        // --------------------------------------------------------------------
        // the receiver posts a single receive buffer to the queue, attaching
        // itself as the context, so that when a message is received
        // the owning receiver is called to handle processing of the buffer
        void receive_tagged_region(region_type *recv_region)
        {
            [[maybe_unused]] auto scp = recv_deb.scope(__func__);
            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("map contents")
                , memory_pool_->region_alloc_pointer_map_.debug_map()));

            // this should never actually return true and yield/sleep
            bool ok = false;
            while(!ok) {
                uint64_t ignore = 0;
                ssize_t ret = fi_trecv(this->endpoint_,
                    recv_region->get_address(),
                    recv_region->get_size(),
                    recv_region->get_local_key(),
                    src_addr_, tag_, ignore, this);
                if (ret ==0) {
                    ok = true;
                }
                else if (ret == -FI_EAGAIN)
                {
                    recv_deb.error("reposting fi_recv\n");
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
                else if (ret != 0)
                {
                    throw fabric_error(int(ret), "pp_post_rx");
                }
            }
        }

        // --------------------------------------------------------------------
        void init_message_data(const libfabric_msg_type &msg, uint64_t tag)
        {
            tag_            = tag;
            message_region_ = msg.get_buffer().m_pointer.region_;
            message_region_->set_message_length(msg.size());
        }

        void init_message_data(const any_libfabric_message &msg, uint64_t tag)
        {
            tag_            = tag;
            message_region_ = msg.region();
            message_region_->set_message_length(msg.size());
        }

        template <typename Message,
                  typename Enable = typename std::enable_if<
                      !std::is_same<libfabric_msg_type, Message>::value &&
                      !std::is_same<any_libfabric_message, Message>::value>::type>
        void init_message_data(Message &msg, uint64_t tag)
        {
            message_holder_.set_rma_from_pointer(msg.data(), msg.size());
            message_region_ = message_holder_.m_region;
            tag_            = tag;
        }

        // libfabric message customization (known memory region)
        template<typename Callback>
        void receive_tagged_msg(Callback &&cb_fn)
        {
            user_cb_ = std::forward<Callback>(cb_fn);
            receive_tagged_region(message_region_);
        }

        // generic message sender (reference to message)
        // message can be a vector or anything that supports data/size
        template<typename Message, typename Callback>
        void receive_tagged_msg(Message &msg,
                             uint64_t tag,
                             Callback &&cb_fn)
        {
            tag_     = tag;
            user_cb_ = std::move(cb_fn);
            receive_tagged_data(msg.data(), msg.size());
        }

        template<typename Callback>
        void receive_tagged_msg(libfabric_msg_type &msg,
                             uint64_t tag,
                             Callback &&cb_fn)
        {
            init_message_data(msg, tag);
            user_cb_ = std::move(cb_fn);
            receive_tagged_region(message_region_);
        }

        // --------------------------------------------------------------------
        // Not used, provided for potential/future rma_base compatibility
        void handle_error(struct fi_cq_err_entry) {}

        // --------------------------------------------------------------------
        void handle_cancel() {
        }

        // --------------------------------------------------------------------
        int handle_recv_completion(std::uint64_t /*len*/)
        {
            [[maybe_unused]] auto scp = recv_deb.scope(__func__);
//            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("map contents")
//                                , memory_pool_->region_alloc_pointer_map_.debug_map())));

            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("handling recv")
                , "tag", hpx::debug::hex<16>(tag_)
                , "pre-posted" , hpx::debug::dec<>(--receives_pre_posted_)));

            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("ghex region")
            , *message_region_));

            ++messages_handled_;

//            // cleanup temp region
//            if (message_region_temp_) {
//                GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("Receiver")
//                                     , hpx::debug::ptr(this)
//                                     , "free temp region "
//                                     , message_region_));
//                memory_pool_->remove_address_from_map(message_region_->get_address(), message_region_);
//                memory_pool_->deallocate(message_region_);
//                GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("map contents")
//                                    , memory_pool_->region_alloc_pointer_map_.debug_map()));
//            }
            message_region_ = nullptr;

            // call user supplied completion callback
            user_cb_();

            // clear the callback to an empty state
            // (in case it holds reference counts that must be released)
            user_cb_ = [](){};

            // return the receiver to the available list
            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("Receiver")
                           , hpx::debug::ptr(this)
                           , "calling postprocess_handler"));
            postprocess_handler_(this);
            return 1;
        }

        bool cancel()
        {
            bool ok = (fi_cancel(&this->endpoint_->fid, this) == 0);
            if (!ok) return ok;

            // cleanup as if we had completed, but without calling any
            // user callbacks
            user_cb_ = [](){};

            // cleanup temp region
//            if (message_region_temp_) {
//                GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("Receiver")
//                               , hpx::debug::ptr(this)
//                               , "disposing of temp region"));
//                GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("free temp region "), message_region_));
//                memory_pool_->remove_address_from_map(message_region_->get_address(), message_region_);
//                memory_pool_->deallocate(message_region_);
//                GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("map contents")
//                                    , memory_pool_->region_alloc_pointer_map_.debug_map()));
//            }
            message_region_ = nullptr;

            // return the receiver to the available list
            GHEX_DP_LAZY(recv_deb, recv_deb.debug(hpx::debug::str<>("Receiver")
                                 , hpx::debug::ptr(this)
                                 , "cancel"
                                 , "calling postprocess_handler"));
            postprocess_handler_(this);
            return ok;
        }
    };

    performance_counter<unsigned int> receiver::messages_handled_(0);
    performance_counter<unsigned int> receiver::receives_pre_posted_(0);

}}}}

#endif
