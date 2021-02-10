/*
            // 1) we need a valid address to send to the other ranks
            // but we must get it before binding the address
            // vector to the endpoint, so create passive endpoint
            ep_passive_ = create_passive_endpoint(fabric_, fabric_info_);

            // 2) enable it
            // -------------------------------------------------------------------
            struct fid_eq *event_queue_ = nullptr;
            fi_eq_attr eq_attr = {};
            eq_attr.wait_obj = FI_WAIT_NONE;
            int ret = fi_eq_open(fabric_, &eq_attr, &event_queue_, nullptr);
            if (ret) throw fabric_error(ret, "fi_eq_open");

            ret = fi_pep_bind(ep_passive_, &event_queue_->fid, 0);
            if (ret) throw fabric_error(ret, "fi_pep_bind");

            ret = fi_listen(ep_passive_);
            if (ret) throw fabric_error(ret, "fi_listen");

            // 3) get the endpoint address
            here_ = get_endpoint_address(&ep_passive_->fid);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Passive 'here'")
                                        , iplocality(here_)));

            // 4) now hand the address to the active endpoint
            fabric_info_->handle = &(ep_passive_->fid);
*/
/*
//            fi_close(&ep_passive_->fid);
//            if (event_queue_)
//                fi_close(&event_queue_->fid);
*/
#ifndef GHEX_LIBFABRIC_CONTROLLER_HPP
#define GHEX_LIBFABRIC_CONTROLLER_HPP

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
// ??
#include <sstream>
#include <cstdint>
#include <cstddef>
#include <cstring>
//
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
//
#include "ghex_libfabric_defines.hpp"
#include <ghex/transport_layer/libfabric/print.hpp>
#include <ghex/transport_layer/libfabric/fabric_error.hpp>
#include <ghex/transport_layer/libfabric/locality.hpp>
#include <ghex/transport_layer/libfabric/libfabric_region_provider.hpp>
#include <ghex/transport_layer/libfabric/rma/memory_pool.hpp>
#include <ghex/transport_layer/libfabric/rma/detail/memory_region_allocator.hpp>
#include <ghex/transport_layer/libfabric/rma_base.hpp>
#include <ghex/transport_layer/libfabric/unique_function.hpp>
//
#include <mpi.h>

// ------------------------------------------------
// Reliable Data Endpoint type
// ------------------------------------------------
#define GHEX_LIBFABRIC_ENDPOINT_RDM
// #define GHEX_LIBFABRIC_THREAD_LOCAL_ENDPOINTS

// ------------------------------------------------
// Needed on Cray for GNI extensions
// ------------------------------------------------
#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
# include "rdma/fi_ext_gni.h"
#endif

#ifdef GHEX_LIBFABRIC_HAVE_PMI
# include <pmi2.h>
#endif

#define GHEX_LIBFABRIC_FI_VERSION_MAJOR 1
#define GHEX_LIBFABRIC_FI_VERSION_MINOR 11

namespace gridtools { namespace ghex {
    // cppcheck-suppress ConfigurationNotChecked
    static hpx::debug::enable_print<true> cnt_deb("CONTROL");
}}

namespace gridtools {
namespace ghex {
namespace tl {
namespace libfabric
{

using region_provider    = libfabric_region_provider;
using region_type        = rma::detail::memory_region_impl<region_provider>;
using libfabric_msg_type = message_buffer<rma::memory_region_allocator<unsigned char>>;
using any_msg_type       = gridtools::ghex::tl::libfabric::any_libfabric_message;

class controller;

// This struct holds the ready state of a future
// we must also store the context used in libfabric, in case
// a request is cancelled - fi_cancel(...) needs it
struct context_info {
    // libfabric requires some space for it's internal bookkeeping
        // so the first member of this struct must be fi_context
    fi_context                      context_reserved_space;
    bool                            m_ready;
    fid_ep                         *endpoint_;
    region_type                    *message_region_;
    libfabric_region_holder         message_holder_;
    unique_function<void(void)>     user_cb_;
        using tag_type = std::uint64_t;
        tag_type                        tag_;
        bool                            is_send_;

    void init_message_data(const libfabric_msg_type &msg, uint64_t tag);
    void init_message_data(const any_libfabric_message &msg, uint64_t tag);
    template <typename Message,
              typename Enable = typename std::enable_if<
                  !std::is_same<libfabric_msg_type, Message>::value &&
                  !std::is_same<any_libfabric_message, Message>::value>::type>
    void init_message_data(Message &msg, uint64_t tag);
    int handle_send_completion();
    int handle_recv_completion(std::uint64_t /*len*/);
        //
        inline void set_ready() {
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("set_ready")
                                        , hpx::debug::hex<16>(tag_)
                                        , hpx::debug::str<4>(is_send_ ? "send" : "recv")));
            if (m_ready) {
                throw std::runtime_error("Future/Ready already set");
            }
            m_ready = true;
        }
        //
    bool cancel();
        //
        void handle_error(struct fi_cq_err_entry /*e*/) {
            std::terminate();
        }
};

    // when using thread local endpoints, we encapsulate things that
    // can be created/destroyed by the wrapper destructor
    struct endpoint_wrapper {
        std::unique_ptr<fid_ep> endpoint_;
        std::unique_ptr<fid_cq> cq_;
        //
        ~endpoint_wrapper() {
            fi_close(&endpoint_->fid);
        }
    };

    //    template<typename Tag, typename ThreadPrimitives>
    //    struct transport_context;

    // struct returned from polling functions
    // if any completions are handled (rma, send, recv),
    // then the completions_handled field should be set to true.
    // if Send/Receive completions that indicate the end of a message that
    // should trigger a future or callback in user level code occur, then
    // the user_msgs field should hold the count (1 per future/callback)
    // A non zero user_msgs count implies completions_handled must be set
    struct progress_count {
        bool completions_handled = false;
        std::uint32_t user_msgs = 0;
        //
        progress_count & operator +=(const progress_count & rhs) {
            completions_handled |= rhs.completions_handled;
            user_msgs += rhs.user_msgs;
            return *this;
        }
    };

    class controller
    {
    public:
        typedef std::mutex                   mutex_type;
        typedef std::lock_guard<mutex_type>  scoped_lock;

    private:
#ifdef GHEX_LIBFABRIC_THREAD_LOCAL_ENDPOINTS
        //
        thread_local endpoint_wrapper ep_local__;
#else
        struct fid_ep     *ep_tx_;
#endif
        struct fid_ep     *ep_rx_;

        struct fi_info    *fabric_info_;
        struct fid_fabric *fabric_;
        struct fid_domain *fabric_domain_;
        struct fid_pep    *ep_passive_;

        // we will use just one event queue for all connections
        struct fid_eq     *event_queue_;
        struct fid_cq     *txcq_, *rxcq_;
        struct fid_av     *av_;

        std::atomic<std::uint32_t> bootstrap_counter_;

        locality here_;
        locality root_;

        // store info about local device
        std::string  device_;
        std::string  interface_;
        sockaddr_in  local_addr_;

        // used during queue creation setup and during polling
        mutex_type   controller_mutex_;
        mutex_type   send_mutex_;
        mutex_type   recv_mutex_;

        // Pinned memory pool used for allocating buffers
        std::shared_ptr<rma::memory_pool<region_provider>> memory_pool_;

    public:
        // --------------------------------------------------------------------
        // constructor gets info from device and sets up all necessary
        // maps, queues and server endpoint etc
        controller(
            std::string const &provider,
            std::string const &domain,
            MPI_Comm mpi_comm, int rank, int size)
          : ep_tx_(nullptr)
          , ep_rx_(nullptr)
          , fabric_info_(nullptr)
          , fabric_(nullptr)
          , fabric_domain_(nullptr)
          , ep_passive_(nullptr)
          , event_queue_(nullptr)
            //
          , txcq_(nullptr)
          , rxcq_(nullptr)
          , av_(nullptr)
        {
            GHEX_DP_ONLY(cnt_deb, eval([](){ std::cout.setf(std::ios::unitbuf); }));
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            open_fabric(provider, domain, rank);

            // @TODO remove this set of flags, I think it's wrong here
            fabric_info_->tx_attr->op_flags |= FI_COMPLETION;
            fabric_info_->rx_attr->op_flags |= FI_COMPLETION;

            // Create a memory pool for pinned buffers
            memory_pool_ = rma::memory_pool<region_provider>::init_memory_pool(fabric_domain_);

            // initialize rma allocator with a pool
            ghex::tl::libfabric::rma::memory_region_allocator<unsigned char> allocator{};
            allocator.init_memory_pool(memory_pool_.get());
            libfabric_region_holder::memory_pool_ = memory_pool_.get();

            // setup an endpoint for receiving messages
            // rx endpoint is shared by all threads
            ep_rx_ = new_endpoint_active(fabric_domain_, fabric_info_);
            // create an address vector that will be bound to endpoints
            av_ = create_address_vector(fabric_info_, size);
            // once enabled we can get the address
            enable_endpoint(ep_rx_);
            here_ = get_endpoint_address(&ep_rx_->fid);

            // Broadcast address of all endpoints to all ranks
            // and fill address vector with info
            exchange_addresses(mpi_comm, rank, size);

            // bind address vector
            bind_address_vector_to_endpoint(ep_rx_, av_);
            // create a completion queue for the rx endpoint
            rxcq_ = create_completion_queue(fabric_domain_, fabric_info_->rx_attr->size);
            // bind CQ to endpoint
            bind_queue_to_endpoint(ep_rx_, rxcq_, FI_RECV);

            // create a completion queue for the tx endpoint
            txcq_ = create_completion_queue(fabric_domain_, fabric_info_->tx_attr->size);

#ifdef SEPARATE_RX_TX_ENDPOINTS
            // setup an endpoint for sending messages
            // this endpoint will be thread local in future
            ep_tx_ = new_endpoint_active(fabric_domain_, fabric_info_);
            enable_endpoint(ep_tx_);
            //
            bind_queue_to_endpoint(ep_tx_, txcq_, FI_SEND);
            bind_address_vector_to_endpoint(ep_tx_, av_);
#else
            bind_queue_to_endpoint(ep_rx_, txcq_, FI_TRANSMIT | FI_SEND);
# endif
        }

        // --------------------------------------------------------------------
        // clean up all resources
        ~controller()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            unsigned int messages_handled_ = 0;
            unsigned int rma_reads_        = 0;
            unsigned int recv_deletes_     = 0;

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("counters")
                , "Received messages" , hpx::debug::dec<>(messages_handled_)
                , "Total reads"       , hpx::debug::dec<>(rma_reads_)
                , "Total deletes"     , hpx::debug::dec<>(recv_deletes_)
                , "deletes error"     , hpx::debug::dec<>(messages_handled_ - recv_deletes_)));

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("closing"), "fabric"));
            if (fabric_)
                fi_close(&fabric_->fid);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("closing"), "ep_active"));
            if (ep_rx_)
                fi_close(&ep_rx_->fid);
#ifndef GHEX_LIBFABRIC_THREAD_LOCAL_ENDPOINTS
            if (ep_tx_)
                fi_close(&ep_tx_->fid);
#endif
            if (ep_passive_)
                fi_close(&ep_passive_->fid);

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("closing"), "event_queue"));
            if (event_queue_)
                fi_close(&event_queue_->fid);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("closing"), "fabric_domain"));
            if (fabric_domain_)
                fi_close(&fabric_domain_->fid);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("closing"), "ep_shared_rx_cxt"));

            // clean up
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("freeing fabric_info"));
            fi_freeinfo(fabric_info_));
        }

        // --------------------------------------------------------------------
        // send address to rank 0 and receive array of all localities
        void MPI_exchange_localities(MPI_Comm comm, int rank, int size)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            std::vector<char> localities(size*locality_defs::array_size, 0);
            //
            if (rank > 0)
            {
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("sending here")
                    , iplocality(here_)
                    , "size", locality_defs::array_size));
                /*int err = */MPI_Send(
                            here_.fabric_data(),
                            locality_defs::array_size,
                            MPI_CHAR,
                            0, // dst rank
                            0, // tag
                            comm);

                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("receiving all")
                    , "size", locality_defs::array_size));

                MPI_Status status;
                /*err = */MPI_Recv(
                            localities.data(),
                            size*locality_defs::array_size,
                            MPI_CHAR,
                            0, // src rank
                            0, // tag
                            comm,
                            &status);
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("received addresses")));
            }
            else {
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("receiving addresses")));
                memcpy(&localities[0], here_.fabric_data(), locality_defs::array_size);
                for (int i=1; i<size; ++i) {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("receiving address"), hpx::debug::dec<>(i)));
                    MPI_Status status;
                    /*int err = */MPI_Recv(
                                &localities[i*locality_defs::array_size],
                                size*locality_defs::array_size,
                                MPI_CHAR,
                                i, // src rank
                                0, // tag
                                comm,
                                &status);
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("received address"), hpx::debug::dec<>(i)));
                }

                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("sending all")));
                for (int i=1; i<size; ++i) {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("sending to"), hpx::debug::dec<>(i)));
                    /*int err = */MPI_Send(
                                &localities[0],
                                size*locality_defs::array_size,
                                MPI_CHAR,
                                i, // dst rank
                                0, // tag
                                comm);
                }
            }

            // all ranks should now have a full localities vector
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("populating vector")));
            for (int i=0; i<size; ++i) {
                locality temp;
                int offset = i*locality_defs::array_size;
                memcpy(temp.fabric_data_writable(), &localities[offset], locality_defs::array_size);
                insert_address(temp);
            }
        }

        // --------------------------------------------------------------------
        // initialize the basic fabric/domain/name
        void open_fabric(std::string const& provider, std::string const& domain, int rank)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            struct fi_info *fabric_hints_ = fi_allocinfo();
            if (!fabric_hints_) {
                throw fabric_error(-1, "Failed to allocate fabric hints");
            }

            here_ = locality("127.0.0.1", "7909");
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Here locality") , iplocality(here_)));

#if defined(GHEX_LIBFABRIC_SOCKETS) || defined(GHEX_LIBFABRIC_TCP)

            fabric_hints_->addr_format  = FI_SOCKADDR_IN;
            // this memory will (should) be deleted in hints destructor
            struct sockaddr_in *socket_data = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
            memcpy(socket_data, here_.fabric_data(), locality_defs::array_size);

            // If we are the root node, then create connection with the right port address
            if (rank == 0) {
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("root locality = src")
                              , iplocality(root_)));
                fabric_hints_->src_addr     = socket_data;
                fabric_hints_->src_addrlen  = sizeof(struct sockaddr_in);
            }
            else {
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("root locality = dest")
                              , iplocality(root_)));
                fabric_hints_->dest_addr    = socket_data;
                fabric_hints_->dest_addrlen = sizeof(struct sockaddr_in);
            }
#endif

            fabric_hints_->caps = FI_MSG | FI_TAGGED | FI_DIRECTED_RECV /*| FI_SOURCE*/;

            fabric_hints_->mode                   = FI_CONTEXT /*| FI_MR_LOCAL*/;
            if (provider.c_str()==std::string("tcp")) {
                fabric_hints_->fabric_attr->prov_name = strdup(std::string(provider + ";ofi_rxm").c_str());
            }
            else {
            fabric_hints_->fabric_attr->prov_name = strdup(provider.c_str());
            }
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("fabric provider")
                , fabric_hints_->fabric_attr->prov_name));
            if (domain.size()>0) {
                fabric_hints_->domain_attr->name  = strdup(domain.c_str());
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("fabric domain")
                    , fabric_hints_->domain_attr->name));
            }

            // use infiniband type basic registration for now
//            fabric_hints_->domain_attr->mr_mode = FI_MR_BASIC;
            fabric_hints_->domain_attr->mr_mode = FI_MR_SCALABLE;

            // Disable the use of progress threads
//            fabric_hints_->domain_attr->control_progress = FI_PROGRESS_MANUAL;
//            fabric_hints_->domain_attr->data_progress = FI_PROGRESS_MANUAL;
            fabric_hints_->domain_attr->control_progress = FI_PROGRESS_AUTO;
            fabric_hints_->domain_attr->data_progress = FI_PROGRESS_AUTO;

            // Enable thread safe mode (Does not work with psm2 provider)
            fabric_hints_->domain_attr->threading = FI_THREAD_SAFE;

            // Enable resource management
            fabric_hints_->domain_attr->resource_mgmt = FI_RM_ENABLED;

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("fabric endpoint"), "RDM"));
            fabric_hints_->ep_attr->type = FI_EP_RDM;

            // by default, we will always want completions on both tx/rx events
            fabric_hints_->tx_attr->op_flags = FI_COMPLETION;
            fabric_hints_->rx_attr->op_flags = FI_COMPLETION;

            uint64_t flags = 0;
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("get fabric info")
                          , "FI_VERSION"
                          , hpx::debug::dec(GHEX_LIBFABRIC_FI_VERSION_MAJOR)
                          , hpx::debug::dec(GHEX_LIBFABRIC_FI_VERSION_MINOR)));
            int ret = fi_getinfo(FI_VERSION(GHEX_LIBFABRIC_FI_VERSION_MAJOR, GHEX_LIBFABRIC_FI_VERSION_MINOR),
                nullptr, nullptr, flags, fabric_hints_, &fabric_info_);
            if (ret) throw fabric_error(ret, "Failed to get fabric info");

            GHEX_DP_ONLY(cnt_deb, trace(hpx::debug::str<>("Fabric info"), "\n"
                , fi_tostr(fabric_info_, FI_TYPE_INFO)));

            bool context = (fabric_hints_->mode & FI_CONTEXT)!=0;
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Requires FI_CONTEXT")
                , context));

            bool mrlocal = (fabric_hints_->mode & FI_MR_LOCAL)!=0;
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Requires FI_MR_LOCAL")
                , mrlocal));

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Creating fi_fabric")));
            ret = fi_fabric(fabric_info_->fabric_attr, &fabric_, nullptr);
            if (ret) throw fabric_error(ret, "Failed to get fi_fabric");

            // Allocate a domain.
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Allocating domain")));
            ret = fi_domain(fabric_, fabric_info_, &fabric_domain_, nullptr);
            if (ret) throw fabric_error(ret, "fi_domain");

#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
            // Enable use of udreg instead of internal MR cache
            ret = _set_check_domain_op_value(GNI_MR_CACHE, "udreg");

            // Experiments showed default value of 2048 too high if
            // launching multiple clients on one node
            int32_t udreg_limit = 1024;
            ret = gni_set_domain_op_value(
                fabric_domain_, GNI_MR_UDREG_REG_LIMIT, &udreg_limit);

        // Cray specific. Disable memory registration cache completely
        _set_disable_registration();
#endif
            fi_freeinfo(fabric_hints_);
        }

        // --------------------------------------------------------------------
        // Special GNI extensions to disable memory registration cache

        // this helper function only works for string ops
        void _set_check_domain_op_value([[maybe_unused]] int op, [[maybe_unused]] const char *value)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
            struct fi_gni_ops_domain *gni_domain_ops;
            char *get_val;
            int ret = fi_open_ops(&fabric_domain_->fid, FI_GNI_DOMAIN_OPS_1,
                      0, (void **) &gni_domain_ops, nullptr);
            if (ret) throw fabric_error(ret, "fi_open_ops");
            GHEX_DP_ONLY(cnt_deb, debug("domain ops returned " , hpx::debug::ptr(gni_domain_ops)));

            ret = gni_domain_ops->set_val(&fabric_domain_->fid,
                    (dom_ops_val_t)(op), &value);
            if (ret) throw fabric_error(ret, "set val (ops)");

            ret = gni_domain_ops->get_val(&fabric_domain_->fid,
                    (dom_ops_val_t)(op), &get_val);
            GHEX_DP_ONLY(cnt_deb, debug("Cache mode set to " , get_val));
            if (std::string(value) != std::string(get_val))
                throw fabric_error(ret, "get val");
#endif
        }

        void _set_disable_registration()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            _set_check_domain_op_value(GNI_MR_CACHE, "none");
#endif
        }

        // --------------------------------------------------------------------
        struct fid_ep *new_endpoint_active(struct fid_domain *domain, struct fi_info *info)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Got info mode")
                                , (info->mode & FI_NOTIFY_FLAGS_ONLY)));
            struct fid_ep *ep;
            int ret = fi_endpoint(domain, info, &ep, nullptr);
            if (ret) throw fabric_error(ret, "fi_endpoint");
            return ep;
        }

        // --------------------------------------------------------------------
        struct fid_ep *get_rx_endpoint()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            return ep_rx_;
        }

        // --------------------------------------------------------------------
        struct fid_ep *get_tx_endpoint()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
#ifdef SEPARATE_RX_TX_ENDPOINTS
#ifdef GHEX_LIBFABRIC_THREAD_LOCAL_ENDPOINTS
            if (ep_tx_ == nullptr) {
                ep_tx_ = new_endpoint_active(fabric_domain_, fabric_info_);
                bind_queue_to_endpoint(ep_tx_, txcq_, FI_SEND);
            }
#else
            assert (ep_tx_ != nullptr);
#endif
            return ep_tx_;
#else
            return ep_rx_;
#endif
        }

        // --------------------------------------------------------------------
        void bind_address_vector_to_endpoint(struct fid_ep *endpoint, struct fid_av *av)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Binding AV")));
            int ret = fi_ep_bind(endpoint, &av->fid, 0);
            if (ret) throw fabric_error(ret, "bind address_vector");
        }

        // --------------------------------------------------------------------
        void bind_queue_to_endpoint(struct fid_ep *endpoint,
                                    struct fid_cq *&cq,
                                    uint32_t cqtype)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Binding CQ")));
            int ret = fi_ep_bind(endpoint, &cq->fid, cqtype);
            if (ret) throw fabric_error(ret, "bind cq");
        }

        // --------------------------------------------------------------------
        void enable_endpoint(struct fid_ep *endpoint)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Enabling endpoint")
                , hpx::debug::ptr(endpoint)));
            int ret = fi_enable(endpoint);
            if (ret) throw fabric_error(ret, "fi_enable");
        }

        // --------------------------------------------------------------------
        locality get_endpoint_address(struct fid *id)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            locality::locality_data local_addr;
            std::size_t addrlen = locality_defs::array_size;
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Get address : size")
                                        , hpx::debug::dec<>(addrlen)));
            int ret = fi_getname(id, local_addr.data(), &addrlen);
            if (ret || (addrlen>locality_defs::array_size)) {
                fabric_error(ret, "fi_getname - size error or other problem");
            }

            // optimized out when debug logging is false
            if (cnt_deb.is_enabled())
            {
                std::stringstream temp1;
                for (std::size_t i=0; i<locality_defs::array_length; ++i) {
                    temp1 << hpx::debug::ipaddr(&local_addr[i]) << " - ";
                }
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("raw address data")
                                            , temp1.str().c_str()));
                std::stringstream temp2;
                for (std::size_t i=0; i<locality_defs::array_length; ++i) {
                    temp2 << hpx::debug::hex<8>(local_addr[i]) << " - ";
                }
                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("raw address data")
                                            , temp2.str().c_str()));
            }
            return locality(local_addr);
        }

        // --------------------------------------------------------------------
        fid_pep* create_passive_endpoint(struct fid_fabric *fabric, struct fi_info *info)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            struct fid_pep *ep;
            int ret = fi_passive_ep(fabric, info, &ep, nullptr);
            if (ret) {
                throw fabric_error(ret, "Failed to create fi_passive_ep");
                }
            return ep;
        }

        // --------------------------------------------------------------------
        // if we did not bootstrap, then fetch the list of all localities
        // from agas and insert each one into the address vector
        void exchange_addresses(MPI_Comm comm, int rank, int size)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("initialize_localities")
                , size , "localities"));

            MPI_exchange_localities(comm, rank, size);
            debug_print_av_vector(size);
            GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Done localities")));
        }

        // --------------------------------------------------------------------
        const locality & here() const { return here_; }

        // --------------------------------------------------------------------
        // returns true when all connections have been disconnected and none are active
        bool isTerminated() {
            return false;
            //return (qp_endpoint_map_.size() == 0);
        }

        // types we need for connection and disconnection callback functions
        // into the main parcelport code.
        typedef std::function<void(fid_ep *endpoint, uint32_t ipaddr)>
            ConnectionFunction;
        typedef std::function<void(fid_ep *endpoint, uint32_t ipaddr)>
            DisconnectionFunction;

        // --------------------------------------------------------------------
        void debug_print_av_vector(std::size_t N)
        {
            libfabric::locality addr;
            std::size_t addrlen = libfabric::locality_defs::array_size;
            for (std::size_t i=0; i<N; ++i) {
                int ret = fi_av_lookup(av_, fi_addr_t(i),
                                       addr.fabric_data_writable(), &addrlen);
                addr.set_fi_address(fi_addr_t(i));
                if ((ret == 0) && (addrlen==libfabric::locality_defs::array_size)) {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("address vector")
                        , hpx::debug::dec<3>(i), iplocality(addr)));

                }
                else {
                    throw std::runtime_error("debug_print_av_vector : address vector traversal failure");
                }
            }
        }

        // --------------------------------------------------------------------
        gridtools::ghex::tl::cb::progress_status poll_for_work_completions()
        {
            bool retry = true;
            int sends = 0;
            int recvs = 0;
            while (retry) {
                progress_count prog_s = poll_send_queue();
                progress_count prog_r = poll_recv_queue();
                sends += prog_s.user_msgs;
                recvs += prog_r.user_msgs;
                // we always retry until no new completion events are
                // found. this helps progress all messages
                retry = prog_s.completions_handled || prog_r.completions_handled;
            }
            return gridtools::ghex::tl::cb::progress_status{sends, recvs, 0};
        }

        // --------------------------------------------------------------------
        progress_count poll_send_queue()
        {
            const int MAX_SEND_COMPLETIONS = 1;
            int ret;
            fi_cq_msg_entry entry[MAX_SEND_COMPLETIONS];
            // create a scoped block for the lock
            {
                std::unique_lock<mutex_type> lock(send_mutex_, std::try_to_lock_t{});
                // if another thread is polling now, just exit
                if (!lock.owns_lock()) {
                    return progress_count{false, 0};
                }

                static auto polling = cnt_deb.make_timer(1
                        , hpx::debug::str<>("poll send queue"));
                cnt_deb.timed(polling);

                // poll for completions
                {
                    ret = fi_cq_read(txcq_, &entry[0], MAX_SEND_COMPLETIONS);
                }
                // if there is an error, retrieve it
                if (ret == -FI_EAVAIL) {
                    struct fi_cq_err_entry e = {};
                    int err_sz = fi_cq_readerr(txcq_, &e ,0);
                    HPX_UNUSED(err_sz);

                    // flags might not be set correctly
                    if (e.flags == (FI_MSG | FI_SEND)) {
                        cnt_deb.error("txcq Error FI_EAVAIL for FI_SEND with len" , hpx::debug::hex<6>(e.len)
                            , "context" , hpx::debug::ptr(e.op_context));
                    }
                    if (e.flags & FI_RMA) {
                        cnt_deb.error("txcq Error FI_EAVAIL for FI_RMA with len" , hpx::debug::hex<6>(e.len)
                            , "context" , hpx::debug::ptr(e.op_context));
                    }
                    context_info* handler = reinterpret_cast<context_info*>(e.op_context);
                    handler->handle_error(e);
                    return progress_count{false, 0};
                }
            }
            //
            // release the lock and process each completion
            //
            if (ret>0) {
                progress_count processed{true, 0};
                for (int i=0; i<ret; ++i) {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                        , "txcq wr_id"
                        , fi_tostr(&entry[i].flags, FI_TYPE_OP_FLAGS)
                        , "(" , hpx::debug::dec<>(entry[i].flags) , ")"
                        , "context" , hpx::debug::ptr(entry[i].op_context)
                        , "length" , hpx::debug::hex<6>(entry[i].len)));
                    if (entry[i].flags == (FI_TAGGED | FI_MSG | FI_SEND)) {
                        GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                            , "txcq MSG tagged send completion"
                            , hpx::debug::ptr(entry[i].op_context)));
                        context_info* handler = reinterpret_cast<context_info*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_send_completion();
                    }
                    else if (entry[i].flags == (FI_MSG | FI_SEND)) {
                        GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                            , "txcq MSG send completion"
                            , hpx::debug::ptr(entry[i].op_context)));
                        context_info* handler = reinterpret_cast<context_info*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_send_completion();
                    }
                    else {
                        cnt_deb.error("Received an unknown txcq completion"
                            , hpx::debug::dec<>(entry[i].flags)
                            , hpx::debug::bin<64>(entry[i].flags));
                        std::terminate();
                    }
                }
                return processed;
            }
            else if (ret==0 || ret==-FI_EAGAIN) {
                // do nothing, we will try again on the next check
            }
            else {
                cnt_deb.error("unknown error in completion txcq read");
            }
            return progress_count{false, 0};
        }

        // --------------------------------------------------------------------
        progress_count poll_recv_queue()
        {
            const int MAX_RECV_COMPLETIONS = 1;
            int ret;
            fi_cq_msg_entry entry[MAX_RECV_COMPLETIONS];
            // create a scoped block for the lock
            {
                std::unique_lock<mutex_type> lock(recv_mutex_, std::try_to_lock_t{});
                // if another thread is polling now, just exit
                if (!lock.owns_lock()) {
                    return progress_count{false, 0};
                }

                static auto polling = cnt_deb.make_timer(1
                        , hpx::debug::str<>("poll recv queue"));
                cnt_deb.timed(polling);

                // poll for completions
                {
                    ret = fi_cq_read(rxcq_, &entry[0], MAX_RECV_COMPLETIONS);
                }
                // if there is an error, retrieve it
                if (ret == -FI_EAVAIL) {
                    // read the full error status
                    struct fi_cq_err_entry e = {};
                    int err_sz = fi_cq_readerr(rxcq_, &e ,0);
                    HPX_UNUSED(err_sz);
                    // from the manpage 'man 3 fi_cq_readerr'
                    if (e.err==FI_ECANCELED) {
                        GHEX_DP_ONLY(cnt_deb, debug("rxcq Cancelled "
                                      , "flags"   , hpx::debug::hex<6>(e.flags)
                                      , "len"     , hpx::debug::hex<6>(e.len)
                                      , "context" , hpx::debug::ptr(e.op_context)));
                        // this seems to be faulty, but we don't need to do anything
                        // so for now, do not call cancel handler as the message
                        // was cleaned up when cancel was called if it was successful
                        //reinterpret_cast<receiver *>
                        //        (entry.op_context)->handle_cancel();
                    }
                    else {
                        cnt_deb.error("rxcq Error ??? "
                                      , "err"     , hpx::debug::dec<>(-e.err)
                                      , "flags"   , hpx::debug::hex<6>(e.flags)
                                      , "len"     , hpx::debug::hex<6>(e.len)
                                      , "context" , hpx::debug::ptr(e.op_context)
                                      , "error"   ,
                        fi_cq_strerror(rxcq_, e.prov_errno, e.err_data, (char*)e.buf, e.len));
                        std::terminate();
                    }
                    return progress_count{false, 0};
                }
            }
            //
            // release the lock and process each completion
            //
            if (ret>0) {
                progress_count processed{true, 0};
                for (int i=0; i<ret; ++i) {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                        , "rxcq wr_id"
                        , fi_tostr(&entry[i].flags, FI_TYPE_OP_FLAGS)
                        , "(" , hpx::debug::dec<>(entry[i].flags) , ")"
                        , "context" , hpx::debug::ptr(entry[i].op_context)
                        , "length"  , hpx::debug::hex<6>(entry[i].len)));
                    if (entry[i].flags == (FI_TAGGED | FI_MSG | FI_RECV)) {
                        GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                            , "rxcq MSG tagged recv completion"
                            , hpx::debug::ptr(entry[i].op_context)));
                        context_info* handler = reinterpret_cast<context_info*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_recv_completion(entry[i].len);
                    }
                    else if (entry[i].flags == (FI_MSG | FI_RECV)) {
                        GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Completion")
                            , "rxcq MSG recv completion"
                            , hpx::debug::ptr(entry[i].op_context)));
                        context_info* handler = reinterpret_cast<context_info*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_recv_completion(entry[i].len);
                    }
                    else {
                        cnt_deb.error("Received an unknown rxcq completion"
                            , hpx::debug::dec<>(entry[i].flags)
                            , hpx::debug::bin<64>(entry[i].flags));
                        std::terminate();
                    }
                }
                return processed;
            }
            else if (ret==0 || ret==-FI_EAGAIN) {
                // do nothing, we will try again on the next check
            }
            else {
                cnt_deb.error("unknown error in completion rxcq read");
            }
            return progress_count{false, 0};
        }

        // --------------------------------------------------------------------
        inline struct fid_domain * get_domain() {
            return fabric_domain_;
        }

        // --------------------------------------------------------------------
        inline std::shared_ptr<rma::memory_pool<region_provider>> get_memory_pool() {
            return memory_pool_;
        };

        // --------------------------------------------------------------------
        inline rma::memory_pool<region_provider>& get_memory_pool_ptr() {
            return *memory_pool_;
        }

        // --------------------------------------------------------------------
        struct fid_cq *create_completion_queue(struct fid_domain *domain, size_t size)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            struct fid_cq *cq;
            fi_cq_attr cq_attr = {};
                cq_attr.format = FI_CQ_FORMAT_MSG;
            cq_attr.wait_obj = FI_WAIT_NONE;
            cq_attr.size     = size;
            cq_attr.flags    = 0 /*FI_COMPLETION*/;
            // open completion queue on fabric domain and set context to null
            int ret = fi_cq_open(domain, &cq_attr, &cq, &cq);
            if (ret) throw fabric_error(ret, "fi_cq_open");
            return cq;
        }

        // --------------------------------------------------------------------
        fid_av* create_address_vector(struct fi_info *info, int N)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            fid_av *av;
            fi_av_attr av_attr = {};
                if (info->domain_attr->av_type != FI_AV_UNSPEC)
                    av_attr.type = info->domain_attr->av_type;
                else {
                    GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("map FI_AV_TABLE")));
                    av_attr.type  = FI_AV_TABLE;
                    av_attr.count = N;
                }

                GHEX_DP_ONLY(cnt_deb, debug(hpx::debug::str<>("Creating AV")));
            int ret = fi_av_open(fabric_domain_, &av_attr, &av, nullptr);
                if (ret) throw fabric_error(ret, "fi_av_open");
            return av;
        }

        // --------------------------------------------------------------------
        libfabric::locality insert_address(const libfabric::locality &address)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            GHEX_DP_ONLY(cnt_deb, trace(hpx::debug::str<>("inserting AV"), iplocality(address)));
            fi_addr_t fi_addr = 0xffffffff;
            int ret = fi_av_insert(av_, address.fabric_data(), 1, &fi_addr, 0, nullptr);
            if (ret < 0) {
                throw fabric_error(ret, "fi_av_insert");
            }
            else if (ret == 0) {
                cnt_deb.error("fi_av_insert called with existing address");
                fabric_error(ret, "fi_av_insert did not return 1");
            }
            // address was generated correctly, now update the locality with the fi_addr
            libfabric::locality new_locality(address, fi_addr);
            GHEX_DP_ONLY(cnt_deb, trace(hpx::debug::str<>("AV add")
                          , "rank" , hpx::debug::dec<>(fi_addr)
                          , iplocality(new_locality)
                          , "fi_addr" , hpx::debug::hex<4>(fi_addr)));
            return new_locality;
        }
    };

}}}}

#endif
