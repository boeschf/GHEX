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
#include <ghex/transport_layer/libfabric/receiver.hpp>
#include <ghex/transport_layer/libfabric/sender.hpp>
#include <ghex/transport_layer/libfabric/rma/detail/memory_region_allocator.hpp>
//
#include <mpi.h>

// ------------------------------------------------
// underlying provider used by libfabric
// ------------------------------------------------
//#define GHEX_LIBFABRIC_PROVIDER_GNI 1
//#define GHEX_LIBFABRIC_PROVIDER_SOCKETS

// ------------------------------------------------
// if we have PMI we can bootstrap
// ------------------------------------------------
//#define GHEX_LIBFABRIC_HAVE_PMI 1

// ------------------------------------------------
// implement bootstrapping directly using libfabric
// currently only valid with sockets provider
// ------------------------------------------------
// #define GHEX_LIBFABRIC_HAVE_BOOTSTRAPPING

// ------------------------------------------------
// Reliable Data Endpoint type
// ------------------------------------------------
#define GHEX_LIBFABRIC_ENDPOINT_RDM

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
    static hpx::debug::enable_print<false> cnt_deb("CONTROL");
}}

namespace gridtools {
namespace ghex {
namespace tl {
namespace libfabric
{

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

        struct fi_info    *fabric_info_;
        struct fid_fabric *fabric_;
        struct fid_domain *fabric_domain_;
        // Server/Listener for RDMA connections.
        struct fid_ep     *ep_active_;
        struct fid_ep     *ep_shared_rx_cxt_;

        // we will use just one event queue for all connections
        struct fid_eq     *event_queue_;
        struct fid_cq     *txcq_, *rxcq_;
        struct fid_av     *av_;

        bool                       immediate_;
        std::atomic<std::uint32_t> bootstrap_counter_;

        // We must maintain a list of receivers that are being used
        using expected_receiver_stack = std::stack<receiver*>;

        static expected_receiver_stack thread_local expected_receivers_;
        std::atomic<unsigned int> expected_receivers_in_use_;

        locality here_;
        locality root_;

        // store info about local device
        std::string  device_;
        std::string  interface_;
        sockaddr_in  local_addr_;

        // Pinned memory pool used for allocating buffers
        std::unique_ptr<rma::memory_pool<libfabric_region_provider>> memory_pool_;

        // used during queue creation setup and during polling
        mutex_type   controller_mutex_;
        mutex_type   send_mutex_;
        mutex_type   recv_mutex_;

        // --------------------------------------------------------------------
        // constructor gets info from device and sets up all necessary
        // maps, queues and server endpoint etc
        controller(
            std::string const &provider,
            std::string const &domain,
            MPI_Comm mpi_comm, int rank, int size)
          : fabric_info_(nullptr)
          , fabric_(nullptr)
          , fabric_domain_(nullptr)
          , ep_active_(nullptr)
          , ep_shared_rx_cxt_(nullptr)
          , event_queue_(nullptr)
            //
          , txcq_(nullptr)
          , rxcq_(nullptr)
          , av_(nullptr)
            //
          , immediate_(false)
        {
            std::cout.setf(std::ios::unitbuf);
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            open_fabric(provider, domain, rank);

            // setup a passive listener, or an active RDM endpoint
            here_ = create_local_endpoint();
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Overriding here") , iplocality(here_)));

            // Create a memory pool for pinned buffers
            memory_pool_.reset(
                new rma::memory_pool<libfabric_region_provider>(fabric_domain_));

            // initialize rma allocator with a pool
            ghex::tl::libfabric::rma::memory_region_allocator<unsigned char> allocator{};
            allocator.init_memory_pool(memory_pool_.get());
            libfabric_region_holder::memory_pool_ = memory_pool_.get();

            initialize_localities(mpi_comm, rank, size);

#if defined(GHEX_LIBFABRIC_HAVE_BOOTSTRAPPING)
            if (bootstrap) {
#if defined(GHEX_LIBFABRIC_PROVIDER_SOCKETS)
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Calling boot SOCKETS")));
                boot_SOCKETS();
#elif defined(GHEX_LIBFABRIC_HAVE_PMI)
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug("Calling boot PMI"));
                boot_PMI();
# endif
                if (root_ == here_) {
                    std::cout << "Libfabric Parcelport boot-step complete" << std::endl;
                }
            }
#endif
            startup();
        }

        // --------------------------------------------------------------------
        // clean up all resources
        ~controller()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            unsigned int messages_handled_ = 0;
            unsigned int rma_reads_        = 0;
            unsigned int recv_deletes_     = 0;

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("counters")
                , "Received messages" , hpx::debug::dec<>(messages_handled_)
                , "Total reads"       , hpx::debug::dec<>(rma_reads_)
                , "Total deletes"     , hpx::debug::dec<>(recv_deletes_)
                , "deletes error"     , hpx::debug::dec<>(messages_handled_ - recv_deletes_)));

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("closing"), "fabric"));
            if (fabric_)
                fi_close(&fabric_->fid);
#ifdef GHEX_LIBFABRIC_ENDPOINT_RDM
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("closing"), "ep_active"));
            if (ep_active_)
                fi_close(&ep_active_->fid);
#endif
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("closing"), "event_queue"));
            if (event_queue_)
                fi_close(&event_queue_->fid);
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("closing"), "fabric_domain"));
            if (fabric_domain_)
                fi_close(&fabric_domain_->fid);
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("closing"), "ep_shared_rx_cxt"));
            if (ep_shared_rx_cxt_)
                fi_close(&ep_shared_rx_cxt_->fid);
            // clean up
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("freeing fabric_info"));
            fi_freeinfo(fabric_info_));
        }

        // --------------------------------------------------------------------
        // boot all ranks when using libfabric sockets provider
        void boot_SOCKETS()
        {
#ifdef GHEX_LIBFABRIC_PROVIDER_SOCKETS
            // we expect N-1 localities to connect to us during bootstrap
            std::size_t N = get_num_localities();
            bootstrap_counter_ = N-1;

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Bootstrap")
                , N , " localities"));

            // create address vector and queues we need if bootstrapping
            create_completion_queues(fabric_info_, N);

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("inserting root addr")
                          , iplocality(root_)));
            root_ = insert_address(root_);
#endif
        }

        // --------------------------------------------------------------------
        // send rank 0 address to root and receive array of localities
        void MPI_exchange_localities(MPI_Comm comm, int rank, int size)
        {
            std::vector<char> localities(size*locality_defs::array_size, 0);
            //
            if (rank > 0)
            {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("sending here")
                    , iplocality(here_)
                    , "size", locality_defs::array_size));
                /*int err = */MPI_Send(
                            here_.fabric_data(),
                            locality_defs::array_size,
                            MPI_CHAR,
                            0, // dst rank
                            0, // tag
                            comm);

                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("receiving all")
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
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("received addresses")));
            }
            else {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("receiving addresses")));
                memcpy(&localities[0], here_.fabric_data(), locality_defs::array_size);
                for (int i=1; i<size; ++i) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("receiving address"), hpx::debug::dec<>(i)));
                    MPI_Status status;
                    /*int err = */MPI_Recv(
                                &localities[i*locality_defs::array_size],
                                size*locality_defs::array_size,
                                MPI_CHAR,
                                i, // src rank
                                0, // tag
                                comm,
                                &status);
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("received address"), hpx::debug::dec<>(i)));
                }

                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("sending all")));
                for (int i=1; i<size; ++i) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("sending to"), hpx::debug::dec<>(i)));
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
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("populating vector")));
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
            // we require message and RMA support, so ask for them
            // we also want receives to carry source address info
            here_ = locality("127.0.0.1", "7909");
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Here locality") , iplocality(here_)));

//#ifdef GHEX_LIBFABRIC_HAVE_BOOTSTRAPPING
#if defined(GHEX_LIBFABRIC_SOCKETS)
            // If we are the root node, then create connection with the right port address
            if (rank == 0) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("root locality = src")
                              , iplocality(root_)));
                // this memory will (should) be deleted in hints destructor                
                struct sockaddr_in *socket_data1 = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
                memcpy(socket_data1, here_.fabric_data(), locality_defs::array_size);
                fabric_hints_->addr_format  = FI_SOCKADDR_IN;
                fabric_hints_->src_addr     = socket_data1;
                fabric_hints_->src_addrlen  = sizeof(struct sockaddr_in);
            }
            else {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("root locality = dest")
                              , iplocality(root_)));
                // this memory will (should) be deleted in hints destructor
                struct sockaddr_in *socket_data2 = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
                memcpy(socket_data2, here_.fabric_data(), locality_defs::array_size);
                fabric_hints_->addr_format  = FI_SOCKADDR_IN;
                fabric_hints_->dest_addr    = socket_data2;
                fabric_hints_->dest_addrlen = sizeof(struct sockaddr_in);
            }
#endif
            //
            fabric_hints_->caps        = FI_MSG | FI_RMA /*| FI_SOURCE*/ | /*FI_SOURCE_ERR |*/
                FI_WRITE | FI_READ | FI_REMOTE_READ | FI_REMOTE_WRITE | FI_RMA_EVENT |
                FI_DIRECTED_RECV | FI_TAGGED;

            fabric_hints_->mode                   = FI_CONTEXT /*| FI_MR_LOCAL*/;
            fabric_hints_->fabric_attr->prov_name = strdup(provider.c_str());
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("fabric provider")
                , fabric_hints_->fabric_attr->prov_name));
            if (domain.size()>0) {
                fabric_hints_->domain_attr->name  = strdup(domain.c_str());
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("fabric domain")
                    , fabric_hints_->domain_attr->name));
            }

            // use infiniband type basic registration for now
            fabric_hints_->domain_attr->mr_mode = FI_MR_BASIC;
//            fabric_hints_->domain_attr->mr_mode = FI_MR_SCALABLE;

            // Disable the use of progress threads
            fabric_hints_->domain_attr->control_progress = FI_PROGRESS_MANUAL;
            fabric_hints_->domain_attr->data_progress = FI_PROGRESS_MANUAL;

            // Enable thread safe mode (Does not work with psm2 provider)
            fabric_hints_->domain_attr->threading = FI_THREAD_SAFE;

            // Enable resource management
            fabric_hints_->domain_attr->resource_mgmt = FI_RM_ENABLED;

#ifdef GHEX_LIBFABRIC_ENDPOINT_RDM
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("fabric endpoint"), "RDM"));
            fabric_hints_->ep_attr->type = FI_EP_RDM;
#endif

            // by default, we will always want completions on both tx/rx events
            fabric_hints_->tx_attr->op_flags = FI_COMPLETION;
            fabric_hints_->rx_attr->op_flags = FI_COMPLETION;

            uint64_t flags = 0;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("get fabric info")
                          , "FI_VERSION"
                          , hpx::debug::dec(GHEX_LIBFABRIC_FI_VERSION_MAJOR)
                          , hpx::debug::dec(GHEX_LIBFABRIC_FI_VERSION_MINOR)));
            int ret = fi_getinfo(FI_VERSION(GHEX_LIBFABRIC_FI_VERSION_MAJOR, GHEX_LIBFABRIC_FI_VERSION_MINOR),
                nullptr, nullptr, flags, fabric_hints_, &fabric_info_);
            if (ret) {
                throw fabric_error(ret, "Failed to get fabric info");
            }
            cnt_deb.trace(hpx::debug::str<>("Fabric info"), "\n"
                , fi_tostr(fabric_info_, FI_TYPE_INFO));

            immediate_ = (fabric_info_->rx_attr->mode & FI_RX_CQ_DATA)!=0;
            cnt_deb.trace(hpx::debug::str<>("Fabric IMMEDIATE")
                , immediate_);
//            bool context = cnt_deb.declare_variable<bool>(
//                        fabric_hints_->mode & FI_CONTEXT);
            bool context = (fabric_hints_->mode & FI_CONTEXT)!=0;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Requires FI_CONTEXT")
                , context));

            bool mrlocal = (fabric_hints_->mode & FI_MR_LOCAL)!=0;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Requires FI_MR_LOCAL")
                , mrlocal));

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Creating fi_fabric")));
            ret = fi_fabric(fabric_info_->fabric_attr, &fabric_, nullptr);
            if (ret) {
                throw fabric_error(ret, "Failed to get fi_fabric");
            }

            // Allocate a domain.
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Allocating domain")));
            ret = fi_domain(fabric_, fabric_info_, &fabric_domain_, nullptr);
            if (ret) throw fabric_error(ret, "fi_domain");

//            // Enable use of udreg instead of internal MR cache
//            ret = _set_check_domain_op_value(GNI_MR_CACHE, "udreg");

//            // Experiments showed default value of 2048 too high if
//            // launching multiple clients on one node
//            int32_t udreg_limit = 1024;
//            ret = gni_set_domain_op_value(
//                fabric_domain_, GNI_MR_UDREG_REG_LIMIT, &udreg_limit);

        // Cray specific. Disable memory registration cache completely
        _set_disable_registration();

            fi_freeinfo(fabric_hints_);
        }

        // -------------------------------------------------------------------
        // create endpoint and get ready for possible communications
        void startup()
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            // only allow one thread to startup the controller
            static std::atomic_flag initialized = ATOMIC_FLAG_INIT;
            if (initialized.test_and_set()) return;
#ifdef GHEX_LIBFABRIC_ENDPOINT_RDM
            bind_endpoint_to_queues(ep_active_);
#endif

            for (std::size_t i = 0; i < 0; ++i)
            {
                receiver *rcv = new_expected_receiver();
                // put the new receiver on the free list
                expected_receivers_.push(rcv);
            }
        }

        receiver *new_expected_receiver()
        {
            // after a receiver has been used, it's postprocess handler
            // is called, this returns it to the free list when possible
            auto handler = [this](receiver* rcv)
                {
//                    --expected_receivers_in_use_;
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("recycle")
                                  , "expected_receiver"
                                  , hpx::debug::ptr(rcv)
                                  /*, hpx::debug::dec<>(expected_receivers_in_use_)*/));
                    expected_receivers_.push(rcv);
                };
            //
            receiver *rcv = new receiver(ep_active_, *memory_pool_, std::move(handler));
            return rcv;
        }


        // --------------------------------------------------------------------
        // return a receiver object
        // --------------------------------------------------------------------
        receiver* get_expected_receiver(int rank)
        {
            receiver* rcv = nullptr;
            if (expected_receivers_.empty())
            {
                rcv = new_expected_receiver();
            }
            else {
                rcv = expected_receivers_.top();
                expected_receivers_.pop();
            }
//            ++expected_receivers_in_use_;

            // debug info only
            if (cnt_deb.is_enabled()) {
                libfabric::locality addr;
                addr.set_fi_address(fi_addr_t(rank));
                std::size_t addrlen = libfabric::locality_defs::array_size;
                int ret = fi_av_lookup(av_, fi_addr_t(rank),
                                       addr.fabric_data_writable(), &addrlen);
                if ((ret == 0) && (addrlen==libfabric::locality_defs::array_size)) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("get_expected_receiver")
                        , hpx::debug::ptr(rcv)
                        , "here", iplocality(here_)
                        , "from", iplocality(addr)
                        , "src rank", hpx::debug::hex<4>(rank)
                        , "fi_addr (rank)" , hpx::debug::hex<4>(fi_addr_t(rank))
                        , "receivers in use (++ get_expected_receiver)"
                        /*, hpx::debug::dec<>(expected_receivers_in_use_)*/));
                }
                else {
                    throw std::runtime_error("get_expected_receiver : address vector traversal failure");
                }
            }

            // set the receiver source address/offset in AV table
            rcv->src_addr_            = fi_addr_t(rank);
            return rcv;
        }

        // --------------------------------------------------------------------
        // Special GNI extensions to disable memory registration cache

        // this helper function only works for string ops
        void _set_check_domain_op_value([[maybe_unused]] int op, [[maybe_unused]] const char *value)
        {
#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
            struct fi_gni_ops_domain *gni_domain_ops;
            char *get_val;
            int ret = fi_open_ops(&fabric_domain_->fid, FI_GNI_DOMAIN_OPS_1,
                      0, (void **) &gni_domain_ops, nullptr);
            if (ret) throw fabric_error(ret, "fi_open_ops");
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug("domain ops returned " , hpx::debug::ptr(gni_domain_ops)));

            ret = gni_domain_ops->set_val(&fabric_domain_->fid,
                    (dom_ops_val_t)(op), &value);
            if (ret) throw fabric_error(ret, "set val (ops)");

            ret = gni_domain_ops->get_val(&fabric_domain_->fid,
                    (dom_ops_val_t)(op), &get_val);
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug("Cache mode set to " , get_val));
            if (std::string(value) != std::string(get_val))
                throw fabric_error(ret, "get val");
#endif
        }

        void _set_disable_registration()
        {
#ifdef GHEX_LIBFABRIC_PROVIDER_GNI
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            _set_check_domain_op_value(GNI_MR_CACHE, "none");
#endif
        }

        // --------------------------------------------------------------------
        locality create_local_endpoint()
        {
            struct fid *id;
            int ret;
#ifdef GHEX_LIBFABRIC_ENDPOINT_RDM
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("active endpoint")));
            new_endpoint_active(fabric_info_, &ep_active_);
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("active endpoint") , hpx::debug::ptr(ep_active_)));
            id = &ep_active_->fid;
#endif

#if !defined(GHEX_LIBFABRIC_HAVE_BOOTSTRAPPING)
            // with tcp we do not use PMI boot, so enable the endpoint now
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Enabling (ep_active)") , hpx::debug::ptr(ep_active_)));
            ret = fi_enable(ep_active_);
            if (ret) throw fabric_error(ret, "fi_enable");
#endif

            locality::locality_data local_addr;
            std::size_t addrlen = locality_defs::array_size;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Get address : size") , hpx::debug::dec<>(addrlen)));
            ret = fi_getname(id, local_addr.data(), &addrlen);
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
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("raw address data") , temp1.str().c_str()));
                std::stringstream temp2;
                for (std::size_t i=0; i<locality_defs::array_length; ++i) {
                    temp2 << hpx::debug::hex<8>(local_addr[i]) << " - ";
                }
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("raw address data") , temp2.str().c_str()));
            };
            return locality(local_addr);
        }

        // --------------------------------------------------------------------
        void new_endpoint_active(struct fi_info *info, struct fid_ep **new_endpoint)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            // create an 'active' endpoint that can be used for sending/receiving
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Active endpoint")));
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Got info mode") , (info->mode & FI_NOTIFY_FLAGS_ONLY)));
            int ret = fi_endpoint(fabric_domain_, info, new_endpoint, nullptr);
            if (ret) throw fabric_error(ret, "fi_endpoint");

            if (info->ep_attr->type == FI_EP_MSG) {
                if (event_queue_) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Binding endpoint to EQ")));
                    ret = fi_ep_bind(*new_endpoint, &event_queue_->fid, 0);
                    if (ret) throw fabric_error(ret, "bind event_queue_");
                }
            }
        }

        // --------------------------------------------------------------------
        void bind_endpoint_to_queues(struct fid_ep *endpoint)
        {
            int ret;
            if (av_) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Binding AV")));
                ret = fi_ep_bind(endpoint, &av_->fid, 0);
                if (ret) throw fabric_error(ret, "bind event_queue_");
            }

            if (txcq_) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Binding TX CQ")));
                ret = fi_ep_bind(endpoint, &txcq_->fid, FI_TRANSMIT);
                if (ret) throw fabric_error(ret, "bind txcq");
            }

            if (rxcq_) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Binding RX CQ")));
                ret = fi_ep_bind(endpoint, &rxcq_->fid, FI_RECV);
                if (ret) throw fabric_error(ret, "rxcq");
            }

            if (ep_shared_rx_cxt_) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Binding RX context")));
                ret = fi_ep_bind(endpoint, &ep_shared_rx_cxt_->fid, 0);
                if (ret) throw fabric_error(ret, "ep_shared_rx_cxt_");
            }

            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Enabling endpoint")
                , hpx::debug::ptr(endpoint)));
            ret = fi_enable(endpoint);
            if (ret) throw fabric_error(ret, "fi_enable");
        }

        // --------------------------------------------------------------------
        // if we did not bootstrap, then fetch the list of all localities
        // from agas and insert each one into the address vector
        void initialize_localities(MPI_Comm comm, int rank, int size)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
#ifndef GHEX_LIBFABRIC_HAVE_BOOTSTRAPPING
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("initialize_localities")
                , size , "localities"));

            // make sure address vector is created
            create_completion_queues(fabric_info_, size);

            MPI_exchange_localities(comm, rank, size);
            debug_print_av_vector(size);
#endif
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Done localities")));
        }

        // --------------------------------------------------------------------
        const locality & here() const { return here_; }

        // --------------------------------------------------------------------
        const bool & immedate_data_supported() const { return immediate_; }

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
        void debug_print_av_vector(int N)
        {
            libfabric::locality addr;
            std::size_t addrlen = libfabric::locality_defs::array_size;
            for (std::size_t i=0; i<N; ++i) {
                int ret = fi_av_lookup(av_, fi_addr_t(i),
                                       addr.fabric_data_writable(), &addrlen);
                addr.set_fi_address(fi_addr_t(i));
                if ((ret == 0) && (addrlen==libfabric::locality_defs::array_size)) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("address vector")
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
/*
        struct progress {
            bool completions_handled = false;
            std::uint32_t user_msgs = 0;
        };
*/
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
                    rma_base *base = reinterpret_cast<rma_base*>(e.op_context);
                    switch (base->context_type()) {
                        case ctx_sender:
                            reinterpret_cast<sender*>(e.op_context)->handle_error(e);
                            break;
                        case ctx_receiver:
                            // this can never happen on a txcq
                            reinterpret_cast<receiver*>(e.op_context)->handle_error(e);
                            break;
                    }
                    return progress_count{false, 0};
                }
            }

            //
            // release the lock and process each completion
            //
//                static auto polling = cnt_deb.make_timer(1
//                        , hpx::debug::str<>("poll send queue"));
//                cnt_deb.timed(polling);
            //
            if (ret>0) {
                progress_count processed{true, 0};
                for (int i=0; i<ret; ++i) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Completion")
                        , "txcq wr_id"
                        , fi_tostr(&entry[i].flags, FI_TYPE_OP_FLAGS)
                        , "(" , hpx::debug::dec<>(entry[i].flags) , ")"
                        , "context" , hpx::debug::ptr(entry[i].op_context)
                        , "length" , hpx::debug::hex<6>(entry[i].len)));
                    if (entry[i].flags == (FI_TAGGED | FI_MSG | FI_SEND)) {
                        GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Completion")
                            , "txcq MSG tagged send completion"));
                        sender* handler = reinterpret_cast<sender*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_send_completion();
                    }
                    else if (entry[i].flags == (FI_MSG | FI_SEND)) {
                        GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Completion")
                            , "txcq MSG send completion"));
                        sender* handler = reinterpret_cast<sender*>(entry[i].op_context);
                        processed.user_msgs += handler->handle_send_completion();
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
                        GHEX_DP_LAZY(cnt_deb, cnt_deb.debug("rxcq Cancelled "
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
//            static auto polling = cnt_deb.make_timer(1
//                    , hpx::debug::str<>("poll recv queue"));
//            cnt_deb.timed(polling);
            //
            if (ret>0) {
                progress_count processed{true, 0};
                for (int i=0; i<ret; ++i) {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Completion")
                        , "rxcq wr_id"
                        , fi_tostr(&entry[i].flags, FI_TYPE_OP_FLAGS)
                        , "(" , hpx::debug::dec<>(entry[i].flags) , ")"
                        , "context" , hpx::debug::ptr(entry[i].op_context)
                        , "length"  , hpx::debug::hex<6>(entry[i].len)));
                    if (entry[i].flags == (FI_TAGGED | FI_MSG | FI_RECV)) {
                        GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Completion")
                            , "rxcq recv tagged completion"
                            , hpx::debug::ptr(entry[i].op_context)));
                        processed.user_msgs += reinterpret_cast<receiver *>
                                (entry[i].op_context)->handle_recv_completion(entry[i].len);
                    }
                    else {
                        cnt_deb.error("Received an unknown rxcq completion"
                            , hpx::debug::dec<>(entry[i].flags));
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
        inline rma::memory_pool<libfabric_region_provider>& get_memory_pool() {
            return *memory_pool_;
        }

        // --------------------------------------------------------------------
        void create_completion_queues(struct fi_info *info, int N)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);

            // only one thread must be allowed to create queues,
            // and it is only required once
            scoped_lock lock(controller_mutex_);
            if (txcq_!=nullptr || rxcq_!=nullptr || av_!=nullptr) {
                return;
            }

            int ret;

            fi_cq_attr cq_attr = {};
            // @TODO - why do we check this
//             if (cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Setting CQ")
                    , "FI_CQ_FORMAT_MSG"));
                cq_attr.format = FI_CQ_FORMAT_MSG;
//             }

            // open completion queue on fabric domain and set context ptr to tx queue
            cq_attr.wait_obj = FI_WAIT_NONE;
            cq_attr.size = info->tx_attr->size;
            info->tx_attr->op_flags |= FI_COMPLETION;
            cq_attr.flags = 0;//|= FI_COMPLETION;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Creating tx CQ")
                          , "size" , hpx::debug::dec<>(info->tx_attr->size)));
            ret = fi_cq_open(fabric_domain_, &cq_attr, &txcq_, &txcq_);
            if (ret) throw fabric_error(ret, "fi_cq_open");

            // open completion queue on fabric domain and set context ptr to rx queue
            cq_attr.size = info->rx_attr->size;
            GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Creating rx CQ")
                          , "size" , hpx::debug::dec<>(info->rx_attr->size)));
            ret = fi_cq_open(fabric_domain_, &cq_attr, &rxcq_, &rxcq_);
            if (ret) throw fabric_error(ret, "fi_cq_open");


            fi_av_attr av_attr = {};
            if (info->ep_attr->type == FI_EP_RDM || info->ep_attr->type == FI_EP_DGRAM) {
                if (info->domain_attr->av_type != FI_AV_UNSPEC)
                    av_attr.type = info->domain_attr->av_type;
                else {
                    GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("map FI_AV_TABLE")));
                    av_attr.type  = FI_AV_TABLE;
                    av_attr.count = N;
                }

                GHEX_DP_LAZY(cnt_deb, cnt_deb.debug(hpx::debug::str<>("Creating AV")));
                ret = fi_av_open(fabric_domain_, &av_attr, &av_, nullptr);
                if (ret) throw fabric_error(ret, "fi_av_open");
            }
        }

        // --------------------------------------------------------------------
        libfabric::locality insert_address(const libfabric::locality &address)
        {
            [[maybe_unused]] auto scp = ghex::cnt_deb.scope(this, __func__);
            cnt_deb.trace(hpx::debug::str<>("inserting AV"), iplocality(address));
            fi_addr_t fi_addr = 0xffffffff;
            int ret = fi_av_insert(av_, address.fabric_data(), 1, &fi_addr, 0, nullptr);
            if (ret < 0) {
                fabric_error(ret, "fi_av_insert");
            }
            else if (ret == 0) {
                cnt_deb.error("fi_av_insert called with existing address");
                fabric_error(ret, "fi_av_insert did not return 1");
            }
            // address was generated correctly, now update the locality with the fi_addr
            libfabric::locality new_locality(address, fi_addr);
            cnt_deb.trace(hpx::debug::str<>("AV add")
                          , "rank" , hpx::debug::dec<>(fi_addr)
                          , iplocality(new_locality)
                          , "fi_addr" , hpx::debug::hex<4>(fi_addr));
            return new_locality;
        }
    };

    controller::expected_receiver_stack thread_local controller::expected_receivers_;

}}}}

#endif
