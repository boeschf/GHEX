//  Copyright (c) 2014-2017 John Biddiscombe
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef HPX_PARCELSET_POLICIES_RMA_memory_block_allocator
#define HPX_PARCELSET_POLICIES_RMA_memory_block_allocator

#include "ghex_libfabric_defines.hpp"
//
#include <ghex/transport_layer/libfabric/rma/atomic_count.hpp>
//
#include <ghex/transport_layer/libfabric/rma/detail/memory_region_impl.hpp>
#include <ghex/transport_layer/libfabric/performance_counter.hpp>
//
#include <boost/lockfree/stack.hpp>
//
#include <atomic>
#include <stack>
#include <iostream>
#include <cstddef>
#include <memory>
#include <array>
#include <sstream>
#include <string>

namespace gridtools {
namespace ghex {
namespace tl {
namespace libfabric {
namespace rma {
namespace detail
{

    static hpx::debug::enable_print<false> mbs_deb("MBALLOC");

    // --------------------------------------------------------------------
    // This is a simple class that implements only malloc and free but is
    // templated over the memory region provider which is transport layer
    // dependent. This class is used internally by the rma memory pools to
    // allocate blocks and should not be called by user code directly.
    // The allocator is intended to be used to generate large blocks that are
    // subdivided and used by the memory pools and not to allocate many
    // small blocks. These blocks are returned as shared pointers.
    template <typename RegionProvider>
    struct memory_block_allocator
    {
        typedef typename RegionProvider::provider_domain domain_type;
        typedef memory_region_impl<RegionProvider>       region_type;
        typedef std::shared_ptr<region_type>             region_ptr;

        // default empty constructor
        memory_block_allocator() {}

        // allocate a registered memory region
        static region_ptr malloc(domain_type *pd, const std::size_t bytes)
        {
            region_ptr region = std::make_shared<region_type>();
            region->allocate(pd, bytes);
            GHEX_DP_ONLY(mbs_deb, trace(hpx::debug::str<>("Allocating")
                          , hpx::debug::hex<4>(bytes)
                          , "chunk mallocator", *region));
            return region;
        }

        // release a registered memory region
        static void free(region_ptr region) {
            GHEX_DP_ONLY(mbs_deb, trace(hpx::debug::str<>("Freeing")
                          , "chunk mallocator", *region));
            region.reset();
        }
    };

}}}}}}

#endif
