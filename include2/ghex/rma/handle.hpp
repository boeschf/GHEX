/* 
 * GridTools
 * 
 * Copyright (c) 2014-2021, ETH Zurich
 * All rights reserved.
 * 
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
#pragma once

#include <ghex/rma/locality.hpp>
#include <ghex/rma/thread/handle.hpp>
#ifdef GHEX_USE_XPMEM
#include <ghex/rma/xpmem/handle.hpp>
#endif
#ifdef __CUDACC__
#include <ghex/rma/cuda/handle.hpp>
#endif
#include <memory>

namespace ghex
{
namespace rma
{
/** @brief General local RMA handle created at the owner's site. Remote counterpart can be generated
  * through the info object which this class exposes. The info object is POD and can be sent easily
  * over any network. */
struct local_handle
{
    struct data_holder
    {
        unsigned int              m_size;
        bool                      m_on_gpu;
        thread::local_data_holder m_thread_data_holder;
#ifdef GHEX_USE_XPMEM
        xpmem::local_data_holder m_xpmem_data_holder;
#endif
#ifdef __CUDACC__
        cuda::local_data_holder m_cuda_data_holder;
#endif
        struct info
        {
            unsigned int m_size;
            bool         m_on_gpu;
            thread::info m_thread_info;
#ifdef GHEX_USE_XPMEM
            xpmem::info m_xpmem_info;
#endif
#ifdef __CUDACC__
            cuda::info m_cuda_info;
#endif
        };

        data_holder(void* ptr, unsigned int size, bool on_gpu)
        : m_size{size}
        , m_on_gpu{on_gpu}
        , m_thread_data_holder(ptr, size, on_gpu)
#ifdef GHEX_USE_XPMEM
        , m_xpmem_data_holder(ptr, size, on_gpu)
#endif
#ifdef __CUDACC__
        , m_cuda_data_holder(ptr, size, on_gpu)
#endif
        {
        }

        info get_info() const
        {
            return {m_size, m_on_gpu, m_thread_data_holder.get_info()
#ifdef GHEX_USE_XPMEM
                                          ,
                m_xpmem_data_holder.get_info()
#endif
#ifdef __CUDACC__
                    ,
                m_cuda_data_holder.get_info()
#endif
            };
        }
    };

    using info = data_holder::info;

    std::unique_ptr<data_holder> m_impl;

    void init(void* ptr, unsigned int size, bool on_gpu)
    {
        if (!m_impl) m_impl.reset(new data_holder(ptr, size, on_gpu));
    }

    info get_info() const { return m_impl->get_info(); }
};

using info = typename local_handle::info;

/** @brief General local RMA handle created at the remote's site. The constructor
  * takes an info object exposed and sent by the owner of the memory. */
struct remote_handle
{
    struct data_holder
    {
        unsigned int               m_size;
        bool                       m_on_gpu;
        thread::remote_data_holder m_thread_data_holder;
#ifdef GHEX_USE_XPMEM
        xpmem::remote_data_holder m_xpmem_data_holder;
#endif
#ifdef __CUDACC__
        cuda::remote_data_holder m_cuda_data_holder;
#endif

        data_holder(const info& info_, locality loc, int rank)
        : m_size{info_.m_size}
        , m_on_gpu{info_.m_on_gpu}
        , m_thread_data_holder(info_.m_thread_info, loc, rank)
#ifdef GHEX_USE_XPMEM
        , m_xpmem_data_holder(info_.m_xpmem_info, loc, rank)
#endif
#ifdef __CUDACC__
        , m_cuda_data_holder(info_.m_cuda_info, loc, rank)
#endif
        {
        }

        void* get_ptr(locality loc) const
        {
            static_assert(
                std::is_same<decltype(loc), locality>::value, ""); // prevent compiler warning
#ifdef GHEX_USE_XPMEM
            if (loc == locality::process && !m_on_gpu) return m_xpmem_data_holder.get_ptr();
#endif
#ifdef __CUDACC__
            if (loc == locality::process && m_on_gpu) return m_cuda_data_holder.get_ptr();
#endif
            return m_thread_data_holder.get_ptr();
        }
    };

    std::unique_ptr<data_holder> m_impl;

    void init(const info& info, locality loc, int rank)
    {
        if (!m_impl) m_impl.reset(new data_holder(info, loc, rank));
    }

    void* get_ptr(locality loc) const { return m_impl->get_ptr(loc); }

    bool on_gpu() const noexcept { return m_impl->m_on_gpu; }
};

} // namespace rma
} // namespace ghex
