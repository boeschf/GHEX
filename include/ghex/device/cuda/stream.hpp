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

#include <ghex/config.hpp>
#include <ghex/device/cuda/error.hpp>
#include <ghex/device/cuda/runtime.hpp>
#include <ghex/util/moved_bit.hpp>
#include <memory>

namespace ghex
{
namespace device
{

namespace detail
{

template<typename Data>
static void stream_callback(cudaStream_t, cudaError_t status, void* user_data)
{
    static_cast<Data*>(user_data)->notify(status);
}

} // namespace detail

/** @brief thin wrapper around a cuda stream */
struct stream
{
    cudaStream_t          m_stream;
    //cudaEvent_t           m_event;
    ghex::util::moved_bit m_moved;

    struct rendezvous
    {
        cudaError_t m_status;
        volatile bool m_done = false;

        rendezvous(cudaStream_t s)
        : m_status{cudaStreamAddCallback(s, detail::stream_callback<rendezvous>, this, 0)}
        {
            if (cudaSuccess != m_status) m_done = true;
        }

        void notify(cudaError_t status) noexcept
        {
            m_status = status;
            m_done = true;
        }

        cudaError_t wait() noexcept
        {
            while(!m_done) {}
            return m_status;
        }
    };

    stream()
    {
        GHEX_CHECK_CUDA_RESULT(cudaStreamCreateWithFlags(&m_stream, cudaStreamNonBlocking));
        //GHEX_CHECK_CUDA_RESULT(cudaEventCreateWithFlags(&m_event, cudaEventDisableTiming));
    }

    stream(const stream&) = delete;
    stream& operator=(const stream&) = delete;
    stream(stream&& other) = default;
    stream& operator=(stream&&) = default;

    ~stream()
    {
        if (!m_moved)
        {
            cudaStreamDestroy(m_stream);
            //cudaEventDestroy(m_event);
        }
    }

    operator bool() const noexcept { return m_moved; }

    operator cudaStream_t() const noexcept { return m_stream; }

    cudaStream_t&       get() noexcept { return m_stream; }
    const cudaStream_t& get() const noexcept { return m_stream; }

    void sync()
    {
        //GHEX_CHECK_CUDA_RESULT(cudaEventRecord(m_event, m_stream));
        //// busy wait here
        //GHEX_CHECK_CUDA_RESULT(cudaEventSynchronize(m_event));

        rendezvous r(m_stream);
        // busy wait here
        GHEX_CHECK_CUDA_RESULT(r.wait());
    }


};
} // namespace device

} // namespace ghex
