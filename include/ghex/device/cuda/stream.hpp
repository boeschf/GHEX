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
#include <ghex/util/moved_bit.hpp>
#include <ghex/common/cuda_runtime.hpp>
#include <memory>

namespace ghex
{
namespace device
{
/** @brief thin wrapper around a cuda stream */
struct stream
{
    cudaStream_t          m_stream;
    ghex::util::moved_bit m_moved;

    stream()
    {
        GHEX_CHECK_CUDA_RESULT(cudaStreamCreateWithFlags(&m_stream, cudaStreamNonBlocking));
    }

    stream(const stream&) = delete;
    stream& operator=(const stream&) = delete;
    stream(stream&& other) = default;
    stream& operator=(stream&&) = default;

    ~stream()
    {
        if (!m_moved) cudaStreamDestroy(m_stream);
    }

    operator bool() const noexcept { return m_moved; }

    operator cudaStream_t() const noexcept { return m_stream; }

    cudaStream_t&       get() noexcept { return m_stream; }
    const cudaStream_t& get() const noexcept { return m_stream; }

    void sync() { GHEX_CHECK_CUDA_RESULT(cudaStreamSynchronize(m_stream)); }
};
} // namespace device

} // namespace ghex
