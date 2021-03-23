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

#pragma once

#include <memory>
#include <array>
#include <numeric>
#include <cassert>
#include <iostream>
#include <iomanip>
#include <ghex/cuda_utils/error.hpp>

namespace ghex {
namespace bench {

template<typename T>
struct memory
{
    unsigned int m_size;
    std::unique_ptr<T[]> m_host_memory;
#ifdef __CUDACC__
    struct cuda_deleter
    {
        // no delete since this messes up the rma stuff
        // when doing 2 tests in a row!!!
        //void operator()(T* ptr) const { cudaFree(ptr); }
        void operator()(T*) const { /* do nothing */ }
    };
    std::unique_ptr<T[],cuda_deleter> m_device_memory;
#endif

    memory(unsigned int size_, const T& value)
    : m_size{size_}
    , m_host_memory{ new T[m_size] }
    {
        for (unsigned int i=0; i<m_size; ++i)
            m_host_memory[i] = value;
#ifdef __CUDACC__
        void* ptr;
        GHEX_CHECK_CUDA_RESULT(cudaMalloc(&ptr, m_size*sizeof(T)))
        m_device_memory.reset((T*)ptr);
        clone_to_device();
#endif
    }

    memory(const memory&) = delete;
    memory(memory&&) = default;

    T* data() const { return m_host_memory.get(); }
    T* host_data() const { return m_host_memory.get(); }
#ifdef __CUDACC__
    T* device_data() const { return m_device_memory.get(); }
    T* hd_data() const { return m_device_memory.get(); }
#else
    T* hd_data() const { return m_host_memory.get(); }
#endif

    unsigned int size() const { return m_size; }

    const T& operator[](unsigned int i) const { return m_host_memory[i]; }
          T& operator[](unsigned int i)       { return m_host_memory[i]; }

    T* begin() { return m_host_memory.get(); }
    T* end() { return m_host_memory.get()+m_size; }

    const T* begin() const { return m_host_memory.get(); }
    const T* end() const { return m_host_memory.get()+m_size; }

#ifdef __CUDACC__
    void clone_to_device()
    {
        GHEX_CHECK_CUDA_RESULT(cudaMemcpy(m_device_memory.get(), m_host_memory.get(),
            m_size*sizeof(T), cudaMemcpyHostToDevice))
    }
    void clone_to_host()
    {
        GHEX_CHECK_CUDA_RESULT(cudaMemcpy(m_host_memory.get(),m_device_memory.get(),
            m_size*sizeof(T), cudaMemcpyDeviceToHost))
    }
#endif
};


template<typename T, unsigned int Dim>
class view
{
public:
    using memory_type = memory<T>;
    using array_type = std::array<unsigned int, Dim>;

private:
    memory_type* m;
    array_type m_dims;
    array_type m_strides;
    static constexpr int s_w = 6;

public:
    view(memory_type* mem, array_type dims)
    : m{mem}
    , m_dims{dims}
    {
        std::partial_sum(dims.begin(), dims.end(), m_strides.begin(), std::multiplies<unsigned int>());
        assert(m_strides[Dim-1] == m->size());
    }

    template<typename... D>
    view(memory_type* mem, D... dims)
    : view(mem, array_type{(unsigned int)dims...})
    {}

    const T& operator()(array_type const & coord) const noexcept
    {
        unsigned int i = coord[0];
        for (unsigned int d=1; d<Dim; ++d)
            i += coord[d]*m_strides[d-1];
        return m->operator[](i);
    }

    template<typename... I>
    const T& operator()(I... i) const noexcept { return this->operator()(array_type{(unsigned int)i...}); }

    T& operator()(array_type const & coord) noexcept
    {
        unsigned int i = coord[0];
        for (unsigned int d=1; d<Dim; ++d)
            i += coord[d]*m_strides[d-1];
        return m->operator[](i);
    }

    template<typename... I>
    T& operator()(I... i) noexcept { return this->operator()(array_type{(unsigned int)i...}); }

    const memory_type& get() const noexcept { return *m; }

    memory_type& get() noexcept { return *m; }

    void print() const
    {
        print(Dim-1, array_type{});
    }

private:
    void print(unsigned int d, array_type coord) const
    {
        if (d==0)
        {
            const int y_s = (Dim>1 ? coord[1]+1 : 1);
            std::cout << std::setw(y_s) << " ";
            for (coord[0]=0; coord[0]<m_dims[0]; ++coord[0])
                std::cout << std::setw(s_w) << std::right << this->operator()(coord);
        }
        else if (d==1)
        {
            for (unsigned int i=0; i<m_dims[1]; ++i)
            {
                coord[1] = m_dims[1]-1-i;
                print(0, coord);
                std::cout << "\n";
            }
        }
        else
        {
            for (unsigned int i=0; i<m_dims[d]; ++i)
            {
                coord[d] = m_dims[d]-1-i;
                print(d-1, coord);
                std::cout << "\n";
            }
        }
    };
};

} // namespace bench
} // namespace ghex
