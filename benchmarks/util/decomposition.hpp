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

#include <array>
#include <vector>
#include <stdexcept>
#include <string>
#include <iostream>

extern "C" {
#include <hwcart.h>
}

namespace ghex {
namespace bench {

class decomposition
{
public:
    using arr = std::array<int,3>;

private:
    struct hw_topo_t {
        hwcart_topo_t m;
        hw_topo_t()
        {
            if (hwcart_init(&m))
                throw std::runtime_error("hwcart init failed");
        }
    };
    hw_topo_t m_hw_topo;
    hwcart_order_t m_order = HWCartOrderYZX;
    MPI_Comm m_comm;
    arr m_node_decomposition = {1,1,1};
    arr m_socket_decomposition = {1,1,1};
    arr m_numa_decomposition = {1,1,1};
    arr m_L3_decomposition = {1,1,1};
    arr m_core_decomposition = {1,1,1};
    arr m_hwthread_decomposition = {1,1,1};
    arr m_thread_decomposition = {1,1,1};
    arr m_global_decomposition;
    std::vector<int> m_topo;
    std::vector<hwcart_split_t> m_levels;
    int m_rank;
    arr m_coord;
    arr m_last_coord;
    int m_threads_per_rank;

    void init()
    {
        if (hwcart_create(m_hw_topo.m, MPI_COMM_WORLD, m_levels.size(), m_levels.data(), m_topo.data(), m_order, &m_comm))
                throw std::runtime_error("hwcart create failed");
        for (int i=0; i<3; ++i)
        {
            m_global_decomposition[i] = 
                m_node_decomposition[i]*
                m_socket_decomposition[i]*
                m_numa_decomposition[i]*
                m_L3_decomposition[i]*
                m_core_decomposition[i]*
                m_hwthread_decomposition[i];
            m_last_coord[i] = m_global_decomposition[i]*m_thread_decomposition[i]-1;
        }
        m_threads_per_rank =
            m_thread_decomposition[0]*
            m_thread_decomposition[1]*
            m_thread_decomposition[2];
        MPI_Comm_rank(m_comm, &m_rank);
        hwcart_rank2coord(m_comm, m_global_decomposition.data(), m_rank, m_order, m_coord.data());
        m_coord[0] *= m_thread_decomposition[0];
        m_coord[1] *= m_thread_decomposition[1];
        m_coord[2] *= m_thread_decomposition[2];
    }

    static hwcart_order_t parse_order(std::string const & order_str)
    {
        if (order_str == "XYZ")
            return HWCartOrderXYZ;
        else if (order_str == "XZY")
            return HWCartOrderXZY;
        else if (order_str == "ZYX")
            return HWCartOrderZYX;
        else if (order_str == "YZX")
            return HWCartOrderYZX;
        else if (order_str == "ZXY")
            return HWCartOrderZXY;
        else if (order_str == "YXZ")
            return HWCartOrderYXZ;
        else
        {
            std::cout << "warning: unrecognized order, using XYZ" << std::endl;
            return HWCartOrderXYZ;
        }
    }

public:
    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
          node_d[0],   node_d[1],   node_d[2]}
    , m_levels{
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& socket_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_socket_decomposition(socket_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
        socket_d[0], socket_d[1], socket_d[2],
          node_d[0],   node_d[1],   node_d[2]}
    , m_levels{
        HWCART_MD_SOCKET,
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& socket_d,
        const arr& numa_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_socket_decomposition(socket_d)
    , m_numa_decomposition(numa_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
          numa_d[0],   numa_d[1],   numa_d[2], 
        socket_d[0], socket_d[1], socket_d[2],
          node_d[0],   node_d[1],   node_d[2]}
    , m_levels{
        HWCART_MD_NUMA,
        HWCART_MD_SOCKET,
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& socket_d,
        const arr& numa_d,
        const arr& l3_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_socket_decomposition(socket_d)
    , m_numa_decomposition(numa_d)
    , m_L3_decomposition(l3_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
            l3_d[0],     l3_d[1],     l3_d[2], 
          numa_d[0],   numa_d[1],   numa_d[2], 
        socket_d[0], socket_d[1], socket_d[2],
          node_d[0],   node_d[1],   node_d[2]}
    , m_levels{
        HWCART_MD_L3CACHE,
        HWCART_MD_NUMA,
        HWCART_MD_SOCKET,
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& socket_d,
        const arr& numa_d,
        const arr& l3_d,
        const arr& core_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_socket_decomposition(socket_d)
    , m_numa_decomposition(numa_d)
    , m_L3_decomposition(l3_d)
    , m_core_decomposition(core_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
          core_d[0],   core_d[1],   core_d[2], 
            l3_d[0],     l3_d[1],     l3_d[2], 
          numa_d[0],   numa_d[1],   numa_d[2], 
        socket_d[0], socket_d[1], socket_d[2],
          node_d[0],   node_d[1],   node_d[2]}
    , m_levels{
        HWCART_MD_CORE,
        HWCART_MD_L3CACHE,
        HWCART_MD_NUMA,
        HWCART_MD_SOCKET,
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(
        const std::string& order, 
        const arr& node_d,
        const arr& socket_d,
        const arr& numa_d,
        const arr& l3_d,
        const arr& core_d,
        const arr& hwthread_d,
        const arr& thread_d)
    : m_hw_topo()
    , m_order{parse_order(order)}
    , m_node_decomposition(node_d)
    , m_socket_decomposition(socket_d)
    , m_numa_decomposition(numa_d)
    , m_L3_decomposition(l3_d)
    , m_core_decomposition(core_d)
    , m_hwthread_decomposition(hwthread_d)
    , m_thread_decomposition(thread_d)
    , m_topo{
      hwthread_d[0], hwthread_d[1], hwthread_d[2], 
          core_d[0],     core_d[1],     core_d[2], 
            l3_d[0],       l3_d[1],       l3_d[2], 
          numa_d[0],     numa_d[1],     numa_d[2], 
        socket_d[0],   socket_d[1],   socket_d[2],
          node_d[0],     node_d[1],     node_d[2]}
    , m_levels{
        HWCART_MD_HWTHREAD,
        HWCART_MD_CORE,
        HWCART_MD_L3CACHE,
        HWCART_MD_NUMA,
        HWCART_MD_SOCKET,
        HWCART_MD_NODE}
    {
        init();
    }

    decomposition(const decomposition&) = delete;

    ~decomposition()
    {
        hwcart_free(&m_hw_topo.m, &m_comm);
    }

    arr coord(int thread_id)
    {
        arr res(m_coord);
        res[0] += thread_id%m_thread_decomposition[0];
        thread_id/=m_thread_decomposition[0];
        res[1] += thread_id%m_thread_decomposition[1];
        thread_id/=m_thread_decomposition[1];
        res[2] += thread_id;
        return res;
    }

    auto mpi_comm() const noexcept { return m_comm; }
    
    const arr& last_coord() const noexcept { return m_last_coord; }

    int threads_per_rank() const noexcept { return m_threads_per_rank; }

    void print()
    {
        std::cout << "should print the stuff now" << std::endl;
        hwcart_print_rank_topology(m_hw_topo.m, MPI_COMM_WORLD, m_levels.size(), m_levels.data(), m_topo.data(), m_order);
    }
};

} // namespace bench
} // namespace ghex

