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

#include <iostream>
#include <iomanip>
#include <thread>
#include <mutex>
#include <chrono>
#include <array>
#include <memory>
#include <vector>
//#include <pthread.h>

#include "../util/decomposition.hpp"
#include "../util/memory.hpp"
#include <ghex/common/timer.hpp>

using clock_type = std::chrono::high_resolution_clock;

template<class Derived>
struct simulation_base
{
    using T = GHEX_FLOAT_TYPE;
    using raw_field_type = ghex::bench::memory<T>;
    using decomp_type = ghex::bench::decomposition;

    decomp_type& decomp;
    int rank;
    int size;
    int num_reps;
    int num_threads;
    bool mt;
    const int num_fields;
    int ext;
    const std::array<int,3> local_ext;
    const std::array<bool,3> periodic;
    const std::array<int,3> g_first;
    const std::array<int,3> g_last;
    const std::array<int,3> offset;
    std::array<int,6> halos;
    const std::array<int,3> local_ext_buffer;
    const int max_memory;
    std::vector<std::vector<raw_field_type>> raw_fields;
    std::mutex io_mutex;
    std::vector<gridtools::ghex::timer> timer_vec;

    simulation_base(
        int num_reps_,
        int ext_,
        int halo,
        int num_fields_,
        ghex::bench::decomposition& decomp_)
    : decomp(decomp_)
    , rank(decomp.rank())
    , size(decomp.size())
    , num_reps{num_reps_}
    , num_threads(decomp.threads_per_rank())
    , mt(num_threads > 1)
    , num_fields{num_fields_}
    , ext{ext_}
    , local_ext{ext,ext,ext}
    , periodic{true,true,true}
    , g_first{0,0,0}
    , g_last{
        decomp.last_coord()[0]*local_ext[0]+local_ext[0]-1,
        decomp.last_coord()[1]*local_ext[1]+local_ext[1]-1,
        decomp.last_coord()[2]*local_ext[2]+local_ext[2]-1}
    , offset{halo,halo,halo}
    , halos{halo,halo,halo,halo,halo,halo}
    , local_ext_buffer{
        local_ext[0]+halos[0]+halos[1],
        local_ext[1]+halos[2]+halos[3],
        local_ext[2]+halos[4]+halos[5]}
    , max_memory{local_ext_buffer[0]*local_ext_buffer[1]*local_ext_buffer[2]}
    , raw_fields(num_threads)
    , timer_vec(num_threads)
    {
    }

    void exchange(int j)
    {
        //std::this_thread::sleep_for(std::chrono::milliseconds(50));
        make_fields(j);

        static_cast<Derived*>(this)->init(j);

        //print_fields(j);
        //static_cast<Derived*>(this)->step(j);
        //check(j);
        //print_fields(j);

        // warm up
        for (int t = 0; t < 50; ++t)
            static_cast<Derived*>(this)->step(j);

        auto start = clock_type::now();
        for (int t = 0; t < num_reps; ++t)
        {
            timer_vec[j].tic();
            static_cast<Derived*>(this)->step(j);
            timer_vec[j].toc();
        }
        auto end = clock_type::now();
        std::chrono::duration<double> elapsed_seconds = end - start;

        if (rank == 0 && j == 0)
        {
            const auto num_elements =
                local_ext_buffer[0] * local_ext_buffer[1] * local_ext_buffer[2]
                - local_ext[0] * local_ext[1] * local_ext[2];
            const auto   num_bytes = num_elements * sizeof(T);
            const double load = 2 * size * num_threads * num_fields * num_bytes;
            const auto   GB_per_s = num_reps * load / (elapsed_seconds.count() * 1.0e9);
            std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
            std::cout << "GB/s : " << GB_per_s << std::endl;
            const auto tt = timer_vec[0];
            std::cout << "mean time:    " << std::setprecision(12) << tt.mean()/1000000.0 << "\n";
            std::cout << "min time:     " << std::setprecision(12) << tt.min()/1000000.0 << "\n";
            std::cout << "max time:     " << std::setprecision(12) << tt.max()/1000000.0 << "\n";
            std::cout << "sdev time:    " << std::setprecision(12) << tt.stddev()/1000000.0 << "\n";
            std::cout << "sdev f time:  " << std::setprecision(12) << tt.stddev()/tt.mean() << "\n";
            std::cout << "GB/s mean:    " << std::setprecision(12) << load / (tt.mean()*1000.0) << std::endl;
            std::cout << "GB/s min:     " << std::setprecision(12) << load / (tt.max()*1000.0) << std::endl;
            std::cout << "GB/s max:     " << std::setprecision(12) << load / (tt.min()*1000.0) << std::endl;
            std::cout << "GB/s sdev:    " << std::setprecision(12) << (tt.stddev()/tt.mean())* (load / (tt.mean()*1000.0)) << std::endl;
        }
    }

private:
    void make_fields(int j)
    {
        for (int i=0; i<num_fields; ++i)
        {
            raw_fields[j].emplace_back(max_memory, 0);
            ghex::bench::view<T,3> v(&raw_fields[j].back(),
                local_ext_buffer[0], local_ext_buffer[1], local_ext_buffer[2]);
            unsigned int c = decomp.domain(j).id + i+1;
            for (int z=0; z<ext; ++z)
            for (int y=0; y<ext; ++y)
            for (int x=0; x<ext; ++x)
            {
                v(x+halos[0], y+halos[2], z+halos[4]) = c;
                ++c;
            }
        }
    }

    void check(int j)
    {
        // TODO: check here
    }

    void print_fields(int j)
    {
        for (int i=0; i<num_fields; ++i)
        {
            ghex::bench::view<T,3> v(&raw_fields[j][i], 
                local_ext_buffer[0], local_ext_buffer[1], local_ext_buffer[2]);
            v.print();
            std::cout << std::endl;
        }
    }
};

