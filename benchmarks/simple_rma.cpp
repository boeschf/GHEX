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

#include <iostream>
#include <iomanip>
#include <thread>
#include <mutex>
#include <chrono>
#include <pthread.h>
#include <array>
#include <memory>
#include <gridtools/common/array.hpp>
#include <pthread.h>
#include <thread>
#include <vector>

#include "./util/decomposition.hpp"
#include "./util/memory.hpp"
#include "./util/options.hpp"

#ifndef GHEX_TEST_USE_UCX
#include <ghex/transport_layer/mpi/context.hpp>
using transport = gridtools::ghex::tl::mpi_tag;
#else
#include <ghex/transport_layer/ucx/context.hpp>
using transport = gridtools::ghex::tl::ucx_tag;
#endif

#include <ghex/bulk_communication_object.hpp>
#include <ghex/structured/pattern.hpp>
#include <ghex/structured/rma_range_generator.hpp>
#include <ghex/structured/regular/domain_descriptor.hpp>
#include <ghex/structured/regular/field_descriptor.hpp>
#include <ghex/structured/regular/halo_generator.hpp>
#include <ghex/common/timer.hpp>

using clock_type = std::chrono::high_resolution_clock;

struct simulation
{
    using T = GHEX_FLOAT_TYPE;
    using raw_field_type = ghex::bench::memory<T>;
    using context_type = typename gridtools::ghex::tl::context_factory<transport>::context_type;
    using context_ptr_type = std::unique_ptr<context_type>;
    using domain_descriptor_type = gridtools::ghex::structured::regular::domain_descriptor<int,3>;
    using halo_generator_type = gridtools::ghex::structured::regular::halo_generator<int,3>;
    template<typename Arch, int... Is>
    using field_descriptor_type  = gridtools::ghex::structured::regular::field_descriptor<T,Arch,domain_descriptor_type, Is...>;

    using field_type     = field_descriptor_type<gridtools::ghex::cpu, 2, 1, 0>;
#ifdef __CUDACC__
    using gpu_field_type = field_descriptor_type<gridtools::ghex::gpu, 2, 1, 0>;
#endif

    int num_reps;
    int num_threads;
    bool mt;
    const int num_fields;
    int ext;
    context_ptr_type context_ptr;
    context_type& context;
    const std::array<int,3> local_ext;
    const std::array<bool,3> periodic;
    const std::array<int,3> g_first;
    const std::array<int,3> g_last;
    const std::array<int,3> offset;
    std::array<int,6> halos;
    const std::array<int,3> local_ext_buffer;
    halo_generator_type halo_gen;
    std::vector<domain_descriptor_type> local_domains;
    const int max_memory;
    std::vector<std::vector<raw_field_type>> raw_fields;
    std::vector<std::vector<field_type>> fields;
#ifdef __CUDACC__
    std::vector<std::vector<gpu_field_type>> fields_gpu;
#endif
    typename context_type::communicator_type comm;
    std::vector<typename context_type::communicator_type> comms;
    std::vector<gridtools::ghex::generic_bulk_communication_object> cos;

    using pattern_type = std::remove_reference_t<decltype(
        gridtools::ghex::make_pattern<gridtools::ghex::structured::grid>(context, halo_gen, local_domains))>;
    std::unique_ptr<pattern_type> pattern;
    std::mutex io_mutex;

    std::vector<gridtools::ghex::timer> timer_vec;

    simulation(
        int num_reps_,
        int ext_,
        int halo,
        int num_fields_,
        ghex::bench::decomposition& decomp)
    : num_reps{num_reps_}
    , num_threads(decomp.threads_per_rank())
    , mt(num_threads > 1)
    , num_fields{num_fields_}
    , ext{ext_}
    , context_ptr{gridtools::ghex::tl::context_factory<transport>::create(decomp.mpi_comm())}
    , context{*context_ptr}
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
    , halo_gen(g_first, g_last, halos, periodic)
    , max_memory{local_ext_buffer[0]*local_ext_buffer[1]*local_ext_buffer[2]}
    , comm{ context.get_serial_communicator() }
    , timer_vec(num_threads)
    {
        cos.resize(num_threads);
        local_domains.reserve(num_threads);
        raw_fields.resize(num_threads);
        fields.resize(num_threads);
#ifdef __CUDACC__
        fields_gpu.resize(num_threads);
#endif
        comms = std::vector<typename context_type::communicator_type>(num_threads, comm);

        for (int j=0; j<num_threads; ++j)
        {
            const auto coord = decomp.coord(j);
            int x = coord[0]*local_ext[0];
            int y = coord[1]*local_ext[1];
            int z = coord[2]*local_ext[2];
            local_domains.push_back(domain_descriptor_type{
                context.rank()*num_threads+j,
                std::array<int,3>{x,y,z},
                std::array<int,3>{x+local_ext[0]-1,y+local_ext[1]-1,z+local_ext[2]-1}});
        }

        pattern = std::unique_ptr<pattern_type>{new pattern_type{
            gridtools::ghex::make_pattern<gridtools::ghex::structured::grid>(
                context, halo_gen, local_domains)}};
    }

    void exchange()
    {
        if (num_threads == 1)
        {
            exchange(0);
            //std::thread t([this](){exchange(0);});
            //// Create a cpu_set_t object representing a set of CPUs. Clear it and mark
            //// only CPU = local rank as set.
            //cpu_set_t cpuset;
            //CPU_ZERO(&cpuset);
            //CPU_SET(decomp.node_resource(comm.rank()), &cpuset);
            //int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
            //if (rc != 0) { std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n"; }
            //t.join();
        }
        else
        {
            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (int j=0; j<num_threads; ++j)
            {
                threads.push_back( std::thread([this,j](){ exchange(j); }) );
                //// Create a cpu_set_t object representing a set of CPUs. Clear it and mark
                //// only CPU j as set.
                //cpu_set_t cpuset;
                //CPU_ZERO(&cpuset);
                //CPU_SET(decomp.node_resource(comm.rank(),j), &cpuset);
                //int rc = pthread_setaffinity_np(threads[j].native_handle(), sizeof(cpu_set_t), &cpuset);
                //if (rc != 0) { std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n"; }
            }
            for (auto& t : threads) t.join();
        }
    }

    void exchange(int j)
    {
        //std::this_thread::sleep_for(std::chrono::milliseconds(50));
        comms[j] = context.get_communicator();
        auto basic_co = gridtools::ghex::make_communication_object<pattern_type>(comms[j]);
        for (int i=0; i<num_fields; ++i)
        {
            raw_fields[j].emplace_back(max_memory, 0);
            fields[j].push_back(gridtools::ghex::wrap_field<gridtools::ghex::cpu,2,1,0>(
                local_domains[j],
                raw_fields[j].back().host_data(),
                offset,
                local_ext_buffer));
#ifdef __CUDACC__
            fields_gpu[j].push_back(gridtools::ghex::wrap_field<gridtools::ghex::gpu,2,1,0>(
                local_domains[j],
                raw_fields[j].back().device_data(),
                offset,
                local_ext_buffer));
#endif
        }

        auto bco = gridtools::ghex::bulk_communication_object<
            gridtools::ghex::structured::rma_range_generator,
            pattern_type,
#ifndef __CUDACC__
            field_type
#else
            gpu_field_type
#endif
        > (basic_co);
#ifndef __CUDACC__
        for (int i=0; i<num_fields; ++i)
            bco.add_field(pattern->operator()(fields[j][i]));
#else
        for (int i=0; i<num_fields; ++i)
            bco.add_field(pattern->operator()(fields_gpu[j][i]));
#endif
        cos[j] = std::move(bco);

        // warm up
        for (int t = 0; t < 50; ++t)
        {
            cos[j].exchange().wait();
        }

        auto start = clock_type::now();
        for (int t = 0; t < num_reps; ++t)
        {
            timer_vec[j].tic();
            cos[j].exchange().wait();
            timer_vec[j].toc();
        }
        auto end = clock_type::now();
        std::chrono::duration<double> elapsed_seconds = end - start;

        if (comm.rank() == 0 && j == 0)
        {
            const auto num_elements =
                local_ext_buffer[0] * local_ext_buffer[1] * local_ext_buffer[2]
                - local_ext[0] * local_ext[1] * local_ext[2];
            const auto   num_bytes = num_elements * sizeof(T);
            const double load = 2 * comm.size() * num_threads * num_fields * num_bytes;
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
};

int main(int argc, char** argv)
{
    const auto options = ghex::options()
        ("domain",   "local domain size",     "d",        {64})
        ("nrep",     "number of repetitions", "r",        {10})
        ("nfields",  "number of fields",      "n",        {1})
        ("halo",     "halo size",             "h",        {1})
        ("node",     "node grid",             "NX NY NZ", 3)
        ("socket",   "socket grid",           "NX NY NZ", 3)
        ("numa",     "numa-node grid",        "NX NY NZ", 3)
        ("l3",       "l3-cache grid",         "NX NY NZ", 3)
        ("core",     "core grid",             "NX NY NZ", 3)
        ("hwthread", "hardware-thread grid",  "NX NY NZ", 3)
        ("thread",   "software-thread grid",  "NX NY NZ", {1,1,1})
        .parse(argc, argv);

    const auto threads = options.get<std::array<int,3>>("thread");
    const auto num_threads = threads[0]*threads[1]*threads[2];
    
    int required = num_threads>1 ?  MPI_THREAD_MULTIPLE :  MPI_THREAD_SINGLE;
    int provided;
    int init_result = MPI_Init_thread(&argc, &argv, required, &provided);
    if (init_result == MPI_ERR_OTHER)
    {
        std::cerr << "MPI init failed\n";
        std::terminate();
    }
    if (provided < required)
    {
        std::cerr << "MPI does not support required threading level\n";
        std::terminate();
    }

    {
        std::unique_ptr<ghex::bench::decomposition> decomp_ptr;
        
        if (options.has("hwthread"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get_or("l3",     std::array<int,3>{1,1,1}),
                options.get_or("core",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("hwthread"),
                threads);
        else if (options.has("core"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get_or("l3",     std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("core"),
                threads);
        else if (options.has("l3"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("l3"),
                threads);
        else if (options.has("numa"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("numa"),
                threads);
        else if (options.has("socket"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("socket"),
                threads);
        else 
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get_or("node",   std::array<int,3>{1,1,1}),
                threads);

        simulation sim(
            options.get<int>("nrep"), 
            options.get<int>("domain"),
            options.get<int>("halo"),
            options.get<int>("nfields"),
            *decomp_ptr
        );

        sim.exchange();
    
        MPI_Barrier(MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
