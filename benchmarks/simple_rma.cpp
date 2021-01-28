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

#include "./util/options.hpp"
#ifdef GHEX_BENCHMARK_USE_RAW_MPI
#include "./simple_rma/simple_rma_mpi.hpp"
#else
#include "./simple_rma/simple_rma_ghex.hpp"
#endif

int main(int argc, char** argv)
{
    const auto options = ghex::options()
        ("domain",   "local domain size",     "d",        {64})
        ("nrep",     "number of repetitions", "r",        {10})
        ("nfields",  "number of fields",      "n",        {1})
        ("halo",     "halo size",             "h",        {1})
        ("order",    "cartesian order",       "IJK",      {"XYZ"})
        ("node",     "node grid",             "NX NY NZ", 3)
        ("socket",   "socket grid",           "NX NY NZ", 3)
        ("numa",     "numa-node grid",        "NX NY NZ", 3)
        ("l3",       "l3-cache grid",         "NX NY NZ", 3)
        ("core",     "core grid",             "NX NY NZ", 3)
        ("hwthread", "hardware-thread grid",  "NX NY NZ", 3)
        ("thread",   "software-thread grid",  "NX NY NZ", {1,1,1})
        ("print",    "print decomposition")
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

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    {
        std::unique_ptr<ghex::bench::decomposition> decomp_ptr;
        
        try {
        if (options.has("hwthread"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get_or("l3",     std::array<int,3>{1,1,1}),
                options.get_or("core",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("hwthread"),
                threads);
        else if (options.has("core"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get_or("l3",     std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("core"),
                threads);
        else if (options.has("l3"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get_or("numa",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("l3"),
                threads);
        else if (options.has("numa"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get_or("socket", std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("numa"),
                threads);
        else if (options.has("socket"))
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                options.get<std::array<int,3>>("socket"),
                threads);
        else 
            decomp_ptr = std::make_unique<ghex::bench::decomposition>(
                options.get<std::string>("order"),
                options.get_or("node",   std::array<int,3>{1,1,1}),
                threads);
        }
        catch (...)
        {
            if (rank == 0)
                std::cout << "could not create decomposition" << std::endl;
            MPI_Finalize();
            return 0;
        }

        if (options.is_set("print"))
        {
            if (rank == 0)
                decomp_ptr->print();
            MPI_Barrier(MPI_COMM_WORLD);
            decomp_ptr.release();
            MPI_Barrier(MPI_COMM_WORLD);
            MPI_Finalize();
            return 0;
        }

        simulation sim(
            options.get<int>("nrep"), 
            options.get<int>("domain"),
            options.get<int>("halo"),
            options.get<int>("nfields"),
            *decomp_ptr
        );

        if (decomp_ptr->threads_per_rank() == 1)
        {
            sim.exchange(0);
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
                threads.push_back( std::thread([&sim,j](){ sim.exchange(j); }) );
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
    
        MPI_Barrier(MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}
