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

#include "./simple_rma_base.hpp"

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

struct simulation : public simulation_base<simulation>
{
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

    context_ptr_type context_ptr;
    context_type& context;
    halo_generator_type halo_gen;
    std::vector<domain_descriptor_type> local_domains;
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

    simulation(
        int num_reps_,
        int ext_,
        int halo,
        int num_fields_,
        bool check_,
        ghex::bench::decomposition& decomp)
    : simulation_base(num_reps_, ext_, halo, num_fields_, check_, decomp)
    , context_ptr{gridtools::ghex::tl::context_factory<transport>::create(decomp.mpi_comm())}
    , context{*context_ptr}
    , halo_gen(g_first, g_last, halos, periodic)
    , fields(num_threads)
#ifdef __CUDACC__
    , fields_gpu.resize(num_threads)
#endif
    , comm{ context.get_serial_communicator() }
    , comms(num_threads, comm)
    , cos(num_threads)
    {
        local_domains.reserve(num_threads);
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

    void init(int j)
    {
        comms[j] = context.get_communicator();
        for (int i=0; i<num_fields; ++i)
        {
            fields[j].push_back(gridtools::ghex::wrap_field<gridtools::ghex::cpu,2,1,0>(
                local_domains[j],
                raw_fields[j][i].host_data(),
                offset,
                local_ext_buffer));
#ifdef __CUDACC__
            fields_gpu[j].push_back(gridtools::ghex::wrap_field<gridtools::ghex::gpu,2,1,0>(
                local_domains[j],
                raw_fields[j][i].device_data(),
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
        > (comms[j]);
#ifndef __CUDACC__
        for (int i=0; i<num_fields; ++i)
            bco.add_field(pattern->operator()(fields[j][i]));
#else
        for (int i=0; i<num_fields; ++i)
            bco.add_field(pattern->operator()(fields_gpu[j][i]));
#endif
        cos[j] = std::move(bco);
    }

    void step(int j)
    {
        cos[j].exchange().wait();
    }
};
