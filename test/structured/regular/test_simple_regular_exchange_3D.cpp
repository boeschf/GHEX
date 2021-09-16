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
#include <gtest/gtest.h>
#include "../../mpi_runner/mpi_test_fixture.hpp"

#include <ghex/config.hpp>
#include <ghex/bulk_communication_object.hpp>
#include <ghex/structured/pattern.hpp>
#include <ghex/structured/rma_range_generator.hpp>
#include <ghex/structured/regular/domain_descriptor.hpp>
#include <ghex/structured/regular/halo_generator.hpp>
#include <ghex/structured/regular/field_descriptor.hpp>
#include <ghex/structured/regular/make_pattern.hpp>
#ifdef GHEX_CUDACC
#include <ghex/device/cuda/runtime.hpp>
#endif

#include "../../util/memory.hpp"
#include <gridtools/common/array.hpp>
#include <array>
#include <iostream>
#include <vector>
#include <future>

using namespace ghex;
using arr = std::array<int, 3>;
using domain = structured::regular::domain_descriptor<int, std::integral_constant<int, 3>>;
using halo_gen = structured::regular::halo_generator<int, std::integral_constant<int, 3>>;

#define DIM  3
#define HALO 1
#define PX   true
#define PY   true
#define PZ   true

constexpr std::array<int, 6>  halos{HALO, HALO, HALO, HALO, HALO, HALO};
constexpr std::array<bool, 3> periodic{PX, PY, PZ};

ghex::test::util::memory<gridtools::array<int, 3>>
allocate_field()
{
    return {(HALO * 2 + DIM) * (HALO * 2 + DIM) * (HALO * 2 + DIM),
        gridtools::array<int, 3>{-1, -1, -1}};
}

template<typename RawField>
auto
wrap_cpu_field(RawField& raw_field, const domain& d)
{
    return wrap_field<cpu, gridtools::layout_map<2, 1, 0>>(d, raw_field.data(),
        arr{HALO, HALO, HALO}, arr{HALO * 2 + DIM, HALO * 2 + DIM, HALO * 2 + DIM});
}

#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
template<typename RawField>
auto
wrap_gpu_field(RawField& raw_field, const domain& d)
{
    return wrap_field<gpu, gridtools::layout_map<2, 1, 0>>(d, raw_field.device_data(),
        arr{HALO, HALO, HALO}, arr{HALO * 2 + DIM, HALO * 2 + DIM, HALO * 2 + DIM});
}
#endif

template<typename Field>
Field&&
fill(Field&& field)
{
    for (int k = 0; k < DIM; ++k)
        for (int j = 0; j < DIM; ++j)
            for (int i = 0; i < DIM; ++i)
            {
                auto& v = field({i, j, k});
                v[0] = field.domain().first()[0] + i;
                v[1] = field.domain().first()[1] + j;
                v[2] = field.domain().first()[2] + k;
            }
    return std::forward<Field>(field);
}

template<typename Field>
Field&&
reset(Field&& field)
{
    for (int k = -HALO; k < DIM + HALO; ++k)
        for (int j = -HALO; j < DIM + HALO; ++j)
            for (int i = -HALO; i < DIM + HALO; ++i)
            {
                auto& v = field({i, j, k});
                v[0] = -1;
                v[1] = -1;
                v[2] = -1;
            }
    return fill(std::forward<Field>(field));
}

int
expected(int coord, int dim, int first, int last, bool periodic_, int halo)
{
    if (coord < 0 && halo <= 0) return -1;
    if (coord >= DIM && halo <= 0) return -1;
    const auto ext = (last + 1) - first;
    int        res = first + coord;
    if (first == 0 && coord < 0) res = periodic_ ? dim * DIM + coord : -1;
    if (last == dim * DIM - 1 && coord >= ext) res = periodic_ ? coord - ext : -1;
    return res;
}

template<typename Arr>
bool
compare(const Arr& v, int x, int y, int z)
{
    if (x == -1 || y == -1 || z == -1)
    {
        x = -1;
        y = -1;
        z = -1;
    }
    return (x == v[0]) && (y == v[1] && (z == v[2]));
}

template<typename Field>
bool
check(const Field& field, const arr& dims)
{
    bool res = true;
    for (int k = -HALO; k < DIM + HALO; ++k)
    {
        const auto z = expected(k, dims[2], field.domain().first()[2], field.domain().last()[2],
            periodic[2], k < 0 ? halos[4] : halos[5]);
        for (int j = -HALO; j < DIM + HALO; ++j)
        {
            const auto y = expected(j, dims[1], field.domain().first()[1], field.domain().last()[1],
                periodic[1], j < 0 ? halos[2] : halos[3]);
            for (int i = -HALO; i < DIM + HALO; ++i)
            {
                const auto x = expected(i, dims[0], field.domain().first()[0],
                    field.domain().last()[0], periodic[0], i < 0 ? halos[0] : halos[1]);
                res = res && compare(field({i, j, k}), x, y, z);
                if (!res)
                    std::cout << "found: (" << field({i, j, k})[0] << ", " << field({i, j, k})[1]
                              << ", " << field({i, j, k})[2] << ") "
                              << "but expected: (" << x << ", " << y << ", " << z
                              << ") at coordinate [" << i << ", " << j << ", " << k << "]"
                              << std::endl;
            }
        }
    }
    return res;
}

auto
make_domain(int rank, arr coord)
{
    const auto x = coord[0] * DIM;
    const auto y = coord[1] * DIM;
    const auto z = coord[2] * DIM;
    return domain{rank, arr{x, y, z}, arr{x + DIM - 1, y + DIM - 1, z + DIM - 1}};
}

struct domain_lu
{
    struct neighbor
    {
        int m_rank;
        int m_id;
        int rank() const noexcept { return m_rank; }
        int id() const noexcept { return m_id; }
    };

    arr m_dims;

    neighbor operator()(int id, arr const& offset) const noexcept
    {
        auto rank = id;
        auto z = rank / (m_dims[0] * m_dims[1]);
        rank -= z * m_dims[0] * m_dims[1];
        auto y = rank / m_dims[0];
        auto x = rank - y * m_dims[0];

        x = (x + offset[0] + m_dims[0]) % m_dims[0];
        y = (y + offset[1] + m_dims[1]) % m_dims[1];
        z = (z + offset[2] + m_dims[2]) % m_dims[2];

        int n_rank = z * m_dims[0] * m_dims[1] + y * m_dims[0] + x;
        int n_id = n_rank;
        return {n_rank, n_id};
    }
};

template<typename Context, typename Pattern, typename SPattern, typename Domains>
bool
run(Context& ctxt, const Pattern& pattern, const SPattern& spattern, const Domains& domains,
    const arr& dims)
{
    bool res = true;
    // fields
    auto raw_field = allocate_field();
    auto field = fill(wrap_cpu_field(raw_field, domains[0]));
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_device();
    auto field_gpu = wrap_gpu_field(raw_field, domains[0]);
#endif

    // general exchange
    // ================
    auto co = make_communication_object<Pattern>(ctxt);

    // classical
    // ---------

#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    co.exchange(pattern(field_gpu)).wait();
#else
    co.exchange(pattern(field)).wait();
#endif

    // check fields
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_device();
#endif

    // using stages
    // ------------

#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    co.exchange(spattern[0]->operator()(field_gpu)).wait();
    co.exchange(spattern[1]->operator()(field_gpu)).wait();
    co.exchange(spattern[2]->operator()(field_gpu)).wait();
#else
    co.exchange(spattern[0]->operator()(field)).wait();
    co.exchange(spattern[1]->operator()(field)).wait();
    co.exchange(spattern[2]->operator()(field)).wait();
#endif

    // check fields
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_device();
#endif

    // bulk exchange (rma)
    // ===================

    // classical
    // ---------

#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    auto bco =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(
            ctxt);
    bco.add_field(pattern(field_gpu));
#else
    auto bco = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(
        ctxt, true);
    bco.add_field(pattern(field));
#endif
    //bco.init();
    bco.exchange().wait();

    // check fields
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_device();
#endif

    // using stages
    // ------------

#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    auto bco_x =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(
            ctxt);
    bco_x.add_field(spattern[0]->operator()(field_gpu));
    auto bco_y =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(
            ctxt);
    bco_y.add_field(spattern[1]->operator()(field_gpu));
    auto bco_z =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(
            ctxt);
    bco_z.add_field(spattern[2]->operator()(field_gpu));
#else
    auto bco_x =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(
            ctxt, true);
    bco_x.add_field(spattern[0]->operator()(field));
    auto bco_y =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(
            ctxt, true);
    bco_y.add_field(spattern[1]->operator()(field));
    auto bco_z =
        bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(
            ctxt, true);
    bco_z.add_field(spattern[2]->operator()(field));
#endif
    //bco_x.init();
    //bco_y.init();
    //bco_z.init();
    bco_x.exchange().wait();
    bco_y.exchange().wait();
    bco_z.exchange().wait();

    // check fields
#if defined(GHEX_USE_GPU) || defined(GHEX_GPU_MODE_EMULATE)
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    return res;
}

void
sim()
{
    context ctxt(MPI_COMM_WORLD, false);
    // 3D domain decomposition
    arr dims{0, 0, 0}, coords{0, 0, 0};
    MPI_Dims_create(ctxt.size(), 3, dims.data());
    if (ctxt.rank() == 0)
        std::cout << "decomposition : " << dims[0] << " " << dims[1] << " " << dims[2] << std::endl;
    coords[2] = ctxt.rank() / (dims[0] * dims[1]);
    coords[1] = (ctxt.rank() - coords[2] * dims[0] * dims[1]) / dims[0];
    coords[0] = ctxt.rank() - coords[2] * dims[0] * dims[1] - coords[1] * dims[0];
    // make 1 domain per rank
    std::vector<domain> domains{make_domain(ctxt.rank(), coords)};
    // neighbor lookup
    domain_lu d_lu{dims};

    auto staged_pattern =
        structured::regular::make_staged_pattern(ctxt, domains, d_lu, arr{0, 0, 0},
            arr{dims[0] * DIM - 1, dims[1] * DIM - 1, dims[2] * DIM - 1}, halos, periodic);

    // make halo generator
    halo_gen gen{arr{0, 0, 0}, arr{dims[0] * DIM - 1, dims[1] * DIM - 1, dims[2] * DIM - 1}, halos,
        periodic};
    // create a pattern for communication
    auto pattern = make_pattern<structured::grid>(ctxt, gen, domains);
    // run
    bool          res = run(ctxt, pattern, staged_pattern, domains, dims);
    // reduce res
    bool all_res = false;
    MPI_Reduce(&res, &all_res, 1, MPI_C_BOOL, MPI_LAND, 0, MPI_COMM_WORLD);
    if (ctxt.rank() == 0) { EXPECT_TRUE(all_res); }
}

TEST_F(mpi_test_fixture, simple_exchange_3d) { sim(); }
