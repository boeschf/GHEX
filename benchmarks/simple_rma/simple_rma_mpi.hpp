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

struct simulation : public simulation_base<simulation>
{
    simulation(
        int num_reps_,
        int ext_,
        int halo,
        int num_fields_,
        ghex::bench::decomposition& decomp)
    : simulation_base(num_reps_, ext_, halo, num_fields_, decomp)
    {
    }

    void init(int j)
    {
    }

    void step(int j)
    {
    }
};
