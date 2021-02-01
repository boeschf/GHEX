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

#include <vector>
#include "./simple_rma_base.hpp"
#include <ghex/transport_layer/mpi/error.hpp>

struct simulation : public simulation_base<simulation>
{
    struct mpi_dtype_deleter
    {
        void operator()(MPI_Datatype* type)
        {
            MPI_Type_free(type);
            delete type;
        }
    };

    using mpi_dtype_unique_ptr = std::unique_ptr<MPI_Datatype,mpi_dtype_deleter>;
    using domain_t = ghex::bench::decomposition::domain_t;

    struct neighborhood
    {
        domain_t d;
        domain_t x_l, x_r;
        domain_t y_l, y_r;
        domain_t z_l, z_r;
    };

    MPI_Comm comm;
    MPI_Datatype mpi_T;
    mpi_dtype_unique_ptr x_recv_l, x_recv_r;
    mpi_dtype_unique_ptr x_send_l, x_send_r;
    mpi_dtype_unique_ptr y_recv_l, y_recv_r;
    mpi_dtype_unique_ptr y_send_l, y_send_r;
    mpi_dtype_unique_ptr z_recv_l, z_recv_r;
    mpi_dtype_unique_ptr z_send_l, z_send_r;
    std::vector<neighborhood> neighbors;

    simulation(
        int num_reps_,
        int ext_,
        int halo,
        int num_fields_,
        bool check_,
        ghex::bench::decomposition& decomp)
    : simulation_base(num_reps_, ext_, halo, num_fields_, check_, decomp)
    , comm{decomp.mpi_comm()}
    {
        for (int i=0; i<num_threads; ++i)
        {
            neighbors.push_back({
                decomp.domain(i),
                decomp.neighbor(i,-1, 0, 0),
                decomp.neighbor(i, 1, 0, 0),
                decomp.neighbor(i, 0,-1, 0),
                decomp.neighbor(i, 0, 1, 0),
                decomp.neighbor(i, 0, 0,-1),
                decomp.neighbor(i, 0, 0, 1)});
            //const auto& n = neighbors.back();
            //std::cout << "thread " << i << "\n"
            //    << "  " << n.d.id << " : " << n.d.rank << " : " << n.d.thread 
            //        << " - " << n.d.coord[0] << " " << n.d.coord[1] << " " << n.d.coord[2] << "\n"
            //    << "  " << n.x_l.id << " : " << n.x_l.rank << " : " << n.x_l.thread 
            //        << " - " << n.x_l.coord[0] << " " << n.x_l.coord[1] << " " << n.x_l.coord[2] << "\n"
            //    << "  " << n.x_r.id << " : " << n.x_r.rank << " : " << n.x_r.thread 
            //        << " - " << n.x_r.coord[0] << " " << n.x_r.coord[1] << " " << n.x_r.coord[2] << "\n"
            //    << "  " << n.y_l.id << " : " << n.y_l.rank << " : " << n.y_l.thread 
            //        << " - " << n.y_l.coord[0] << " " << n.y_l.coord[1] << " " << n.y_l.coord[2] << "\n"
            //    << "  " << n.y_r.id << " : " << n.y_r.rank << " : " << n.y_r.thread 
            //        << " - " << n.y_r.coord[0] << " " << n.y_r.coord[1] << " " << n.y_r.coord[2] << "\n"
            //    << "  " << n.z_l.id << " : " << n.z_l.rank << " : " << n.z_l.thread 
            //        << " - " << n.z_l.coord[0] << " " << n.z_l.coord[1] << " " << n.z_l.coord[2] << "\n"
            //    << "  " << n.z_r.id << " : " << n.z_r.rank << " : " << n.z_r.thread 
            //        << " - " << n.z_r.coord[0] << " " << n.z_r.coord[1] << " " << n.z_r.coord[2] << "\n"
            //    << std::endl;
        }
        
        MPI_Type_match_size(MPI_TYPECLASS_REAL, sizeof(T), &mpi_T);

        {
        x_recv_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        x_recv_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        x_send_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        x_send_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        const int x_recv_l_dims[3] = {halos[0],local_ext[1],local_ext[2]};
        const int x_recv_r_dims[3] = {halos[1],local_ext[1],local_ext[2]};
        const int x_recv_l_offsets[3] = {0,halos[2],halos[4]};
        const int x_recv_r_offsets[3] = {halos[0]+local_ext[0],halos[2],halos[4]};
        const int x_send_l_offsets[3] = {halos[0],halos[2],halos[4]};
        const int x_send_r_offsets[3] = {halos[0]+local_ext[0]-halos[1],halos[2],halos[4]};
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), x_recv_l_dims, x_recv_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, x_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), x_recv_r_dims, x_recv_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, x_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), x_recv_r_dims, x_send_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, x_send_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), x_recv_l_dims, x_send_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, x_send_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(x_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(x_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(x_send_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(x_send_r.get()));
        }

        {
        y_recv_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        y_recv_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        y_send_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        y_send_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        const int y_recv_l_dims[3] = {local_ext_buffer[0],halos[2],local_ext[2]};
        const int y_recv_r_dims[3] = {local_ext_buffer[0],halos[3],local_ext[2]};
        const int y_recv_l_offsets[3] = {0,0,halos[4]};
        const int y_recv_r_offsets[3] = {0,halos[2]+local_ext[1],halos[4]};
        const int y_send_l_offsets[3] = {0,halos[2],halos[4]};
        const int y_send_r_offsets[3] = {0,halos[2]+local_ext[1]-halos[3],halos[4]};
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), y_recv_l_dims, y_recv_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, y_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), y_recv_r_dims, y_recv_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, y_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), y_recv_r_dims, y_send_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, y_send_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), y_recv_l_dims, y_send_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, y_send_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(y_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(y_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(y_send_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(y_send_r.get()));
        }

        {
        z_recv_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        z_recv_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        z_send_l = mpi_dtype_unique_ptr(new MPI_Datatype);
        z_send_r = mpi_dtype_unique_ptr(new MPI_Datatype);
        const int z_recv_l_dims[3] = {local_ext_buffer[0],local_ext_buffer[1],halos[4]};
        const int z_recv_r_dims[3] = {local_ext_buffer[0],local_ext_buffer[1],halos[5]};
        const int z_recv_l_offsets[3] = {0,0,0};
        const int z_recv_r_offsets[3] = {0,0,halos[4]+local_ext[2]};
        const int z_send_l_offsets[3] = {0,0,halos[4]};
        const int z_send_r_offsets[3] = {0,0,halos[4]+local_ext[2]-halos[5]};
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), z_recv_l_dims, z_recv_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, z_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), z_recv_r_dims, z_recv_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, z_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), z_recv_r_dims, z_send_l_offsets,
                MPI_ORDER_FORTRAN, mpi_T, z_send_l.get()));
        GHEX_CHECK_MPI_RESULT(
            MPI_Type_create_subarray(3, local_ext_buffer.data(), z_recv_l_dims, z_send_r_offsets,
                MPI_ORDER_FORTRAN, mpi_T, z_send_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(z_recv_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(z_recv_r.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(z_send_l.get()));
        GHEX_CHECK_MPI_RESULT(MPI_Type_commit(z_send_r.get()));
        }
        MPI_Barrier(comm);
    }

    void init(int) 
    {
    }

    void step(int j)
    {
        for (int i=0; i<num_fields; ++i)
        {
            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *x_send_l, neighbors[j].x_l.rank, i + num_fields*0 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *x_recv_r, neighbors[j].x_r.rank, i + num_fields*0 + num_fields*6*neighbors[j].x_r.thread,
                comm, MPI_STATUS_IGNORE));
            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *x_send_r, neighbors[j].x_r.rank, i + num_fields*1 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *x_recv_l, neighbors[j].x_l.rank, i + num_fields*1 + num_fields*6*neighbors[j].x_l.thread,
                comm, MPI_STATUS_IGNORE));

            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *y_send_l, neighbors[j].y_l.rank, i + num_fields*2 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *y_recv_r, neighbors[j].y_r.rank, i + num_fields*2 + num_fields*6*neighbors[j].y_r.thread,
                comm, MPI_STATUS_IGNORE));
            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *y_send_r, neighbors[j].y_r.rank, i + num_fields*3 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *y_recv_l, neighbors[j].y_l.rank, i + num_fields*3 + num_fields*6*neighbors[j].y_l.thread,
                comm, MPI_STATUS_IGNORE));

            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *z_send_l, neighbors[j].z_l.rank, i + num_fields*4 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *z_recv_r, neighbors[j].z_r.rank, i + num_fields*4 + num_fields*6*neighbors[j].z_r.thread,
                comm, MPI_STATUS_IGNORE));
            GHEX_CHECK_MPI_RESULT(MPI_Sendrecv(
                raw_fields[j][i].hd_data(), 1, *z_send_r, neighbors[j].z_r.rank, i + num_fields*5 + num_fields*6*neighbors[j].d.thread,
                raw_fields[j][i].hd_data(), 1, *z_recv_l, neighbors[j].z_l.rank, i + num_fields*5 + num_fields*6*neighbors[j].z_l.thread,
                comm, MPI_STATUS_IGNORE));
        }
    }
};
