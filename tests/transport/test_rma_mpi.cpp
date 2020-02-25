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
#include <ghex/threads/std_thread/primitives.hpp>
#include <ghex/threads/atomic/primitives.hpp>
#include <ghex/threads/none/primitives.hpp>
#include <iostream>
#include <iomanip>

#include <gtest/gtest.h>

#include <ghex/transport_layer/mpi/context.hpp>
using transport = gridtools::ghex::tl::mpi_tag;

using threading_std = gridtools::ghex::threads::std_thread::primitives;
using threading_atc = gridtools::ghex::threads::atomic::primitives;
using threading_non = gridtools::ghex::threads::none::primitives;
using threading = threading_atc;

const std::size_t size = 10*1024;

//TEST(rma_mpi, window) {
//    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
//    auto& context = *context_ptr;
//
//    MPI_Info info;
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Info_create(&info)
//    )
//    //GHEX_CHECK_MPI_RESULT(
//    //    MPI_Info_set(info, "no_locks", "true")
//    //)
//    MPI_Win win;
//    /*GHEX_CHECK_MPI_RESULT(
//        MPI_Win_create_dynamic(info, MPI_COMM_WORLD, &win)
//    )
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Info_free(&info)
//    )*/
//
//    void* memory;
//    using T = double;
//
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Win_allocate(size*sizeof(T)+sizeof(unsigned int), 1, info, MPI_COMM_WORLD, &memory, &win)
//    )
//
//    if (context.rank() == 0)
//    {
//        /*GHEX_CHECK_MPI_RESULT(
//            MPI_Alloc_mem(size*sizeof(T)+sizeof(unsigned int), MPI_INFO_NULL, &memory)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_attach(win, memory, size*sizeof(T)+sizeof(unsigned int))
//        )
//        MPI_Aint memory_address;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Recv(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE)
//        )*/
//        GHEX_CHECK_MPI_RESULT(
//            //MPI_Win_lock_all(0, win)
//            MPI_Win_lock(MPI_LOCK_SHARED, 1, 0, win)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_lock(MPI_LOCK_SHARED, 1, 0, win)
//        )
//        /*MPI_Request req;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Rput(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                memory_address,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win,
//                &req)
//        )
//        MPI_Status status;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Wait(&req, &status)
//        )
//        */
//        /*GHEX_CHECK_MPI_RESULT(
//            MPI_Put(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                memory_address,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win)
//        )*/
//        *(reinterpret_cast<unsigned int*>((T*)memory+size)) = 99;
//        *((T*)memory) = 77;
//        *((T*)memory + size/2) = 88;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Put(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                0,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win)
//        )
//
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_flush_local(1, win)
//        )
//
//        GHEX_CHECK_MPI_RESULT(
//        //    MPI_Win_unlock_all(win)
//            MPI_Win_unlock(1, win)
//        )
//        //GHEX_CHECK_MPI_RESULT(
//        //    MPI_Win_fence(0, win)
//        //)
//
//    }
//    else if (context.rank() == 1)
//    {
//        /*GHEX_CHECK_MPI_RESULT(
//            MPI_Alloc_mem(size*sizeof(T)+sizeof(unsigned int), MPI_INFO_NULL, &memory)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_attach(win, memory, size*sizeof(T)+sizeof(unsigned int))
//        )
//        MPI_Aint memory_address;
//        void* win_ptr;
//        MPI_Aint win_address;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Get_address(memory, &memory_address)
//        )
//        int flag;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_get_attr(win, MPI_WIN_BASE, &win_ptr, &flag)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Get_address(win_ptr, &win_address)
//        )
//        std::cout << "memory address: " << memory_address << std::endl;
//        std::cout << "wîndow address: " << win_address << std::endl;
//        memory_address = memory_address - win_address;
//        std::cout << "memory address: " << memory_address << std::endl;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Send(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 0, 0, MPI_COMM_WORLD)
//        )
//        //GHEX_CHECK_MPI_RESULT(
//        //    MPI_Win_fence(0, win)
//        //)*/
//        for (int i=0; i<100; ++i)
//            std::cout << *((T*)memory) << " " << *((T*)memory + size/2) << " " << *(reinterpret_cast<unsigned int*>((T*)memory+size)) << std::endl;
//    }
//    //MPI_Barrier(MPI_COMM_WORLD);
//
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Win_free(&win)
//    )
//        
//    /*if (context.rank() < 2)
//    {
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Free_mem(memory)
//        )
//    }*/
//}

//TEST(rma_mpi, window_dyn) {
//    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
//    auto& context = *context_ptr;
//
//    MPI_Info info;
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Info_create(&info)
//    )
//    //GHEX_CHECK_MPI_RESULT(
//    //    MPI_Info_set(info, "no_locks", "true")
//    //)
//    MPI_Win win;
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Win_create_dynamic(info, MPI_COMM_WORLD, &win)
//    )
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Info_free(&info)
//    )
//
//    void* memory;
//    using T = double;
//
//    if (context.rank() == 0)
//    {
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Alloc_mem(size*sizeof(T)+sizeof(unsigned int), MPI_INFO_NULL, &memory)
//        )
//        //GHEX_CHECK_MPI_RESULT(
//        //    MPI_Win_attach(win, memory, size*sizeof(T)+sizeof(unsigned int))
//        //)
//        MPI_Aint memory_address;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Recv(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_lock(MPI_LOCK_SHARED, 1, 0, win)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_lock(MPI_LOCK_SHARED, 1, 0, win)
//        )
//        /*MPI_Request req;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Rput(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                memory_address,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win,
//                &req)
//        )
//        MPI_Status status;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Wait(&req, &status)
//        )
//        */
//        /*GHEX_CHECK_MPI_RESULT(
//            MPI_Put(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                memory_address,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win)
//        )*/
//        *(reinterpret_cast<unsigned int*>((T*)memory+size)) = 99;
//        *((T*)memory) = 77;
//        *((T*)memory + size/2) = 88;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Put(
//                memory, 
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                1,
//                memory_address,
//                size*sizeof(T)+sizeof(unsigned int),
//                MPI_BYTE, 
//                win)
//        )
//
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_flush_local(1, win)
//        )
//
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_unlock(1, win)
//        )
//    }
//    else if (context.rank() == 1)
//    {
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Alloc_mem(size*sizeof(T)+sizeof(unsigned int), MPI_INFO_NULL, &memory)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_attach(win, memory, size*sizeof(T)+sizeof(unsigned int))
//        )
//        MPI_Aint memory_address;
//        void* win_ptr;
//        MPI_Aint win_address;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Get_address(memory, &memory_address)
//        )
//        int flag;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Win_get_attr(win, MPI_WIN_BASE, &win_ptr, &flag)
//        )
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Get_address(win_ptr, &win_address)
//        )
//        std::cout << "memory address: " << memory_address << std::endl;
//        std::cout << "window address: " << win_address << std::endl;
//        memory_address = memory_address - win_address;
//        std::cout << "memory address: " << memory_address << std::endl;
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Send(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 0, 0, MPI_COMM_WORLD)
//        )
//        //GHEX_CHECK_MPI_RESULT(
//        //    MPI_Win_fence(0, win)
//        //)*/
//        //for (int i=0; i<100; ++i)
//        //    std::cout << *((T*)memory) << " " << *((T*)memory + size/2) << " " << *(reinterpret_cast<unsigned int*>((T*)memory+size)) << std::endl;
//    }
//    //MPI_Barrier(MPI_COMM_WORLD);
//
//    GHEX_CHECK_MPI_RESULT(
//        MPI_Win_free(&win)
//    )
//        
//    if (context.rank() < 2)
//    {
//        GHEX_CHECK_MPI_RESULT(
//            MPI_Free_mem(memory)
//        )
//    }
//}


TEST(rma_mpi, window_active_group) {
    
    const int num_threads = 3;
    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(num_threads, MPI_COMM_WORLD);
    auto& context = *context_ptr;


    auto func = [&context]() {
        auto token = context.get_token();
        auto comm  = context.get_communicator(token);

        const auto rank = comm.rank();
        const auto size = comm.size();
        const auto left_rank = ((rank+size)-1) % size;
        const auto right_rank = (rank+1) % size;
        const auto tag = token.id();

        const auto buffer_size = 1;
        auto send_msg = comm.make_message<>(buffer_size);
        auto recv_msg = comm.make_message<>(buffer_size);

        //comm.register_send(send_msg, right_rank, tag);
        //comm.register_recv(recv_msg, left_rank, tag);
        comm.sync_register();

        for (int i=0; i<10; ++i) {
            comm.post_bulk();
            comm.wait_bulk();
        }

        comm.register_send(send_msg, right_rank, tag);
        comm.register_recv(recv_msg, left_rank, tag);
        comm.sync_register();

        for (int i=0; i<10; ++i) {
            comm.post_bulk();
            comm.wait_bulk();
        }

    };


    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i=0; i<num_threads; ++i)
        threads.push_back(std::thread{func});

    for (auto& t : threads)
        t.join();

    

    //MPI_Info info;
    //GHEX_CHECK_MPI_RESULT(
    //    MPI_Info_create(&info)
    //)
    //GHEX_CHECK_MPI_RESULT(
    //    MPI_Info_set(info, "no_locks", "true")
    //)
    //MPI_Win win;

    //void* memory;
    //using T = double;
    //int rank;
    //MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //GHEX_CHECK_MPI_RESULT(
    //    //MPI_Win_allocate(size*sizeof(T)+sizeof(unsigned int), 1, MPI_INFO_NULL, MPI_COMM_WORLD, &memory, &win)
    //    MPI_Win_create_dynamic(info, MPI_COMM_WORLD, &win)
    //)

    //MPI_Group group;
    //GHEX_CHECK_MPI_RESULT(
    //    MPI_Win_get_group(win, &group)
    //)

    //GHEX_CHECK_MPI_RESULT(
    //    MPI_Win_fence(0, win)
    //)

    //if (rank == 0)
    //{
    //    memory = new char[size*sizeof(T)+sizeof(unsigned int)];
    //    MPI_Aint memory_address;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Recv(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE)
    //    )
    //    MPI_Group access_group;
    //    int access_ranks[2] = {1,2};
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Group_incl(group, 2, access_ranks, &access_group)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_start(access_group, 0, win)
    //        //MPI_Win_start(access_group, MPI_MODE_NOCHECK, win)
    //    )
    //    *(reinterpret_cast<unsigned int*>((T*)memory+size)) = 99;
    //    *((T*)memory) = 77;
    //    *((T*)memory + size/2) = 88;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Put(
    //            memory, 
    //            size*sizeof(T)+sizeof(unsigned int),
    //            MPI_BYTE, 
    //            1,
    //            memory_address,
    //            size*sizeof(T)+sizeof(unsigned int),
    //            MPI_BYTE, 
    //            win)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_complete(win)
    //    )
    //}
    //else if (rank == 1)
    //{
    //    //memory = new char[size*sizeof(T)+sizeof(unsigned int)];
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Alloc_mem(size*sizeof(T)+sizeof(unsigned int),  MPI_INFO_NULL, &memory)
    //    )
    //    *((double*)memory) = 100; 
    //    std::cout << *((double*)memory) << std::endl;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_attach(win, memory, size*sizeof(T)+sizeof(unsigned int))
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_sync(win)
    //    )
    //    MPI_Aint memory_address;
    //    void* win_ptr;
    //    MPI_Aint win_address;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Get_address(memory, &memory_address)
    //    )
    //    int flag;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_get_attr(win, MPI_WIN_BASE, &win_ptr, &flag)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Get_address(win_ptr, &win_address)
    //    )
    //    std::cout << "memory address: " << memory_address << std::endl;
    //    std::cout << "window address: " << win_address << std::endl;
    //    memory_address = memory_address - win_address;
    //    memory_address = reinterpret_cast<char*>(memory) - reinterpret_cast<char*>(MPI_BOTTOM);
    //    std::cout << "memory address: " << memory_address << std::endl;
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Send(&memory_address, sizeof(MPI_Aint), MPI_BYTE, 0, 0, MPI_COMM_WORLD)
    //    )
    //    MPI_Group exposure_group;
    //    int exposure_ranks[1] = {0};
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Group_incl(group, 1, exposure_ranks, &exposure_group)
    //    )
    //    MPI_Group access_group;
    //    int access_ranks[1] = {2};
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Group_incl(group, 1, access_ranks, &access_group)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_start(access_group, 0, win)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_post(exposure_group, 0, win)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_wait(win)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_complete(win)
    //    )
    //}
    //else if (rank == 2)
    //{
    //    MPI_Group exposure_group;
    //    int exposure_ranks[2] = {0,1};
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Group_incl(group, 2, exposure_ranks, &exposure_group)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_post(exposure_group, 0, win)
    //    )
    //    GHEX_CHECK_MPI_RESULT(
    //        MPI_Win_wait(win)
    //    )
    //}

    //GHEX_CHECK_MPI_RESULT(
    //    MPI_Win_free(&win)
    //)

}


//TEST(rma_mpi, window_active_group) {
//    auto context_ptr = gridtools::ghex::tl::context_factory<transport,threading>::create(1, MPI_COMM_WORLD);
//    auto& context = *context_ptr;
////Start up MPI...
//    MPI_Group comm_group, group;
//    
//    int ranks[3];
//
//    for (int i=0;i<3;i++) {
//        ranks[i] = i;     //For forming groups, later
//    }
//    MPI_Comm_group(MPI_COMM_WORLD,&comm_group);
//
//    auto buf = new int[3];
//    //auto rank = context.rank();
//    int rank;
//    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
//    MPI_Win win;
//
//    /* Create new window for this comm */
//    if (rank == 0) {
//        MPI_Win_create(buf,sizeof(int)*3,sizeof(int),
//            MPI_INFO_NULL,MPI_COMM_WORLD,&win);
//    }
//    else {
//        /* Rank 1 or 2 */
//        MPI_Win_create(NULL,0,sizeof(int),
//            MPI_INFO_NULL,MPI_COMM_WORLD,&win);
//    }
//    /* Now do the communication epochs */
//    if (rank == 0) {
//        /* Origin group consists of ranks 1 and 2 */
//        MPI_Group_incl(comm_group,2,ranks+1,&group);
//        /* Begin the exposure epoch */
//        MPI_Win_post(group,0,win);
//        /* Wait for epoch to end */
//        MPI_Win_wait(win);
//    }
//    else {
//        /* Target group consists of rank 0 */
//        MPI_Group_incl(comm_group,1,ranks,&group);
//        /* Begin the access epoch */
//        MPI_Win_start(group,0,win);
//        /* Put into rank==0 according to my rank */
//        MPI_Put(buf,1,MPI_INT,0,rank,1,MPI_INT,win);
//        /* Terminate the access epoch */
//        MPI_Win_complete(win);
//    }
//
//    /* Free window and groups */
//    MPI_Win_free(&win);
//    MPI_Group_free(&group);
//    MPI_Group_free(&comm_group);
//    
//    //Shut down...
//}
