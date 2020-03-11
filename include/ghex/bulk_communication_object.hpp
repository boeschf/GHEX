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
#ifndef INCLUDED_GHEX_BULK_COMMUNICATION_OBJECT_HPP
#define INCLUDED_GHEX_BULK_COMMUNICATION_OBJECT_HPP

#include <string>
#include <sstream>
#include <iostream>
//#include <boost/interprocess/ipc/message_queue.hpp>
//#include <boost/interprocess/sync/named_mutex.hpp>
//#include <boost/interprocess/shared_memory_object.hpp>
//#include <boost/interprocess/sync/scoped_lock.hpp>
#include "./communication_object_2.hpp"

#define GHEX_QUEUE_CHUNK 128

namespace gridtools {

    namespace ghex {

        template<typename BulkExchange, typename GridType, typename DomainIdType>
        class bulk_communication_object
        : private communication_object<typename BulkExchange::communicator_type, GridType, DomainIdType> {
        public:
            using communicator_type = typename BulkExchange::communicator_type;
            using base = communication_object<communicator_type,GridType,DomainIdType>;
            using pattern_type            = pattern<communicator_type,GridType,DomainIdType>;
            using pattern_container_type  = pattern_container<communicator_type,GridType,DomainIdType>;
            template<typename D, typename F>
            using buffer_info_type        = buffer_info<pattern_type,D,F>;

        private:
            //BulkExchange m_bulk_exchange;
            
            using data_type = typename communicator_type::mpi_data_type;
            using rank_type = typename communicator_type::rank_type;
            using tag_type  = typename communicator_type::tag_type;

            //boost::interprocess::message_queue* try_open_queue(const std::string& name)
            //{
            //    try {
            //        return new boost::interprocess::message_queue(boost::interprocess::open_only, name.c_str());
            //    }
            //    catch(boost::interprocess::interprocess_exception &ex){
            //        return nullptr;
            //    }
            //}

            //template<class InterprocessObject, typename... Args>
            //InterprocessObject* try_open(const std::string& name, Args&&... args)
            //{
            //    try {
            //        return new InterprocessObject(boost::interprocess::open_only, name.c_str(), std::forward<Args>(args)...);
            //    }
            //    catch(boost::interprocess::interprocess_exception &ex){
            //        return nullptr;
            //    }
            //}

            struct info {
                data_type mpi_type;
                rank_type rank;
                tag_type tag;
                typename communicator_type::template future<void> fut;
            };
            
            //struct pack_info {
            //    std::vector<structured::subarray_t> subarrays;
            //    std::size_t element_size;
            //    std::size_t num_elements;
            //    std::string queue_name;
            //    std::unique_ptr<boost::interprocess::message_queue> msg_queue_ptr;
            //    //std::string shmem_name;
            //    //std::unique_ptr<boost::interprocess::shared_memory_object> shmem_ptr;
            //    //std::string mutex_name;
            //    //std::unique_ptr<boost::interprocess::named_mutex> mutex_ptr;
            //    //boost::interprocess::mapped_region shmem_region;
            //};

            std::vector<info> sends;
            std::vector<info> recvs;

            //std::vector<pack_info> send_queues;
            //std::vector<pack_info> recv_queues;

        public:
            
            template<typename T, typename... Archs, int... Order>
            bulk_communication_object(communicator_type comm,
                buffer_info_type<
                    Archs,
                    structured::simple_field_wrapper<T,cpu,structured::domain_descriptor<DomainIdType,sizeof...(Order)>,Order...>
                >... buffer_infos)
            : base(comm)
            {
                this->prepare_exchange(buffer_infos...);
                using field_type = structured::simple_field_wrapper<T,cpu,structured::domain_descriptor<DomainIdType,sizeof...(Order)>,Order...>;
                auto mem_ptr = &(std::get<typename base::template buffer_memory<cpu>>(this->m_mem));
                for (auto& p0 : mem_ptr->recv_memory)
                    for (auto& p1: p0.second)
                        if (p1.second.size > 0u) {
                            //p1.second.buffer.resize(p1.second.size);
                            //std::stringstream sstr;
                            //sstr << "ghex_queue_A_" << p1.second.address << "_" << this->m_comm.address() << "_" << p1.second.tag;
                            //recv_queues.push_back(pack_info{{}, sizeof(T), p1.second.size/sizeof(T), sstr.str(), {}});
                            ////recv_queues.push_back(pack_info{{}, sizeof(T), p1.second.size/sizeof(T), sstr.str(), {}, sstr.str() + "_mutex", {},{}});
                            std::vector<structured::subarray<Order...>> subarrays;
                            for (auto& f_info : p1.second.field_infos) {
                                auto field_ptr = reinterpret_cast<field_type*>(f_info.field_ptr);
                                T* origin = field_ptr->data();
                                const auto byte_strides = field_ptr->byte_strides();
                                const auto offsets = field_ptr->offsets();
                                subarrays.emplace_back(origin,offsets,byte_strides);
                                //std::size_t buffer_offset = f_info.offset;
                                //recv_queues.back().subarrays.push_back(
                                //    structured::make_subarray_t<Order...>(
                                //        p1.second.buffer.data()+buffer_offset, 
                                //        field_ptr->data(), 
                                //        field_ptr->byte_strides()));
                                //auto& sa = recv_queues.back().subarrays.back();
                                for (const auto& is : *(f_info.index_container)) {
                                    const auto first = is.local().first();
                                    const auto last = is.local().last();
                                    const auto extent = last-first + 1; 
                                    subarrays.back().m_regions.emplace_back(first,extent);
                                    //const auto is_size = is.size();
                                    //sa.m_regions.push_back(
                                    //    structured::make_region_t<Order...>(
                                    //        //p1.second.buffer.data()+buffer_offset,
                                    //        buffer_offset,
                                    //        is.local().first(),
                                    //        is.local().last(),
                                    //        field_ptr->offsets()));
                                    //sa.m_num_elements += is_size;
                                    //buffer_offset += is_size*sizeof(T);
                                }
                                //std::sort(sa.m_regions.begin(), sa.m_regions.end());
                            }
                            recvs.push_back(info{this->m_comm.get_type(subarrays), p1.second.address, p1.second.tag, {}});
                        }
                //if (this->m_comm.rank() == 0)
                //for (const auto& pi : recv_queues) {
                //    std::cout << "recv queue " << pi.queue_name << " has size " << pi.num_elements << " a " << pi.element_size << " bytes" << std::endl; 
                //    for (const auto& sa : pi.subarrays) {
                //        std::cout << "  subarray has size " << sa.m_num_elements << " data at " << sa.m_data << " buffer at " << sa.m_buffer << std::endl;
                //        for (const auto& r : sa.m_regions) {
                //            std::cout << "    region at buffer "<< r.m_buffer << ": " << r.m_first[0] << ", " << r.m_first[1] << ", " << r.m_first[2] << " - " 
                //            << r.m_extent[0] << ", " << r.m_extent[1] << ", " << r.m_extent[2] << std::endl;

                //        }
                //    }
                //}
                for (auto& p0 : mem_ptr->send_memory)
                    for (auto& p1: p0.second)
                        if (p1.second.size > 0u) {
                            //p1.second.buffer.resize(p1.second.size);
                            //std::stringstream sstr;
                            //sstr << "ghex_queue_A_" << this->m_comm.address() << "_" << p1.second.address << "_" << p1.second.tag;
                            //send_queues.push_back(pack_info{{}, sizeof(T), p1.second.size/sizeof(T), sstr.str(), {}});
                            ////send_queues.push_back(pack_info{{}, sizeof(T), p1.second.size/sizeof(T), sstr.str(), {}, sstr.str() + "_mutex", {},{}});
                            std::vector<structured::subarray<Order...>> subarrays;
                            for (auto& f_info : p1.second.field_infos) {
                                auto field_ptr = reinterpret_cast<field_type*>(f_info.field_ptr);
                                T* origin = field_ptr->data();
                                const auto byte_strides = field_ptr->byte_strides();
                                const auto offsets = field_ptr->offsets();
                                subarrays.emplace_back(origin,offsets,byte_strides);
                                //std::size_t buffer_offset = f_info.offset;
                                //send_queues.back().subarrays.push_back(
                                //    structured::make_subarray_t<Order...>(
                                //        p1.second.buffer.data()+buffer_offset, 
                                //        field_ptr->data(), 
                                //        field_ptr->byte_strides()));
                                //auto& sa = send_queues.back().subarrays.back();
                                for (const auto& is : *(f_info.index_container)) {
                                    const auto first = is.local().first();
                                    const auto last = is.local().last();
                                    const auto extent = last-first + 1; 
                                    subarrays.back().m_regions.emplace_back(first,extent);
                                    //const auto is_size = is.size();
                                    //sa.m_regions.push_back(
                                    //    structured::make_region_t<Order...>(
                                    //        //p1.second.buffer.data()+buffer_offset,
                                    //        buffer_offset,
                                    //        is.local().first(),
                                    //        is.local().last(),
                                    //        field_ptr->offsets()));
                                    //sa.m_num_elements += is_size;
                                    //buffer_offset += is_size*sizeof(T);
                                }
                                //std::sort(sa.m_regions.begin(), sa.m_regions.end());
                            }
                            sends.push_back(info{this->m_comm.get_type(subarrays), p1.second.address, p1.second.tag, {}});
                        }
                //if (this->m_comm.rank() == 0)
                //for (const auto& pi : send_queues) {
                //    std::cout << "send queue " << pi.queue_name << " has size " << pi.num_elements << " a " << pi.element_size << " bytes" << std::endl; 
                //    for (const auto& sa : pi.subarrays) {
                //        std::cout << "  subarray has size " << sa.m_num_elements << " data at " << sa.m_data << " buffer at " << sa.m_buffer << std::endl;
                //        for (const auto& r : sa.m_regions) {
                //            std::cout << "    region at buffer "<< r.m_buffer << ": " << r.m_first[0] << ", " << r.m_first[1] << ", " << r.m_first[2] << " - " 
                //            << r.m_extent[0] << ", " << r.m_extent[1] << ", " << r.m_extent[2] << std::endl;

                //        }
                //    }
                //}

                //for (auto& pi : send_queues) {
                //    try{
                //        //Erase previous message queue
                //        boost::interprocess::message_queue::remove(pi.queue_name.c_str());
                //        pi.msg_queue_ptr.reset( new boost::interprocess::message_queue(
                //            boost::interprocess::create_only,
                //            pi.queue_name.c_str(),
                //            pi.num_elements, 
                //            //pi.element_size) );
                //            GHEX_QUEUE_CHUNK) );
                //        //boost::interprocess::named_mutex::remove(pi.mutex_name.c_str());
                //        //pi.mutex_ptr.reset(new boost::interprocess::named_mutex(
                //        //    boost::interprocess::create_only,
                //        //    pi.mutex_name.c_str()));
                //        //boost::interprocess::shared_memory_object::remove(pi.shmem_name.c_str());
                //        //pi.shmem_ptr.reset(new boost::interprocess::shared_memory_object(
                //        //    boost::interprocess::create_only,
                //        //    pi.shmem_name.c_str(),
                //        //    boost::interprocess::read_write));
                //        //pi.shmem_ptr->truncate(pi.num_elements * pi.element_size + 16);
                //        //pi.shmem_region = boost::interprocess::mapped_region(
                //        //    *pi.shmem_ptr,                           // memory-mappable object
                //        //    boost::interprocess::read_write,         // access mode
                //        //    0,                                       // offset from the beginning of shm
                //        //    pi.num_elements * pi.element_size + 16); // length of the region
                //        //*reinterpret_cast<bool*>(pi.shmem_region.get_address()) = false;

                //    } catch(boost::interprocess::interprocess_exception &ex){
                //        std::cout << ex.what() << std::endl;
                //        throw;
                //    }
                //}
    
                //for (auto& pi : recv_queues) {
                //    boost::interprocess::message_queue* mq = nullptr;
                //    while(!mq) { mq = try_open_queue(pi.queue_name); }
                //    pi.msg_queue_ptr.reset(mq);
                //    //boost::interprocess::named_mutex* mtx = nullptr;
                //    //while(!mtx) { mtx = try_open<boost::interprocess::named_mutex>(pi.mutex_name);}
                //    //pi.mutex_ptr.reset(mtx);
                //    //boost::interprocess::shared_memory_object* obj = nullptr;
                //    //while(!obj) { obj = try_open<boost::interprocess::shared_memory_object>(pi.shmem_name, boost::interprocess::read_write);}
                //    //pi.shmem_ptr.reset(obj);
                //    //pi.shmem_region = boost::interprocess::mapped_region(
                //    //    *pi.shmem_ptr,                           // memory-mappable object
                //    //    boost::interprocess::read_write,         // access mode
                //    //    0,                                       // offset from the beginning of shm
                //    //    pi.num_elements * pi.element_size + 16); // length of the region
                //}
            }

            //~bulk_communication_object() {
            //    for (auto& pi : send_queues) {
            //        boost::interprocess::message_queue::remove(pi.queue_name.c_str());
            //        //boost::interprocess::named_mutex::remove(pi.mutex_name.c_str());
            //        //boost::interprocess::shared_memory_object::remove(pi.shmem_name.c_str());
            //    }
            //    for (auto& pi : recv_queues) {
            //        boost::interprocess::message_queue::remove(pi.queue_name.c_str());
            //        //boost::interprocess::named_mutex::remove(pi.mutex_name.c_str());
            //        //boost::interprocess::shared_memory_object::remove(pi.shmem_name.c_str());
            //    }
            //}
            
            bulk_communication_object(bulk_communication_object&&) = default;
            
            //void pack(pack_info& pi) {
            //    while(true) {
            //        boost::interprocess::scoped_lock<boost::interprocess::named_mutex> lock(*pi.mutex_ptr);
            //        bool& finished = *reinterpret_cast<bool*>(pi.shmem_region.get_address());
            //        if (finished)
            //            continue;
            //        finished = true;
            //        unsigned char* buffer = reinterpret_cast<unsigned char*>(pi.shmem_region.get_address())+16;
            //        for (auto& sa : pi.subarrays) {
            //            for (const auto& r : sa.m_regions) {
            //                //std::cout << "  packing region" << std::endl;
            //                pack(buffer, r.m_offset, sa.m_data, sa.m_stride, r, (int)(sa.m_stride.size()-1));
            //            }
            //        }
            //        break;
            //    }
            //}

            //template<typename Stride>
            //std::size_t pack(unsigned char* buffer, std::size_t buffer_offset, 
            //          const void* data, const Stride& stride, const structured::region_t& r, int dim, std::size_t offset = 0u)
            //{
            //    if (dim==0) {
            //        const std::size_t new_offset = offset + (r.m_first[0])*stride[0];
            //        //std::cout << "    writing at offset " << buffer_offset << " extent = " << r.m_extent[0] << ", " << (r.m_extent[0]*stride[0]) << std::endl;
            //        std::memcpy(buffer+buffer_offset, reinterpret_cast<const unsigned char*>(data)+new_offset, r.m_extent[0]*stride[0]);
            //        return buffer_offset + r.m_extent[0]*stride[0];
            //    }
            //    else {
            //        for (std::size_t i=0; i<r.m_extent[dim]; ++i)
            //        {
            //            const std::size_t new_offset = offset + (r.m_first[dim]+i)*stride[dim];
            //            buffer_offset = pack(buffer, buffer_offset, data, stride, r, dim-1, new_offset);
            //        }
            //        //std::cout << "dim = " << dim << ", returning " << buffer_offset << std::endl;
            //        return buffer_offset;
            //    }
            //}
            //
            //void unpack(pack_info& pi) {
            //    while(true) {
            //        boost::interprocess::scoped_lock<boost::interprocess::named_mutex> lock(*pi.mutex_ptr);
            //        bool& finished = *reinterpret_cast<bool*>(pi.shmem_region.get_address());
            //        if (!finished)
            //            continue;
            //        finished = false;
            //        const unsigned char* buffer = reinterpret_cast<const unsigned char*>(pi.shmem_region.get_address())+16;
            //        for (auto& sa : pi.subarrays) {
            //            for (const auto& r : sa.m_regions) {
            //                unpack(buffer, r.m_offset, sa.m_data, sa.m_stride, r, (int)(sa.m_stride.size()-1));
            //            }
            //        }
            //        break;
            //    }
            //}

            //template<typename Stride>
            //std::size_t unpack(const unsigned char* buffer, std::size_t buffer_offset, 
            //          void* data, const Stride& stride, const structured::region_t& r, int dim, std::size_t offset = 0u)
            //{
            //    if (dim==0) {
            //        const std::size_t new_offset = offset + (r.m_first[0])*stride[0];
            //        std::memcpy(reinterpret_cast<unsigned char*>(data)+new_offset, buffer+buffer_offset, r.m_extent[0]*stride[0]);
            //        return buffer_offset + r.m_extent[0]*stride[0];
            //    }
            //    else {
            //        for (std::size_t i=0; i<r.m_extent[dim]; ++i)
            //        {
            //            const std::size_t new_offset = offset + (r.m_first[dim]+i)*stride[dim];
            //            buffer_offset = unpack(buffer, buffer_offset, data, stride, r, dim-1, new_offset);
            //        }
            //        return buffer_offset;
            //    }
            //}
            
            //void pack(pack_info& pi) {
            //    for (auto& sa : pi.subarrays) {
            //        for (const auto& r : sa.m_regions) {
            //            pack(sa.m_data, sa.m_stride, r, *pi.msg_queue_ptr, (int)(sa.m_stride.size()-1));
            //        }
            //    }
            //}

            //void unpack(pack_info& pi) {
            //    for (auto& sa : pi.subarrays) {
            //        for (const auto& r : sa.m_regions) {
            //            unpack(sa.m_data, sa.m_stride, r, *pi.msg_queue_ptr, (int)(sa.m_stride.size()-1));
            //        }
            //    }
            //}

            //template<typename Queue, typename Stride>
            //void pack(void* data, const Stride& stride, const structured::region_t& r, Queue& q, int dim, std::size_t offset = 0)
            //{
            //    if (dim==0) {
            //        //for (std::size_t i=0; i<r.m_extent[0]; ++i)
            //        //{
            //        //    const std::size_t new_offset = offset + (r.m_first[0]+i)*stride[0];
            //        //    q.send((reinterpret_cast<char*>(data)+new_offset), stride[0], 0);
            //        //}
            //        //std::cout << "packing" << std::endl;
            //        offset += (r.m_first[0])*stride[0];
            //        const std::size_t up = ((r.m_extent[0]*stride[0])/GHEX_QUEUE_CHUNK);
            //        for (std::size_t i=0; i<up; ++i) {
            //            const std::size_t new_offset = offset + i*GHEX_QUEUE_CHUNK;
            //            q.send((reinterpret_cast<char*>(data)+new_offset), GHEX_QUEUE_CHUNK, 0);
            //        }
            //        const std::size_t rem = r.m_extent[0]*stride[0] - up*GHEX_QUEUE_CHUNK;
            //        if (rem > 0u) {
            //            const std::size_t new_offset = offset + up*GHEX_QUEUE_CHUNK;
            //            q.send((reinterpret_cast<char*>(data)+new_offset), rem, 0);
            //        }
            //    }
            //    else {
            //        for (std::size_t i=0; i<r.m_extent[dim]; ++i)
            //        {
            //            const std::size_t new_offset = offset + (r.m_first[dim]+i)*stride[dim];
            //            pack(data, stride, r, q, dim-1, new_offset);
            //        }
            //    }
            //}

            //template<typename Queue, typename Stride>
            //void unpack(void* data, const Stride& stride, const structured::region_t& r, Queue& q, int dim, std::size_t offset = 0)
            //{
            //    if (dim==0) {
            //        //for (std::size_t i=0; i<r.m_extent[0]; ++i)
            //        //{
            //        //    unsigned int priority;
            //        //    boost::interprocess::message_queue::size_type recvd_size;
            //        //    const std::size_t new_offset = offset + (r.m_first[0]+i)*stride[0];
            //        //    q.receive((reinterpret_cast<char*>(data)+new_offset), stride[0], recvd_size, priority);
            //        //}
            //        //std::cout << "unpacking" << std::endl;
            //        //std::cout << "initial offset = " << offset << std::endl;
            //        offset += (r.m_first[0])*stride[0];
            //        //std::cout << "offset1 = " << offset << std::endl;
            //        const std::size_t up = ((r.m_extent[0]*stride[0])/GHEX_QUEUE_CHUNK);
            //        for (std::size_t i=0; i<up; ++i) {
            //            unsigned int priority;
            //            boost::interprocess::message_queue::size_type recvd_size;
            //            const std::size_t new_offset = offset + i*GHEX_QUEUE_CHUNK;
            //            q.receive((reinterpret_cast<char*>(data)+new_offset), GHEX_QUEUE_CHUNK, recvd_size, priority);
            //        }
            //        const std::size_t rem = r.m_extent[0]*stride[0] - up*GHEX_QUEUE_CHUNK;
            //        //std::cout << "up is " << up << std::endl;
            //        //std::cout << "remainder is " << rem << std::endl;
            //        if (rem > 0u) {
            //            unsigned int priority;
            //            boost::interprocess::message_queue::size_type recvd_size;
            //            const std::size_t new_offset = offset + up*GHEX_QUEUE_CHUNK;
            //            //std::cout << "new offset = " << new_offset << std::endl;
            //            unsigned char buffer[GHEX_QUEUE_CHUNK];
            //            q.receive(buffer, GHEX_QUEUE_CHUNK, recvd_size, priority);
            //            //q.receive((reinterpret_cast<char*>(data)+new_offset), rem, recvd_size, priority);
            //            //std::cout << "recvd_size " << recvd_size << std::endl;
            //            std::memcpy((reinterpret_cast<char*>(data)+new_offset), buffer, rem);
            //        }
            //    }
            //    else {
            //        for (std::size_t i=0; i<r.m_extent[dim]; ++i)
            //        {
            //            const std::size_t new_offset = offset + (r.m_first[dim]+i)*stride[dim];
            //            unpack(data, stride, r, q, dim-1, new_offset);
            //        }
            //    }
            //}

            //void exchange() {
            //    // pack
            //    //std::cout << "packing" << std::endl;
            //    for (auto& pi : send_queues) pack(pi);
            //    // unpack
            //    for (auto& pi : recv_queues) unpack(pi);
            //    //std::vector<bool> finished(recv_queues.size(), false);

            //}
            
            void exchange() {
                for (auto& i : recvs) i.fut = this->m_comm.recv((void*)0, i.mpi_type, i.rank, i.tag);
                for (auto& i : sends) i.fut = this->m_comm.send((const void*)0, i.mpi_type, i.rank, i.tag);
                for (auto& i : sends) i.fut.wait();
                for (auto& i : recvs) i.fut.wait();
            }

            //template<typename... Archs, typename... Fields>
            //bulk_communication_object(communicator_type comm, buffer_info_type<Archs,Fields>... buffer_infos)
            //: base(comm)
            //, m_bulk_exchange(comm)
            //{
            //    this->prepare_exchange(buffer_infos...);
            //    // register sends and recv
            //    detail::for_each(this->m_mem, [this](auto& m)
            //    {
            //        for (auto& p0 : m.recv_memory)
            //            for (auto& p1: p0.second)
            //                if (p1.second.size > 0u)
            //                {
            //                    p1.second.buffer.resize(p1.second.size);
            //                    m_bulk_exchange.register_recv(p1.second.buffer, p1.second.address, p1.second.tag);
            //                }
            //        for (auto& p0 : m.send_memory)
            //            for (auto& p1: p0.second)
            //                if (p1.second.size > 0u)
            //                {
            //                    p1.second.buffer.resize(p1.second.size);
            //                    p1.second.bulk_send_id =
            //                    m_bulk_exchange.register_send(p1.second.buffer, p1.second.address, p1.second.tag).m_index;
            //                }
            //    });
            //    m_bulk_exchange.sync();
            //}

            //void exchange() {
            //    m_bulk_exchange.start_epoch();
            //    detail::for_each(this->m_mem, [this](auto& m)
            //    {
            //        using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
            //        packer<arch_type>::bulk_pack(m,m_bulk_exchange);
            //    });
            //    m_bulk_exchange.end_epoch();
            //    detail::for_each(this->m_mem, [this](auto& m)
            //    {
            //        using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
            //        packer<arch_type>::bulk_unpack(m);
            //    });
            //}

            /*void start_exchange() {
                m_bulk_exchange.start_epoch();
                detail::for_each(this->m_mem, [this](auto& m)
                {
                    using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
                    packer<arch_type>::bulk_pack(m,m_bulk_exchange);
                });
            }

            void end_exchange() {
                m_bulk_exchange.end_epoch();
                detail::for_each(this->m_mem, [this](auto& m)
                {
                    using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
                    packer<arch_type>::bulk_unpack(m);
                });
            }*/
        };


        template<class BulkExchange, class GridType, class DomainIdType, class... Archs, class... Fields>
        auto make_bulk_communication_object(typename BulkExchange::communicator_type comm,
             buffer_info<pattern<typename BulkExchange::communicator_type, GridType, DomainIdType>, Archs, Fields>... buffer_infos) {
            return bulk_communication_object<BulkExchange,GridType,DomainIdType>(comm, buffer_infos...);
        }

    } // namespace ghex
        
} // namespace gridtools

#endif /* INCLUDED_GHEX_BULK_COMMUNICATION_OBJECT_HPP */
