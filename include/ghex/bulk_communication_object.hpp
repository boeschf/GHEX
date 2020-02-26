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

#include "./communication_object_2.hpp"

namespace gridtools {

    namespace ghex {

        template<typename BulkExchange, typename GridType, typename DomainIdType>
        class bulk_communication_object
        : private communication_object<typename BulkExchange::communicator_type, GridType, DomainIdType> {
        public:
            using communicator_type = typename BulkExchange::communicator_type;
            using base = communication_object<communicator_type,GridType,DomainIdType>;
            //using grid_type               = GridType;
            //using domain_id_type          = DomainIdType;
            using pattern_type            = pattern<communicator_type,GridType,DomainIdType>;
            using pattern_container_type  = pattern_container<communicator_type,GridType,DomainIdType>;
            template<typename D, typename F>
            using buffer_info_type        = buffer_info<pattern_type,D,F>;

        private:
            //co_type m_co;
            BulkExchange m_bulk_exchange;

        public:

            template<typename... Archs, typename... Fields>
            bulk_communication_object(communicator_type comm, buffer_info_type<Archs,Fields>... buffer_infos)
            : base(comm)
            , m_bulk_exchange(comm)
            {
                this->prepare_exchange(buffer_infos...);
                // register sends and recv
                detail::for_each(this->m_mem, [this](auto& m)
                {
                    for (auto& p0 : m.recv_memory)
                        for (auto& p1: p0.second)
                            if (p1.second.size > 0u)
                            {
                                p1.second.buffer.resize(p1.second.size);
                                m_bulk_exchange.register_recv(p1.second.buffer, p1.second.address, p1.second.tag);
                            }
                    for (auto& p0 : m.send_memory)
                        for (auto& p1: p0.second)
                            if (p1.second.size > 0u)
                            {
                                p1.second.buffer.resize(p1.second.size);
                                p1.second.bulk_send_id =
                                m_bulk_exchange.register_send(p1.second.buffer, p1.second.address, p1.second.tag).m_index;
                            }
                });
                m_bulk_exchange.sync();
            }

            void exchange() {
                m_bulk_exchange.start_epoch();
                detail::for_each(this->m_mem, [this](auto& m)
                {
                    using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
                    packer<arch_type>::bulk_pack(m,m_bulk_exchange);
                });
                m_bulk_exchange.end_epoch();
                detail::for_each(this->m_mem, [this](auto& m)
                {
                    using arch_type = typename std::remove_reference_t<decltype(m)>::arch_type;
                    packer<arch_type>::bulk_unpack(m);
                });
            }
        };


        template<class BulkExchange, class GridType, class DomainIdType, class... Archs, class... Fields>
        auto make_bulk_communication_object(typename BulkExchange::communicator_type comm,
             buffer_info<pattern<typename BulkExchange::communicator_type, GridType, DomainIdType>, Archs, Fields>... buffer_infos) {
            return bulk_communication_object<BulkExchange,GridType,DomainIdType>(comm, buffer_infos...);
        }

    } // namespace ghex
        
} // namespace gridtools

#endif /* INCLUDED_GHEX_BULK_COMMUNICATION_OBJECT_HPP */
