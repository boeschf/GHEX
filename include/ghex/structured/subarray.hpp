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
#ifndef INCLUDED_GHEX_STRUCTURED_SUBARRAY_HPP
#define INCLUDED_GHEX_STRUCTURED_SUBARRAY_HPP

#include <array>
#include <vector>
#include <algorithm>
#include <iterator>
#include <gridtools/common/layout_map.hpp>
#include "../common/coordinate.hpp"

namespace gridtools {
    namespace ghex {
        namespace structured {

            template<unsigned int D>
            struct region {
                using coordinate_type = coordinate<std::array<int,D>>;
                coordinate_type m_first;
                coordinate_type m_extent;
                region(const coordinate_type& first, const coordinate_type& extent)
                : m_first{first}, m_extent{extent}
                {}
            };
            
            template<int... Order>
            struct subarray {
                using layout_map  = gridtools::layout_map<Order...>;
                using offset_type = coordinate<std::array<int,sizeof...(Order)>>;
                using stride_type = coordinate<std::array<std::size_t,sizeof...(Order)>>;
                using region_type = region<sizeof...(Order)>;

                std::size_t              m_size;
                void*                    m_data;
                offset_type              m_offset;
                stride_type              m_stride;
                std::vector<region_type> m_regions;

                template<typename T, typename Offsets, typename Strides>
                subarray(T* data, const Offsets& o, const Strides& s)
                : m_size{sizeof(T)}, m_data{data} {
                    std::copy(std::begin(o), std::end(o), std::begin(m_offset));
                    std::copy(std::begin(s), std::end(s), std::begin(m_stride));
                }
            };
        }
    }
}
                
#endif // INCLUDED_GHEX_STRUCTURED_SUBARRAY_HPP
