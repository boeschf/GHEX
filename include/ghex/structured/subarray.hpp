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
            
            
            struct region_t {
                //void* m_buffer;
                std::size_t m_offset;
                std::vector<std::size_t> m_first;
                std::vector<std::size_t> m_extent;
            };

            inline bool operator<(const region_t& a, const region_t& b) noexcept {
                //return a.m_offset < b.m_offset;
                if (a.m_first.size() < b.m_first.size()) return true;
                if (b.m_first.size() < a.m_first.size()) return false;
                const int dimension = (int)(a.m_first.size());
                for (int dim = dimension; dim>0; --dim) {
                    if (a.m_first[dim-1] < b.m_first[dim-1]) return true;
                    if (b.m_first[dim-1] < a.m_first[dim-1]) return false;
                }
                return false;
            }

            struct subarray_t {
                void* m_buffer;
                void* m_data;
                std::vector<std::size_t> m_stride;
                std::size_t m_num_elements;
                std::vector<region_t> m_regions;
            };

            template<int... Order>
            inline auto make_lookup() {
                const int dimension = sizeof...(Order);
                const int order[dimension] = { Order... };
                std::vector<int> lookup(dimension);
                for (int d=dimension; d>0; --d) {
                    for (int i=0; i<dimension; ++i) {
                        if (order[i] == d-1) {
                            lookup[dimension-d] = i;
                            break;
                        }
                    }
                }
                return lookup;
            }

            template<unsigned int D, typename I, typename Array, typename Lookup>
            inline auto reorder(const Array& array, const Lookup& lu) {
                std::vector<I> result(D);
                for (unsigned int i=0; i<D; ++i)
                    result[i] = array[lu[i]];
                return result;
            }

            template<int... Order, typename Coord, typename Offsets>
            inline region_t make_region_t(
                //void* buffer,
                std::size_t buffer_offset,
                const Coord& first, 
                const Coord& last, 
                const Offsets& offsets)
            {
                const auto lu = make_lookup<Order...>();
                const auto extent = (last-first)+1;
                Coord first_(first);
                for (unsigned int i=0; i<sizeof...(Order); ++i)
                    first_[i] += offsets[i];
                return {buffer_offset,
                        reorder<sizeof...(Order), std::size_t>(first_,lu),
                        reorder<sizeof...(Order),std::size_t>(extent,lu)};
            }
            
            template<int... Order, typename Stride>
            inline subarray_t make_subarray_t(void* buffer, void* data, const Stride& stride, std::size_t num_elements = 0u)
            {
                const auto lu = make_lookup<Order...>();
                return {buffer,data,reorder<sizeof...(Order), std::size_t>(stride,lu), num_elements, {}};
            }

        }
    }
}
                
#endif // INCLUDED_GHEX_STRUCTURED_SUBARRAY_HPP
