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
#ifndef INCLUDED_GHEX_TRANSPORT_LAYER_MPI_SUBARRAY_HPP
#define INCLUDED_GHEX_TRANSPORT_LAYER_MPI_SUBARRAY_HPP

#include <map>
#include "./error.hpp"
#include "../../structured/subarray.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace mpi {

                struct region {
                    int m_dimension;
                    std::vector<int> m_extent;
                    std::vector<int> m_stride;
                };

                inline bool operator<(const region& a, const region& b) noexcept {
                    if (a.m_dimension < b.m_dimension) return true;
                    if (b.m_dimension < a.m_dimension) return false;
                    for (int i=0; i<a.m_dimension; ++i) {
                        if (a.m_extent[i] < b.m_extent[i]) return true;
                        if (b.m_extent[i] < a.m_extent[i]) return false;
                    }
                    for (int i=0; i<a.m_dimension; ++i) {
                        if (a.m_stride[i] < b.m_stride[i]) return true;
                        if (b.m_stride[i] < a.m_stride[i]) return false;
                    }
                    return false;
                }

                struct region_type {
                    MPI_Aint m_offset;
                    std::pair<const region, MPI_Datatype>* m_map_ptr;
                };
                
                inline bool operator<(const region_type& a, const region_type& b) noexcept {
                    if (a.m_offset < b.m_offset) return true;
                    if (b.m_offset < a.m_offset) return false;
                    if (a.m_map_ptr < b.m_map_ptr) return true;
                    if (b.m_map_ptr < a.m_map_ptr) return false;
                    return false;
                }

                struct region_cache {

                    std::map<region, MPI_Datatype> m_cache;

                    inline MPI_Datatype make_type(const region& r) const noexcept {
                        MPI_Datatype new_type;
                        MPI_Type_contiguous(r.m_extent[0], MPI_BYTE, &new_type);
                        for (int i=1; i<r.m_dimension; ++i)
                        {
                            MPI_Datatype old_type = new_type;
                            MPI_Type_create_hvector(r.m_extent[i], 1, r.m_stride[i], old_type, &new_type);
                        }
                        return new_type;
                    }

                    template<int... Order>
                    inline auto make_regions(const ::gridtools::ghex::structured::subarray<Order...>& sa) {
                        //1,2,0
                        const int dimension = sizeof...(Order);
                        const int order[dimension] = { Order... };
                        int lookup[dimension];
                        for (int d=dimension; d>0; --d) {
                            for (int i=0; i<dimension; ++i) {
                                if (order[i] == d-1) {
                                    lookup[dimension-d] = i;
                                    break;
                                }
                            }
                        }
                        std::vector<int> stride(dimension);
                        for (int i=0; i<dimension; ++i)
                            stride[i] = sa.m_stride[lookup[i]];

                        //std::cout << "    lookup table = ";
                        //for (int i=0; i<dimension; ++i) std::cout << lookup[i] << " ";
                        //std::cout << std::endl;
                        //std::cout << "    new strides = ";
                        //for (int i=0; i<dimension; ++i) std::cout << stride[i] << " ";
                        //std::cout << std::endl;

                        std::vector<region_type> regions;
                        regions.reserve(sa.m_regions.size());

                        const auto sa_offset = reinterpret_cast<std::size_t>(reinterpret_cast<unsigned char*>(sa.m_data));
                        for (const auto& r : sa.m_regions) {
                            const auto first_a = sa.m_offset + r.m_first;
                            std::vector<int> first(dimension);
                            for (int i=0; i<dimension; ++i)
                                first[i] = first_a[lookup[i]];
                            std::size_t offset = sa_offset;
                            for (int i=0; i<dimension; ++i) offset += first[i]*stride[i];
                            std::vector<int> extent(dimension);
                            for (int i=0; i<dimension; ++i)
                                extent[i] = r.m_extent[lookup[i]];
                            extent[0] *= sa.m_size;
                            auto p = m_cache.insert(std::make_pair(region{dimension, std::move(extent), stride}, MPI_Datatype{}));
                            if (p.second) {
                                p.first->second = make_type(p.first->first);
                            }
                            regions.push_back(region_type{(MPI_Aint)offset, &(*p.first)});
                        }
                        //std::sort(regions.begin(), regions.end(), [](const auto& p1, const auto& p2) { return (p1.m_offset < p2.m_offset); });
                        return regions;
                    }
                };

                struct region_vector {
                    std::vector<region_type> m_vec;
                };

                inline bool operator<(const region_vector& a, const region_vector& b) noexcept {
                    if (a.m_vec.size() < b.m_vec.size()) return true;
                    if (b.m_vec.size() < a.m_vec.size()) return false;
                    for (std::size_t i=0; i<a.m_vec.size(); ++i) {
                        if (a.m_vec[i] < b.m_vec[i]) return true;
                        if (b.m_vec[i] < a.m_vec[i]) return false;
                    }
                    return false;
                }

                struct subarray_cache {
                    
                    region_cache m_region_cache;

                    std::map<region_vector,MPI_Datatype> m_array_cache;

                    inline MPI_Datatype make_type(const region_vector& rvec) const noexcept {
                        std::vector<int> array_of_blocklengths(rvec.m_vec.size(),1);
                        std::vector<MPI_Aint> array_of_displacements(rvec.m_vec.size());
                        std::vector<MPI_Datatype> array_of_types(rvec.m_vec.size());
                        for (std::size_t i=0; i<rvec.m_vec.size(); ++i) {
                            array_of_displacements[i] = rvec.m_vec[i].m_offset;
                            array_of_types[i] = rvec.m_vec[i].m_map_ptr->second;
                        }
                        MPI_Datatype new_type;
                        MPI_Type_create_struct(
                            rvec.m_vec.size(), 
                            array_of_blocklengths.data(), 
                            array_of_displacements.data(), 
                            array_of_types.data(), 
                            &new_type);
                        return new_type;
                    }
                    
                    template<int... Order>
                    MPI_Datatype get_type(const std::vector<::gridtools::ghex::structured::subarray<Order...>>& subarrays) {
                        std::size_t num_regions = 0;
                        for (const auto& sa : subarrays)
                            num_regions += sa.m_regions.size();
                        region_vector rvec;
                        rvec.m_vec.reserve(num_regions);

                        for (const auto& sa : subarrays) {
                            auto regions = m_region_cache.make_regions<Order...>(sa);
                            for (auto& r : regions)
                                rvec.m_vec.push_back(std::move(r));
                        }
                        auto p = m_array_cache.insert(std::pair<region_vector, MPI_Datatype>(std::move(rvec), MPI_Datatype{}));
                        if (p.second) {
                            p.first->second = make_type(p.first->first);
                            MPI_Type_commit(&(p.first->second));
                        }
                        return p.first->second;
                    }
                };

            }
        }
    }
}
#endif // INCLUDED_GHEX_TRANSPORT_LAYER_MPI_SUBARRAY_HPP
