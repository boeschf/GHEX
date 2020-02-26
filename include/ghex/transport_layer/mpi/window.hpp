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
#ifndef INCLUDED_GHEX_TL_MPI_WINDOW_HPP
#define INCLUDED_GHEX_TL_MPI_WINDOW_HPP

#include <set>
#include "./error.hpp"

namespace gridtools {

    namespace ghex {

        namespace tl {

            namespace mpi {
                    
                struct window_t {
                    using rank_type = int;
                    window_t(MPI_Comm comm) : m_comm{comm} {}
                    ~window_t() {
                        MPI_Win_free(&m_win);
                        MPI_Group_free(&m_group);
                        MPI_Group_free(&m_access_group);
                        MPI_Group_free(&m_exposure_group);
                    }
                    MPI_Comm m_comm;
                    MPI_Win m_win;
                    MPI_Group m_group;
                    MPI_Group m_access_group;
                    MPI_Group m_exposure_group;
                    std::set<rank_type> m_access_ranks;
                    std::set<rank_type> m_exposure_ranks;
                    volatile bool m_epoch = false;
                };

            } // namespace mpi

        } // namespace tl

    } // namespace ghex

} //namespace gridtools

#endif /* INCLUDED_GHEX_TL_MPI_WINDOW_HPP */
