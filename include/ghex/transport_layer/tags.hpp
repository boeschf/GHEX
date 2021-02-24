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
#ifndef INCLUDED_GHEX_TL_TAGS_HPP
#define INCLUDED_GHEX_TL_TAGS_HPP

namespace gridtools {

    namespace ghex {
    
        namespace tl {

            /** @brief mpi transport tag */
            struct mpi_tag {};
            struct ucx_tag {};
            struct libfabric_tag {};

            const char *tag_to_string(const mpi_tag &) { return "mpi"; }
            const char *tag_to_string(const ucx_tag &) { return "ucx"; }
            const char *tag_to_string(const libfabric_tag &) { return "libfabric"; }
        } // namespace tl

    } // namespace ghex

} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_TAGS_HPP */

