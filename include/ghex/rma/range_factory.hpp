/*
 * GridTools
 *
 * Copyright (c) 2014-2021, ETH Zurich
 * All rights reserved.
 *
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 */
#pragma once

#include <ghex/rma/locality.hpp>
#include <ghex/rma/handle.hpp>
#include <ghex/rma/access_guard.hpp>
#include <ghex/rma/event.hpp>
#include <ghex/rma/range.hpp>
#include <vector>
#include <cstring>
#include <boost/mp11.hpp>

namespace ghex
{
namespace rma
{
/** @brief Serializes and de-serializes range types which are among the types in RangeList. This
  * class manages the type erasure and type injection before and after serialization.
  * @tparam RangeList a list of range types */
template<typename RangeList>
struct range_factory
{
    using range_type = range;

    static_assert(boost::mp11::mp_is_set<RangeList>::value, "range types must be unique");

    static inline constexpr unsigned int a16(unsigned int size) noexcept
    {
        return ((size + 15) / 16) * 16;
    }

    template<typename Range>
    using range_size_p = boost::mp11::mp_size_t<sizeof(Range)>;
    using max_range_size =
        boost::mp11::mp_max_element<boost::mp11::mp_transform<range_size_p, RangeList>,
            boost::mp11::mp_less>;

    static constexpr std::size_t serial_size = a16(sizeof(int)) + a16(sizeof(info)) +
                                               a16(sizeof(typename local_access_guard::info)) +
                                               a16(sizeof(event_info)) + a16(sizeof(int)) +max_range_size::value;

    //template<typename Range>
    //static std::vector<unsigned char> serialize(
    //    info field_info, local_access_guard& g, local_event& e, const Range& r)
    //{
    //    std::vector<unsigned char> res(serial_size);
    //    serialize(field_info, g, e, r, res.data());
    //    return res;
    //}

    static range deserialize(unsigned char* buffer, int rank, bool on_gpu)
    {
        int id;
        //std::memcpy(&id, buffer, sizeof(int));
        //buffer += a16(sizeof(int));
        buffer = read(buffer, id);

        info field_info;
        //std::memcpy(&field_info, buffer, sizeof(field_info));
        //buffer += a16(sizeof(field_info));
        buffer = read(buffer, field_info);

        typename local_access_guard::info info_;
        //std::memcpy(&info_, buffer, sizeof(typename local_access_guard::info));
        //buffer += a16(sizeof(typename local_access_guard::info));
        buffer = read(buffer, info_);

        event_info e_info_;
        //std::memcpy(&e_info_, buffer, sizeof(event_info));
        //buffer += a16(sizeof(event_info));
        buffer = read(buffer, e_info_);

        int device_id;
        buffer = read(buffer, device_id);

        return boost::mp11::mp_with_index<boost::mp11::mp_size<RangeList>::value>(
            id, [buffer, field_info, info_, e_info_, rank, on_gpu, device_id](auto Id) {
                using range_t = boost::mp11::mp_at<RangeList, decltype(Id)>;
                return range(std::move(*reinterpret_cast<range_t*>(buffer)), decltype(Id)::value,
                    field_info, info_, e_info_, rank, on_gpu, device_id);
            });
    }

    // type injection here
    template<typename Func>
    static void call_back_with_type(range& r, Func&& f)
    {
        boost::mp11::mp_with_index<boost::mp11::mp_size<RangeList>::value>(
            r.m_id, [&r, f = std::forward<Func>(f)](auto Id) {
                using range_t = boost::mp11::mp_at<RangeList, decltype(Id)>;
                f(reinterpret_cast<range_impl<range_t>*>(r.m_impl.get())->m);
            });
    }

  //private:
    template<typename Range>
    static void serialize(info field_info, local_access_guard& g, local_event& e, int device_id, const Range& r,
        unsigned char* buffer)
    {
        static_assert(
            boost::mp11::mp_set_contains<RangeList, Range>::value, "range type not registered");
        using id = boost::mp11::mp_find<RangeList, Range>;

        const int m_id = id::value;
        //std::memcpy(buffer, &m_id, sizeof(int));
        //buffer += a16(sizeof(int));
        buffer = write(buffer, m_id);

        //std::memcpy(buffer, &field_info, sizeof(field_info));
        //buffer += a16(sizeof(field_info));
        buffer = write(buffer, field_info);

        auto info_ = g.get_info();
        //std::memcpy(buffer, &info_, sizeof(typename local_access_guard::info));
        //buffer += a16(sizeof(typename local_access_guard::info));
        buffer = write(buffer, info_);

        auto e_info_ = e.get_info();
        //std::memcpy(buffer, &e_info_, sizeof(event_info));
        //buffer += a16(sizeof(event_info));
        buffer = write(buffer, e_info_);

        //std::memcpy(buffer, &device_id, sizeof(int));
        //buffer += a16(sizeof(int));
        buffer = write(buffer, device_id);

        std::memcpy(buffer, &r, sizeof(Range));
    }

    private:
    template<typename T>
    static unsigned char* write(unsigned char* buffer, T const& x)
    {
        std::memcpy(buffer, &x, sizeof(T));
        return buffer + a16(sizeof(T));
    }
    template<typename T>
    static unsigned char* read(unsigned char* buffer, T& x)
    {
        std::memcpy(&x, buffer, sizeof(T));
        return buffer + a16(sizeof(T));
    }

};

} // namespace rma
} // namespace ghex
