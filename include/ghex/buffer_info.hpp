/*
 * ghex-org
 *
 * Copyright (c) 2014-2023, ETH Zurich
 * All rights reserved.
 *
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#pragma once

#include <ghex/arch_traits.hpp>
#include <vector>
#include <functional>
#include <variant>
#include <memory>

namespace ghex
{
// forward declaration
template<typename GridType, typename DomainIdType>
class pattern;
template<typename GridType, typename DomainIdType>
class pattern_container;

template<typename Field>
class shared_field_ptr {
  public:
    using field_type = Field;

  private:
    std::variant<field_type*, std::shared_ptr<field_type>> m_field = nullptr;

  public:
    shared_field_ptr() noexcept : m_field{nullptr} {}
    shared_field_ptr(std::nullptr_t) noexcept : m_field{nullptr} {}
    shared_field_ptr(field_type* ptr) noexcept : m_field{ptr} {}
    shared_field_ptr(field_type f) noexcept : m_field{std::make_shared<field_type>(std::move(f))} {}
    shared_field_ptr(shared_field_ptr&&) noexcept = default;
    shared_field_ptr(const shared_field_ptr&) noexcept = default;
    shared_field_ptr& operator=(std::nullptr_t) noexcept { m_field.template emplace<field_type*>(nullptr); return *this; }
    shared_field_ptr& operator=(shared_field_ptr&&) noexcept = default;
    shared_field_ptr& operator=(const shared_field_ptr&) noexcept = default;

    field_type* get() const noexcept { return m_field.index() ? std::get<1>(m_field).get() : std::get<0>(m_field); }
    field_type& operator*() const noexcept { return *get(); }
    field_type* operator->() const noexcept { return get(); }
};

// forward declaration
template<typename Pattern, typename Arch, typename Field>
struct buffer_info;

/** @brief ties together field, pattern and device
  * @tparam GridType grid tag type
  * @tparam DomainIdType domain id type
  * @tparam Arch device type
  * @tparam Field field descriptor type */
template<typename GridType, typename DomainIdType, typename Arch, typename Field>
struct buffer_info<pattern<GridType, DomainIdType>, Arch, Field>
{
  public: // member types
    using pattern_type = pattern<GridType, DomainIdType>;
    using pattern_container_type = pattern_container<GridType, DomainIdType>;
    using arch_type = Arch;
    using field_type = Field;
    using device_id_type = typename arch_traits<arch_type>::device_id_type;
    using value_type = typename field_type::value_type;

  private: // friend class
    friend class pattern<GridType, DomainIdType>;

  private: // private ctor
    buffer_info(const pattern_type& p, field_type field, device_id_type id) noexcept
    : m_p{&p}
    , m_field{std::move(field)}
    , m_id{id}
    {
    }

    buffer_info(const pattern_type& p, field_type* field_ptr, device_id_type id) noexcept
    : m_p{&p}
    , m_field{field_ptr}
    , m_id{id}
    {
    }

    buffer_info(const pattern_type& p, shared_field_ptr<field_type> field_ptr, device_id_type id) noexcept
    : m_p{&p}
    , m_field{std::move(field_ptr)}
    , m_id{id}
    {
    }

  public: // copy and move ctors
    buffer_info(const buffer_info&) noexcept = default;
    buffer_info(buffer_info&&) noexcept = default;

  public: // member functions
    device_id_type                device_id() const noexcept { return m_id; }
    const pattern_type&           get_pattern() const noexcept { return *m_p; }
    const pattern_container_type& get_pattern_container() const noexcept { return m_p->container(); }
    auto                          get_field() noexcept { return m_field; }

  private: // members
    const pattern_type*          m_p;
    shared_field_ptr<field_type> m_field;
    device_id_type               m_id;
};

template<typename T>
struct is_buffer_info : public std::false_type
{
};

template<typename Pattern, typename Arch, typename Field>
struct is_buffer_info<buffer_info<Pattern, Arch, Field>> : public std::true_type
{
};

} // namespace ghex
