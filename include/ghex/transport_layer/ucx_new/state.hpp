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

#pragma once

#include "../util/pthread_spin_mutex.hpp"
#include "./worker.hpp"

namespace gridtools{
namespace ghex {
namespace tl {
namespace ucx {

struct request_cache
{
    struct status
    {
        unsigned int m_completed;
        unsigned int m_outstanding;
    };

    std::vector<request_header*> m_outstanding_reqs;

    auto size() const noexcept { return m_outstanding_reqs.size(); }

    void add(request_header& req)
    {
        if (req.ready()) return;
        m_outstanding_reqs.push_back(&req);
    }
    
    status progress()
    {
        const auto s = m_outstanding_reqs.size();
        const auto end = std::remove_if(m_outstanding_reqs.begin(), m_outstanding_reqs.end(),
            [](request_header* req)
            {
                const bool ret = (UCS_INPROGRESS != ucp_request_check_status(req->m_ucx_request));
                if (ret)
                    req->mark_completed();
                return ret;
            });
        m_outstanding_reqs.resize(end-m_outstanding_reqs.begin());
        return {(unsigned int)(s-m_outstanding_reqs.size()),
                (unsigned int)m_outstanding_reqs.size()};
    }
};

class state;

// non-copyable, non-movable
class shared_state
{
public: // member types
    using rank_type = typename address_db_t::rank_type;
    using tag_type = typename address_db_t::tag_type;
    using mutex_type = std::mutex;
    //using mutex_type = pthread_spin::recursive_mutex;
    using lock_type = std::lock_guard<mutex_type>;

    friend class state;

private: // members
    worker m_worker;
    mutex_type m_worker_mutex;

public: // ctors
    shared_state(ucp_context_h context) : m_worker(context) {}
    shared_state(const shared_state&) = delete;
    shared_state(shared_state&&) = delete;

public: // member functions
    address_t address() const noexcept { return m_worker.address(); }
};

// non-copyable, non-movable
class state
{
public: // member types
    using rank_type = typename address_db_t::rank_type;
    using tag_type = typename address_db_t::tag_type;
    using endpoint_cache_type = std::unordered_map<rank_type, endpoint_t>;
    using pool_type = typename request_data::pool_type;
    using message_type = cb::any_message;
    using queue_type = cb::callback_queue<request, rank_type, tag_type>;
    using completion_type = cb::request;
    using progress_status = cb::progress_status;
    using mutex_type = typename shared_state::mutex_type;
    using lock_type = typename shared_state::lock_type;

    struct request_cb
    {
        using rank_type = typename state::rank_type;
        using tag_type = typename state::tag_type;
        using queue_type = typename state::queue_type;
        using completion_type = typename state::completion_type;

        queue_type* m_queue = nullptr;
        completion_type m_completed;
        
        bool test()
        {
            if(!m_queue) return true;
            if (m_completed.is_ready())
            {
                m_queue = nullptr;
                m_completed.reset();
                return true;
            }
            return false;
        }

        bool cancel()
        {
            if(!m_queue) return false;
            auto res = m_queue->cancel(m_completed.queue_index());
            if (res)
            {
                m_queue = nullptr;
                m_completed.reset();
            }
            return res;
        }
    };

private: // members
    pool_type m_pool;
    worker m_worker;
    request_cache m_send_request_cache;
    request_cache m_recv_request_cache;
    address_db_t& m_address_db;
    shared_state* m_shared_state;
    endpoint_cache_type m_endpoint_cache;
    queue_type m_send_callback_queue;
    queue_type m_recv_callback_queue;
    rank_type m_rank;
    rank_type m_size;
    const mpi::rank_topology& m_rank_topology;
    unsigned int m_num_immediate_sends = 0u;
    unsigned int m_num_immediate_send_cbs = 0u;
    unsigned int m_num_immediate_recv_cbs = 0u;
    unsigned int m_num_cancels = 0u;

public: // ctors
    state(address_db_t& db, ucp_context_h context, shared_state* shared, const mpi::rank_topology& t)
    : m_pool(((context_request_size(context)+sizeof(request_header)+15)/16)*16)
    , m_worker(context)
    , m_address_db{db}
    , m_shared_state{shared}
    , m_rank{db.rank()}
    , m_size{db.size()}
    , m_rank_topology(t)
    {}

    state(const state&) = delete;
    state(state&&) = delete;

public: // member functions
    rank_type rank() const noexcept { return m_rank; }
    rank_type size() const noexcept { return m_size; }
    address_t address() const noexcept { return m_shared_state->address(); }

    auto progress_recvs()
    {
        if (m_recv_request_cache.size())
        {
            lock_type lk(m_shared_state->m_worker_mutex);
            m_shared_state->m_worker.progress();
        } 
        return m_recv_request_cache.progress();
    }

    void progress_recvs(request& req)
    {
        while(!m_shared_state->m_worker_mutex.try_lock())
        {
            m_recv_request_cache.progress();
            if (req.ready()) return;
        }
        m_shared_state->m_worker.progress();
        m_shared_state->m_worker_mutex.unlock();
        m_recv_request_cache.progress();
    }

    auto progress_sends()
    {
        if (m_send_request_cache.size())
            m_worker.progress();
        return m_send_request_cache.progress();
    }

    void progress(request& req)
    {
        if (req.kind() == request_kind::send)
        {
            progress_sends();
            if (req.ready()) return;
            progress_recvs();
        } 
        else
        {
            progress_recvs(req);
            if (req.ready()) return;
            progress_sends();
        }
    }

    progress_status progress()
    {
        // progress receives (shared worker)
        /*const auto recv_stats =*/ progress_recvs();
        // progress sends (thread local worker)
        /*const auto send_stats =*/ progress_sends();
        // progress callbacks (thread local queue)
        const auto completed_send_callbacks = m_send_callback_queue.progress();
        const auto completed_recv_callbacks = m_recv_callback_queue.progress();
        return {
            (int)(completed_send_callbacks + std::exchange(m_num_immediate_send_cbs, 0u)),
            (int)(completed_recv_callbacks + std::exchange(m_num_immediate_recv_cbs, 0u)), 
            (int)(std::exchange(m_num_cancels, 0u))
        };
    }

    // this function should only be called for recv requests
    void cancel(request& req)
    {
        {
            lock_type lk(m_shared_state->m_worker_mutex);
            ucp_request_cancel(m_shared_state->m_worker.get(), req.data().get().m_ucx_request);
        } 
        while(!req.ready())
        {
            progress_recvs();
        }
        ++m_num_cancels;
    }


    template <typename Message>
    request send(const Message &msg, rank_type dst, tag_type tag)
    {
        request_data req(&m_pool);
        const auto& ep = connect(dst);
        const auto stag = ((std::uint_fast64_t)tag << 32) | (std::uint_fast64_t)(rank());
        const auto ret = ucp_tag_send_nbr(
            ep.get(),
            msg.data(),
            msg.size()*sizeof(typename Message::value_type),
            ucp_dt_make_contig(1),
            stag,
            req.get().m_ucx_request);
        if (UCS_OK == ret)
        {
            // immeadiate completion
            req.get().mark_completed();
            ++m_num_immediate_sends;
        }
        else if (UCS_INPROGRESS == ret)
        {
            m_send_request_cache.add(req.get());
        }
        else
        {
            // an error occurred
            throw std::runtime_error("ghex: ucx error - send operation failed");
        }
        return {std::move(req), request_kind::send, this};
    }

    template <typename Message>
    request recv(Message &msg, rank_type src, tag_type tag)
    {
        request_data req(&m_pool);
        const auto rtag = ((std::uint_fast64_t)tag << 32) | (std::uint_fast64_t)(src);
        ucs_status_t ret;
        {
            lock_type lk(m_shared_state->m_worker_mutex);
            ret = ucp_tag_recv_nbr(
                m_shared_state->m_worker.get(),
                msg.data(),
                msg.size()*sizeof(typename Message::value_type),
                ucp_dt_make_contig(1),
                rtag,
                ~std::uint_fast64_t(0ul),
                req.get().m_ucx_request);
        }
        if (ret==UCS_OK || ret==UCS_INPROGRESS)
        {
            m_recv_request_cache.add(req.get());
        }
        else
        {
            // an error occurred
            throw std::runtime_error("ghex: ucx error - recv operation failed");
        }
        return {std::move(req), request_kind::recv, this};
    }

    template<typename CallBack>
    request_cb send(message_type&& msg, rank_type dst, tag_type tag, CallBack&& callback)
    {
        auto req = send(msg,dst,tag);
        if (req.ready())
        {
            ++m_num_immediate_send_cbs;
            callback(std::move(msg), dst, tag);
            return {};
        }
        else
        {
            return {&m_send_callback_queue, m_send_callback_queue.enqueue(
                std::move(msg), dst, tag, std::move(req), std::forward<CallBack>(callback))};
        }
    }

    template<typename CallBack>
    request_cb recv(message_type&& msg, rank_type src, tag_type tag, CallBack&& callback)
    {
        auto req = recv(msg,src,tag);
        if (req.ready())
        {
            ++m_num_immediate_recv_cbs;
            callback(std::move(msg), src, tag);
            return {};
        }
        else
        {
            return {&m_recv_callback_queue, m_recv_callback_queue.enqueue(
                std::move(msg), src, tag, std::move(req), std::forward<CallBack>(callback))};
        }
    }

    const auto& rank_topology() const noexcept { return m_rank_topology; }

    auto mpi_comm() const noexcept { return m_rank_topology.mpi_comm(); }

private: // implementation
    static std::size_t context_request_size(ucp_context_h context)
    {
        ucp_context_attr_t attr;
        attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
        ucp_context_query(context, &attr);
        return attr.request_size;
    }

    const endpoint_t& connect(rank_type rank)
    {
        auto it = m_endpoint_cache.find(rank);
        if (it != m_endpoint_cache.end())
            return it->second;
        auto addr = m_address_db.find(rank);
        auto p = m_endpoint_cache.insert(std::make_pair(rank, endpoint_t{rank, m_worker.get(), addr}));
        return p.first->second;
    }
};

inline bool request::cancel()
{
    if (m_kind == request_kind::invalid) return false;
    if (m_kind == request_kind::send) return false;
    m_state->cancel(*this);
    return true;
}

inline void request::progress()
{
    m_state->progress(*this);
}

} // namespace ucx
} // namespace tl
} // namespace ghex
} // namespace gridtools
