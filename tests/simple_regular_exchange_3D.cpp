#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <future>

#ifndef GHEX_TEST_USE_UCX
#include <ghex/transport_layer/mpi/context.hpp>
#define TRANSPORT tl::mpi_tag
#else
#include <ghex/transport_layer/ucx/context.hpp>
#define TRANSPORT tl::ucx_tag
#endif
#include <ghex/transport_layer/util/barrier.hpp>
#include <ghex/bulk_communication_object.hpp>
#include <ghex/structured/pattern.hpp>
#include <ghex/structured/rma_range_generator.hpp>
#include <ghex/structured/regular/domain_descriptor.hpp>
#include <ghex/structured/regular/field_descriptor.hpp>
#include <ghex/structured/regular/halo_generator.hpp>
#include <ghex/structured/regular/make_pattern.hpp>
#include <ghex/cuda_utils/error.hpp>
#include <gridtools/common/array.hpp>

using namespace gridtools::ghex;
using arr       = std::array<int,3>;
using transport = TRANSPORT;
using factory   = tl::context_factory<transport>;
using domain    = structured::regular::domain_descriptor<int,3>;
using halo_gen  = structured::regular::halo_generator<int,3>;

#define DIM 3
#define HALO 1
#define PX true
#define PY true
#define PZ false

constexpr std::array<int,6>  halos{HALO,HALO,HALO,HALO,HALO,HALO};
constexpr std::array<bool,3> periodic{PX,PY,PZ};

template<typename T>
struct memory
{
    unsigned int m_size;
    std::unique_ptr<T[]> m_host_memory;
#ifdef __CUDACC__
    struct cuda_deleter
    {
        // no delete since this messes up the rma stuff
        // when doing 2 tests in a row!!!
        //void operator()(T* ptr) const { cudaFree(ptr); }
        void operator()(T*) const { /* do nothing */ }
    };
    std::unique_ptr<T[],cuda_deleter> m_device_memory;
#endif

    memory(unsigned int size_, const T& value)
    : m_size{size_}
    , m_host_memory{ new T[m_size] }
    {
        for (unsigned int i=0; i<m_size; ++i)
            m_host_memory[i] = value;
#ifdef __CUDACC__
        void* ptr;
        GHEX_CHECK_CUDA_RESULT(cudaMalloc(&ptr, m_size*sizeof(T)))
        m_device_memory.reset((T*)ptr);
        clone_to_device();
#endif
    }

    memory(const memory&) = delete;
    memory(memory&&) = default;

    T* data() const { return m_host_memory.get(); }
    T* host_data() const { return m_host_memory.get(); }
#ifdef __CUDACC__
    T* device_data() const { return m_device_memory.get(); }
#endif

    unsigned int size() const { return m_size; }

    const T& operator[](unsigned int i) const { return m_host_memory[i]; }
          T& operator[](unsigned int i)       { return m_host_memory[i]; }

    T* begin() { return m_host_memory.get(); }
    T* end() { return m_host_memory.get()+m_size; }

    const T* begin() const { return m_host_memory.get(); }
    const T* end() const { return m_host_memory.get()+m_size; }

#ifdef __CUDACC__
    void clone_to_device()
    {
        GHEX_CHECK_CUDA_RESULT(cudaMemcpy(m_device_memory.get(), m_host_memory.get(),
            m_size*sizeof(T), cudaMemcpyHostToDevice))
    }
    void clone_to_host()
    {
        GHEX_CHECK_CUDA_RESULT(cudaMemcpy(m_host_memory.get(),m_device_memory.get(),
            m_size*sizeof(T), cudaMemcpyDeviceToHost))
    }
#endif
};

memory<gridtools::array<int,3>> allocate_field()
{ 
    return {(HALO*2+DIM) * (HALO*2+DIM)* (HALO*2+DIM), gridtools::array<int,3>{-1,-1,-1}};
}

template<typename RawField>
auto wrap_cpu_field(RawField& raw_field, const domain& d)
{
    return wrap_field<cpu,2,1,0>(d, raw_field.data(), arr{HALO, HALO, HALO}, arr{HALO*2+DIM, HALO*2+DIM, HALO*2+DIM});
}

#ifdef __CUDACC__
template<typename RawField>
auto wrap_gpu_field(RawField& raw_field, const domain& d)
{
    return wrap_field<gpu,2,1,0>(d, raw_field.device_data(), arr{HALO, HALO, HALO}, arr{HALO*2+DIM, HALO*2+DIM, HALO*2+DIM});
}
#endif

template<typename Field>
Field&& fill(Field&& field)
{
    for (int k=0; k<DIM; ++k)
    for (int j=0; j<DIM; ++j)
        for (int i=0; i<DIM; ++i)
        {
            auto& v = field({i,j,k});
            v[0] = field.domain().first()[0]+i;
            v[1] = field.domain().first()[1]+j;
            v[2] = field.domain().first()[2]+k;
        }
    return std::forward<Field>(field);
}

template<typename Field>
Field&& reset(Field&& field)
{
    for (int k=-HALO; k<DIM+HALO; ++k)
    for (int j=-HALO; j<DIM+HALO; ++j)
        for (int i=-HALO; i<DIM+HALO; ++i)
        {
            auto& v = field({i,j,k});
            v[0] = -1;
            v[1] = -1;
            v[2] = -1;
        }
    return fill(std::forward<Field>(field));
}

int expected(int coord, int dim, int first, int last, bool periodic_, int halo)
{
    if (coord < 0 && halo <= 0) return -1;
    if (coord >= DIM && halo <= 0) return -1;
    const auto ext = (last+1) - first;
    int res = first+coord;
    if (first == 0 && coord < 0) res = periodic_ ? dim*DIM + coord : -1;
    if (last == dim*DIM-1 && coord >= ext) res = periodic_ ? coord - ext : -1;
    return res;
}
//int expected_z(int coord, int dim, int first, int last, bool periodic_, int halo)
//{
//    std::cout << coord << ", " << dim << ", " << first << ", " << last << ", " << periodic_ << ", " << halo << std::endl;
//    std::cout << "coord < HALO ? " << (coord < HALO) << std::endl;
//    if (coord < 0 && halo <= 0) return -1;
//     std::cout << "  A" << std::endl;
//    if (coord >= DIM && halo <= 0) return -1;
//     std::cout << "  B" << std::endl;
//    const auto ext = (last+1) - first;
//    int res = first+coord;
//    if (first == 0 && coord < 0) res = periodic_ ? dim*DIM + coord : -1;
//    if (last == dim*DIM-1 && coord >= ext) res = periodic_ ? coord - ext : -1;
//    std::cout << "  return " << res << std::endl;
//    return res;
//}

template<typename Arr>
bool compare(const Arr& v, int x, int y, int z)
{
    if (x == -1 || y == -1 || z == -1) { x = -1; y = -1; z = -1; }
    return (x == v[0]) && (y == v[1] && (z == v[2]));
} 

template<typename Field>
bool check(const Field& field, const arr& dims)
{
    bool res = true;
    for (int k=-HALO; k<DIM+HALO; ++k)
    {
        const auto z = expected(k, dims[2], field.domain().first()[2],
            field.domain().last()[2], periodic[2], k<0?halos[4]:halos[5]);
        for (int j=-HALO; j<DIM+HALO; ++j)
        {
            const auto y = expected(j, dims[1], field.domain().first()[1],
                field.domain().last()[1], periodic[1], j<0?halos[2]:halos[3]);
            for (int i=-HALO; i<DIM+HALO; ++i)
            {
                const auto x = expected(i, dims[0], field.domain().first()[0],
                    field.domain().last()[0], periodic[0], i<0?halos[0]:halos[1]);
                res = res && compare(field({i,j,k}),x,y,z);
                if (!res)
                    std::cout << "found: (" 
                        << field({i,j,k})[0] << ", "
                        << field({i,j,k})[1] << ", "
                        << field({i,j,k})[2] << ") "
                        << "but expected: ("
                        << x << ", "
                        << y << ", "
                        << z << ") at coordinate ["
                        << i << ", "
                        << j << ", "
                        << k << "]"
                        << std::endl;
            }
        }
    }
    return res;
}

auto make_domain(int rank, arr coord)
{
    const auto x = coord[0]*DIM;
    const auto y = coord[1]*DIM;
    const auto z = coord[2]*DIM;
    return domain{rank, arr{x, y, z}, arr{x+DIM-1, y+DIM-1, z+DIM-1}};
}

struct domain_lu
{
    struct neighbor
    {
        int m_rank;
        int m_id;
        int rank() const noexcept { return m_rank; }
        int id() const noexcept { return m_id; }
    };
    
    arr m_dims;

    neighbor operator()(int id, arr const & offset) const noexcept
    {
        auto rank = id;
        auto z = rank/(m_dims[0]*m_dims[1]);
        rank -= z*m_dims[0]*m_dims[1];
        auto y = rank/m_dims[0];
        auto x = rank - y*m_dims[0];

        x = (x + offset[0] + m_dims[0])%m_dims[0];
        y = (y + offset[1] + m_dims[1])%m_dims[1];
        z = (z + offset[2] + m_dims[2])%m_dims[2];
        
        int n_rank = z*m_dims[0]*m_dims[1] + y*m_dims[0] + x;
        int n_id = n_rank;
        return {n_rank, n_id};
    }
};

template<typename Context, typename Pattern, typename SPattern, typename Domains, typename Barrier>
bool run(Context& context, const Pattern& pattern, const SPattern& spattern, const Domains& domains, const arr& dims, Barrier& barrier)
{
    bool res = true;
    // fields
    auto raw_field = allocate_field();
    auto field     = fill(wrap_cpu_field(raw_field, domains[0]));
#ifdef __CUDACC__
    raw_field.clone_to_device();
    auto field_gpu = wrap_gpu_field(raw_field, domains[0]);
#endif
    // get a communcator
    auto comm = context.get_communicator();
    
    // general exchange
    // ================
    auto co = make_communication_object<Pattern>(comm);

    // classical
    // ---------

#ifdef __CUDACC__
    co.exchange(pattern(field_gpu)).wait();
#else
    co.exchange(pattern(field)).wait();
#endif

    // check fields
#ifdef __CUDACC__
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#ifdef __CUDACC__
    raw_field.clone_to_device();
#endif

    barrier(comm);

    // using stages
    // ------------

#ifdef __CUDACC__
    co.exchange(spattern[0]->operator()(field_gpu)).wait();
    co.exchange(spattern[1]->operator()(field_gpu)).wait();
    co.exchange(spattern[2]->operator()(field_gpu)).wait();
#else
    co.exchange(spattern[0]->operator()(field)).wait();
    co.exchange(spattern[1]->operator()(field)).wait();
    co.exchange(spattern[2]->operator()(field)).wait();
#endif

    // check fields
#ifdef __CUDACC__
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#ifdef __CUDACC__
    raw_field.clone_to_device();
#endif

    barrier(comm);

    // bulk exchange (rma)
    // ===================
    
    // classical
    // ---------
    
#ifdef __CUDACC__
    auto bco = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(co);
    bco.add_field(pattern(field_gpu));
#else
    auto bco = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(co,true);
    bco.add_field(pattern(field));
#endif
    bco.init();
    barrier(comm);
    bco.exchange().wait();

    // check fields
#ifdef __CUDACC__
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    // reset fields
    reset(field);
#ifdef __CUDACC__
    raw_field.clone_to_device();
#endif

    barrier(comm);

    // using stages
    // ------------

#ifdef __CUDACC__
    auto bco_x = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(co);
    bco_x.add_field(spattern[0]->operator()(field_gpu));
    auto bco_y = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(co);
    bco_y.add_field(spattern[1]->operator()(field_gpu));
    auto bco_z = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field_gpu)>(co);
    bco_z.add_field(spattern[2]->operator()(field_gpu));
#else
    auto bco_x = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(co,true);
    bco_x.add_field(spattern[0]->operator()(field));
    auto bco_y = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(co,true);
    bco_y.add_field(spattern[1]->operator()(field));
    auto bco_z = bulk_communication_object<structured::rma_range_generator, Pattern, decltype(field)>(co,true);
    bco_z.add_field(spattern[2]->operator()(field));
#endif
    bco_x.init();
    bco_y.init();
    bco_z.init();
    barrier(comm);
    bco_x.exchange().wait();
    bco_y.exchange().wait();
    bco_z.exchange().wait();

    // check fields
#ifdef __CUDACC__
    raw_field.clone_to_host();
#endif
    res = res && check(field, dims);

    return res;
}

void sim()
{
    // make a context from mpi world and number of threads
    auto context_ptr = factory::create(MPI_COMM_WORLD);
    auto& context    = *context_ptr;
    // 2D domain decomposition
    arr dims{0,0,0}, coords{0,0,0};
    MPI_Dims_create(context.size(), 3, dims.data());
    if (context.rank()==0)
    std::cout << "decomposition : " << dims[0] << " " << dims[1] << " " << dims[2] << std::endl;
    coords[2] = context.rank()/(dims[0]*dims[1]);
    coords[1] = (context.rank() - coords[2]*dims[0]*dims[1])/dims[0];
    coords[0] = context.rank() - coords[2]*dims[0]*dims[1] - coords[1]*dims[0];
    // make 1 domain per rank
    std::vector<domain> domains{ make_domain(context.rank(), coords) };
    // neighbor lookup
    domain_lu d_lu{dims};
    //if (context.rank()==2)
    //{
    //    {
    //    auto n_0 = d_lu(context.rank()*2+1, {0,-1});
    //    std::cout << n_0.rank() << ", " << n_0.id() << std::endl;
    //    std::cout << domains[0].domain_id() << std::endl;
    //    }
    //    {
    //    auto n_0 = d_lu(context.rank()*2, {0,1});
    //    std::cout << n_0.rank() << ", " << n_0.id() << std::endl;
    //    std::cout << domains[1].domain_id() << std::endl;
    //    }
    //}

    auto staged_pattern = structured::regular::make_staged_pattern(context, domains, d_lu,
        arr{0,0,0}, arr{dims[0]*DIM-1,dims[1]*DIM-1,dims[2]*DIM-1}, halos, periodic);

    //if (context.rank()==1)
    //{
    //int i = 0;
    //for (auto& p_cont_ptr : staged_pattern)
    //{
    //    std::cout << "i = " << i << std::endl;
    //    ++i;
    //    for (auto& p : *p_cont_ptr)
    //    {
    //        std::cout << "pattern domain id = " << p.domain_id() << std::endl;
    //        std::cout << "recv halos: " << std::endl;
    //        for (auto& r : p.recv_halos())
    //        {
    //            std::cout << r.first << ": " << std::endl;
    //            for (auto& h : r.second)
    //                std::cout << "  " << h << std::endl;
    //        }
    //        std::cout << "send halos: " << std::endl;
    //        for (auto& r : p.send_halos())
    //        {
    //            std::cout << r.first << ": " << std::endl;
    //            for (auto& h : r.second)
    //                std::cout << "  " << h << std::endl;
    //        }
    //        std::cout << std::endl;
    //    }
    //}
    //}

    // make halo generator
    halo_gen gen{arr{0,0,0}, arr{dims[0]*DIM-1,dims[1]*DIM-1,dims[2]*DIM-1}, halos, periodic};
    // create a pattern for communication
    auto pattern = make_pattern<structured::grid>(context, gen, domains);
    // run
    tl::barrier_t barrier(1);
    bool res = run(context, pattern, staged_pattern, domains, dims, barrier);
    // reduce res
    bool all_res = false;
    MPI_Reduce(&res, &all_res, 1, MPI_C_BOOL, MPI_LAND, 0, MPI_COMM_WORLD);
    if (context.rank() == 0)
    {
        EXPECT_TRUE(all_res);
    }
}

TEST(simple_regular_exchange, 3D)
{
    sim();
}


