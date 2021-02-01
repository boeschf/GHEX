# simple benchmarks
# -----------------
find_package(OpenMP REQUIRED)

# Variable used for benchmarks that DO NOT require multithreading support
set(_benchmarks_simple simple_comm_test_halo_exchange_3D_generic_full)
# Variable used for benchmarks that require multithreading support
set(_benchmarks_simple_mt )
foreach (_t ${_benchmarks_simple})
    add_executable(${_t} ${_t}.cpp )
    target_link_libraries(${_t} gtest_main_bench)
endforeach()

foreach (_t ${_benchmarks_simple_mt})
    add_executable(${_t}_mt ${_t}.cpp )
    target_link_libraries(${_t}_mt gtest_main_bench_mt)
endforeach()


# advanced benchmarks
# -------------------

# Variable used for benchmarks that DO NOT require multithreading support
set(_benchmarks gcl_test_halo_exchange_3D_generic_full comm_2_test_halo_exchange_3D_generic_full)
# Variable used for benchmarks that require multithreading support
set(_benchmarks_mt )

foreach (_t ${_benchmarks})
    add_executable(${_t} ${_t}.cpp)
    target_link_libraries(${_t} gtest_main_bench)

    add_executable(${_t}_1_pattern ${_t}.cpp)
    target_compile_definitions(${_t}_1_pattern PUBLIC GHEX_1_PATTERN_BENCHMARK GCL_MPI) 
    target_link_libraries(${_t}_1_pattern gtest_main_bench)

    if(USE_GPU)
        add_executable(${_t}_gpu ${_t}.cu)
        target_link_libraries(${_t}_gpu gtest_main_bench)

        add_executable(${_t}_1_pattern_gpu ${_t}.cu)
        target_compile_definitions(${_t}_1_pattern_gpu PUBLIC GHEX_1_PATTERN_BENCHMARK)
        target_link_libraries(${_t}_1_pattern_gpu gtest_main_bench)
    endif()
endforeach()

foreach (_t ${_benchmarks_mt})
    add_executable(${_t}_mt ${_t}.cpp)
    target_link_libraries(${_t}_mt gtest_main_bench_mt)

    add_executable(${_t}_1_pattern_mt ${_t}.cpp)
    target_compile_definitions(${_t}_1_pattern_mt PUBLIC GHEX_1_PATTERN_BENCHMARK)
    target_link_libraries(${_t}_1_pattern_mt gtest_main_bench_mt)

    if(USE_GPU)
        add_executable(${_t}_gpu_mt ${_t}.cu)
        target_link_libraries(${_t}_gpu_mt gtest_main_bench_mt)

        add_executable(${_t}_1_pattern_gpu_mt ${_t}.cu)
        target_compile_definitions(${_t}_1_pattern_gpu_mt PUBLIC GHEX_1_PATTERN_BENCHMARK)
        target_link_libraries(${_t}_1_pattern_gpu_mt gtest_main_bench_mt)
    endif()
endforeach()

# RMA benchmark
if (HWCART_FOUND)
set(_rma_benchmarks simple_rma)
foreach (_t ${_rma_benchmarks})
    # MPI, no rma, float, cpu
    # ========================
    set(t ${_t}_mpi_none_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, no rma, double, cpu
    # ========================
    set(t ${_t}_mpi_none_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)

    # MPI, threads, float, cpu
    # ========================
    set(t ${_t}_mpi_threads_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, threads, double, cpu
    # ========================
    set(t ${_t}_mpi_threads_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    if (GHEX_USE_XPMEM)
    # MPI, xpmem, float, cpu
    # ========================
    set(t ${_t}_mpi_xpmem_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, xpmem, double, cpu
    # ========================
    set(t ${_t}_mpi_xpmem_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    endif()

    if (GHEX_USE_UCP)
    # UCX, no rma, float, cpu
    # ========================
    set(t ${_t}_ucx_none_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, no rma, double, cpu
    # ========================
    set(t ${_t}_ucx_none_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)

    # UCX, threads, float, cpu
    # ========================
    set(t ${_t}_ucx_threads_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, threads, double, cpu
    # ========================
    set(t ${_t}_ucx_threads_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    if (GHEX_USE_XPMEM)
    # UCX, xpmem, float, cpu
    # ========================
    set(t ${_t}_ucx_xpmem_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, xpmem, double, cpu
    # ========================
    set(t ${_t}_ucx_xpmem_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    endif()
    endif()

    if(USE_GPU)
    # MPI, no rma, float, gpu
    # ========================
    set(t ${_t}_mpi_none_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, no rma, double, gpu
    # ========================
    set(t ${_t}_mpi_none_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, threads, float, gpu
    # ========================
    set(t ${_t}_mpi_threads_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, threads, double, gpu
    # ========================
    set(t ${_t}_mpi_threads_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    if (GHEX_USE_XPMEM)
    # MPI, xpmem, float, gpu
    # ========================
    set(t ${_t}_mpi_xpmem_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # MPI, xpmem, double, gpu
    # ========================
    set(t ${_t}_mpi_xpmem_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    endif()

    if (GHEX_USE_UCP)
    # UCX, no rma, float, gpu
    # ========================
    set(t ${_t}_ucx_none_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, no rma, double, gpu
    # ========================
    set(t ${_t}_ucx_none_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_compile_definitions(${t} PUBLIC GHEX_NO_RMA)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, threads, float, gpu
    # ========================
    set(t ${_t}_ucx_threads_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, threads, double, gpu
    # ========================
    set(t ${_t}_ucx_threads_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    if (GHEX_USE_XPMEM)
    # UCX, xpmem, float, gpu
    # ========================
    set(t ${_t}_ucx_xpmem_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # UCX, xpmem, double, gpu
    # ========================
    set(t ${_t}_ucx_xpmem_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_USE_XPMEM)
    target_compile_definitions(${t} PUBLIC GHEX_TEST_USE_UCX)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    endif()
    endif()
    endif()

    # float, cpu
    # ===========
    set(t ${_t}_mpi_raw_float_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_BENCHMARK_USE_RAW_MPI)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # double, cpu
    # ===========
    set(t ${_t}_mpi_raw_double_cpu)
    add_executable(${t} ${_t}.cpp)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_BENCHMARK_USE_RAW_MPI)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)

    if(USE_GPU)
    # float, gpu
    # ===========
    set(t ${_t}_mpi_raw_float_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=float)
    target_compile_definitions(${t} PUBLIC GHEX_BENCHMARK_USE_RAW_MPI)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    
    # double, gpu
    # ===========
    set(t ${_t}_mpi_raw_double_gpu)
    add_executable(${t} ${_t}.cu)
    target_compile_definitions(${t} PUBLIC GHEX_FLOAT_TYPE=double)
    target_compile_definitions(${t} PUBLIC GHEX_BENCHMARK_USE_RAW_MPI)
    target_link_libraries(${t} ghexlib)
    target_link_libraries(${t} OpenMP::OpenMP_CXX)
    target_link_libraries(${t} HWCART::libhwcart HWLOC::libhwloc)
    endif()
endforeach()
endif()


add_subdirectory(transport)


# atlas benchmarks
# ----------------

if (GHEX_ENABLE_ATLAS_BINDINGS)
    # Variable used for benchmarks that DO NOT require multithreading support
    set(_benchmarks atlas_halo_exchange_nodecolumns)

    foreach (_t ${_benchmarks})
        add_executable(${_t} ${_t}.cpp)
        target_link_libraries(${_t} gtest_main_bench_atlas)

        if(USE_GPU)
            add_executable(${_t}_gpu ${_t}.cu)
            target_compile_definitions(${_t}_gpu PUBLIC GT_USE_GPU)
            target_link_libraries(${_t}_gpu gtest_main_bench_atlas)
        endif()
    endforeach()
endif()
