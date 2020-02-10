
set(_tests test_low_level test_send_multi test_barrier test_barrier_mt test_cancel test_context test_send_recv)

foreach(t_ ${_tests})
    add_executable( ${t_} ./${t_}.cpp )
    target_link_libraries(${t_} gtest_main_mt)
    add_test(
        NAME ${t_}
        COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS} ${t_} ${MPIEXEC_POSTFLAGS}
    )

    if (GHEX_USE_UCP)
        add_executable( ${t_}_ucx ./${t_}.cpp )
        target_link_libraries(${t_}_ucx gtest_main_ucx)
        target_compile_definitions(${t_}_ucx PUBLIC GHEX_TEST_USE_UCX)
        if (GHEX_USE_PMIX)
            target_compile_definitions(${t_}_ucx PRIVATE GHEX_USE_PMI)
        endif()
        add_test(
            NAME ${t_}_ucx
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS} ${t_}_ucx ${MPIEXEC_POSTFLAGS}
        )
    endif()
endforeach(t_ ${_tests})

if (GHEX_USE_UCP)
    set(_tests_ucx test_ucx_context)

    foreach(t_ ${_tests_ucx})

        add_executable( ${t_} ./${t_}.cpp )
        target_link_libraries(${t_} gtest_main_ucx)
        if (GHEX_USE_PMIX)
            target_compile_definitions(${t_} PRIVATE GHEX_USE_PMI)
        endif()
        add_test(
            NAME ${t_}
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS} ${t_} ${MPIEXEC_POSTFLAGS}
        )

    endforeach(t_ ${_tests_ucx})
endif()

if (USE_GPU)
    set(_cuda_tests test_low_level)

    foreach(t_ ${_cuda_tests})
        add_executable( ${t_}_cuda ./${t_}.cu )
        target_link_libraries(${t_}_cuda gtest_main_mt)
        add_test(
            NAME ${t_}_cuda
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS} ${t_}_cuda ${MPIEXEC_POSTFLAGS}
        )

        if (GHEX_USE_UCP)
            add_executable( ${t_}_cuda_ucx ./${t_}.cu )
            target_link_libraries(${t_}_cuda_ucx gtest_main_ucx)
            target_compile_definitions(${t_}_cuda_ucx PUBLIC GHEX_TEST_USE_UCX)
            if (GHEX_USE_PMIX)
                target_compile_definitions(${t_}_cuda_ucx PRIVATE GHEX_USE_PMI)
            endif()
            add_test(
                NAME ${t_}_cuda_ucx
                COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 ${MPIEXEC_PREFLAGS} ${t_}_cuda_ucx ${MPIEXEC_POSTFLAGS}
            )
        endif()
    endforeach(t_ ${_cuda_tests})
endif()
