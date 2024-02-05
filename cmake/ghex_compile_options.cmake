
set(c_cxx_lang "$<COMPILE_LANGUAGE:C,CXX>")
set(c_cxx_lang_clang "$<COMPILE_LANG_AND_ID:CXX,Clang>")
set(cuda_lang "$<COMPILE_LANGUAGE:CUDA>")
set(fortran_lang_intel "$<COMPILE_LANG_AND_ID:Fortran,Intel>")
set(fortran_lang_cray "$<COMPILE_LANG_AND_ID:Fortran,Cray>")
set(fortran_lang_gnu "$<COMPILE_LANG_AND_ID:Fortran,GNU>")

function(ghex_target_compile_options target)
    target_compile_options(${target} PRIVATE
    # flags for CXX builds
    $<${c_cxx_lang}:$<BUILD_INTERFACE:-Wall -Wextra -Wpedantic -Wno-unknown-pragmas>>
    $<${c_cxx_lang_clang}:$<BUILD_INTERFACE:-Wno-unused-lambda-capture>>
    # flags for CUDA builds
    $<${cuda_lang}:$<BUILD_INTERFACE:-Xcompiler=-Wall -Wextra -Wno-unknown-pragmas --default-stream per-thread>>
    # flags for Fortran builds
    $<${fortran_lang_intel}:$<BUILD_INTERFACE:-cpp>>
    $<${fortran_lang_cray}:$<BUILD_INTERFACE:-eZ>>
    $<${fortran_lang_gnu}:$<BUILD_INTERFACE:-cpp -ffree-line-length-none>>)
    if (OOMPH_ENABLE_BARRIER)
        target_compile_definitions(${target} PRIVATE GHEX_ENABLE_BARRIER)
    endif()
    if (ghex_gpu_mode STREQUAL "hip")
        find_package(hip REQUIRED)
        target_link_libraries(${target} PRIVATE hip::device)
    endif()
endfunction()

function(compile_as_cuda)
    cmake_parse_arguments(P "" "DIRECTORY" "SOURCES" ${ARGN})
    if(NOT P_DIRECTORY)
        message(FATAL_ERROR "You must provide a directory")
    endif()
    if (ghex_gpu_mode STREQUAL "cuda")
        set_source_files_properties(${P_SOURCES} PROPERTIES LANGUAGE CUDA DIRECTORY ${P_DIRECTORY})
    endif()
endfunction()
