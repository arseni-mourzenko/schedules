cmake_minimum_required(VERSION 3.31)

set(PROJECT_NAME "schedules")

project(${PROJECT_NAME} LANGUAGES CXX HIP)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(HIP_HIPCC_FLAGS "${HIP_HIPCC_FLAGS} -O3")

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)

list(APPEND CMAKE_PREFIX_PATH 
    /opt/rocm-7.1.1
    /opt/rocm-7.1.1/lib/cmake/hip
)

set(ROCM_PATH "/opt/rocm-7.1.1")

find_package(HIP REQUIRED)

if(NOT HIP_FOUND)
    message(FATAL_ERROR "HIP not found. Please install ROCm/HIP.")
endif()

set(SOURCE_FILES src/main.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})

set_source_files_properties(${SOURCE_FILES} PROPERTIES LANGUAGE HIP)

set(CMAKE_HIP_ARCHITECTURES "gfx1010")

set_target_properties(${PROJECT_NAME} PROPERTIES HIP_ARCHITECTURES "gfx1010")

target_link_libraries(${PROJECT_NAME} ${LIBPQXX_LIBRARIES} hip::host hip::device)

target_compile_options(${PROJECT_NAME} PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wall -Wextra -Wno-deprecated-declarations -Wno-unused-result>
    $<$<COMPILE_LANGUAGE:HIP>:-O3 -Wno-deprecated-declarations -Wno-unused-result>
)