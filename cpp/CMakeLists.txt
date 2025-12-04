cmake_minimum_required(VERSION 3.31)

set(PROJECT_NAME "schedules")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++2c -O3 -march=native -pedantic -Wswitch -Wall -Wundef -Wcast-align -Wwrite-strings -Wlogical-op -Wmissing-declarations -Wredundant-decls -Woverloaded-virtual -Wno-deprecated-declarations")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -Werror=return-type")
endif()

project(${PROJECT_NAME})

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPQXX REQUIRED libpqxx)

add_executable(${PROJECT_NAME})

target_sources(${PROJECT_NAME}
    PUBLIC
    src/implementations/avx2.h
    src/implementations/int64.h
    src/implementations/plain.h
    src/implementations/sse.h
    src/implementations/threads.h
    src/main.cpp
)

target_link_libraries(${PROJECT_NAME} ${LIBPQXX_LIBRARIES})
