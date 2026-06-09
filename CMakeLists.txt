cmake_minimum_required(VERSION 3.14)
project(thread_pool LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# The queue is header-only; only the pool has a translation unit.
add_library(thread_pool src/thread_pool.cpp)
target_include_directories(thread_pool PUBLIC include)
target_link_libraries(thread_pool PUBLIC Threads::Threads)
target_compile_options(thread_pool PRIVATE -Wall -Wextra)

enable_testing()

add_executable(thread_pool_tests tests/test_thread_pool.cpp)
target_link_libraries(thread_pool_tests PRIVATE thread_pool)
add_test(NAME thread_pool_tests COMMAND thread_pool_tests)

add_executable(benchmark bench/benchmark.cpp)
target_link_libraries(benchmark PRIVATE thread_pool)

# Optional: -DSANITIZE=thread (or address) to build everything with a sanitizer.
if(SANITIZE)
    add_compile_options(-fsanitize=${SANITIZE} -g)
    add_link_options(-fsanitize=${SANITIZE})
endif()
