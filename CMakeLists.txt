cmake_minimum_required(VERSION 3.16)
project(coroutine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

add_library(coroutine SHARED
    src/st_coroutine.cpp
    src/st_scheduler.cpp
    src/st_sync.cpp
    src/st_io.cpp
)

target_include_directories(coroutine PUBLIC
    ${CMAKE_SOURCE_DIR}/inc
)

add_subdirectory(test)