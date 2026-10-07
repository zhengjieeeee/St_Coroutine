cmake_minimum_required(VERSION 3.16)
project(test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/test/bin)

add_executable(test test.cpp)
target_link_libraries(test PRIVATE coroutine)   

