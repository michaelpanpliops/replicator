cmake_minimum_required(VERSION 3.10)
project(replicator)
set(CMAKE_CXX_COMPILER g++)
add_definitions(-std=c++17)

find_package(Threads)

if(NOT DEFINED INCLUDE_PATH)
  message(FATAL_ERROR "INCLUDE_PATH for the required xdprocks headers is not specified")
endif()

if(NOT DEFINED LIBRARY_PATH)
  message(FATAL_ERROR "LIBRARY_PATH for the xdprocks library is not specified")
endif()

#set(CMAKE_BUILD_TYPE Debug)


include_directories(${INCLUDE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/pliops)

link_directories(${LIBRARY_PATH})
add_definitions(-DROCKSDB_NAMESPACE=xdprocks)

set(SOURCES
    client.cc
    rpc.cc
    server.cc
    pliops/communication.cc
    pliops/consumer.cc
    pliops/producer.cc
    )

add_compile_options(-Wno-format-security)
add_executable(client_exe client_main.cc ${SOURCES})

target_link_libraries(client_exe xdprocks ${CMAKE_THREAD_LIBS_INIT})

add_compile_options(-Wno-format-security)
add_executable(server_exe server_main.cc ${SOURCES})

target_link_libraries(server_exe xdprocks ${CMAKE_THREAD_LIBS_INIT})
