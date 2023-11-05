cmake_minimum_required(VERSION 3.10)
project(replicator)
set(CMAKE_CXX_COMPILER g++)
add_definitions(-std=c++17)

find_package(Threads)

if(NOT DEFINED XDPROCKS_INCLUDE_PATH)
  message(FATAL_ERROR "XDPROCKS_INCLUDE_PATH for the xdprocks header files is not specified")
endif()

if(NOT DEFINED XDPROCKS_LIBRARY_PATH)
  message(FATAL_ERROR "XDPROCKS_LIBRARY_PATH for the xdprocks library is not specified")
endif()

if(NOT DEFINED ROCKSDB_INCLUDE_PATH)
  message(FATAL_ERROR "ROCKSDB_INCLUDE_PATH for the rocksdb header files is not specified")
endif()

if(NOT DEFINED ROCKSDB_LIBRARY_PATH)
  message(FATAL_ERROR "ROCKSDB_LIBRARY_PATH for the rocksdb library is not specified")
endif()

#set(CMAKE_BUILD_TYPE Debug)


include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/pliops)

link_directories(${XDPROCKS_LIBRARY_PATH} ${ROCKSDB_LIBRARY_PATH})

set(SOURCES
    rpc.cc
    pliops/communication.cc
    pliops/kv_pair_simple_serializer.cc
    pliops/simple_logger.cc
    pliops/status.cc
    )

set(CLIENT_SOURCES
    client.cc
    client_main.cc
    pliops/consumer.cc
    ${SOURCES}
    )

set(SERVER_SOURCES
    server.cc
    server_main.cc
    pliops/producer.cc
    ${SOURCES}
    )

add_compile_options(-Wno-format-security)

add_executable(client_exe ${CLIENT_SOURCES})
target_link_libraries(client_exe PRIVATE xdprocks ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(client_exe PRIVATE ${XDPROCKS_INCLUDE_PATH})
target_compile_definitions(client_exe PRIVATE ROCKSDB_NAMESPACE=xdprocks XDPROCKS)

add_executable(server_exe ${SERVER_SOURCES})
target_link_libraries(server_exe PRIVATE xdprocks ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(server_exe PRIVATE ${XDPROCKS_INCLUDE_PATH})
target_compile_definitions(server_exe PRIVATE ROCKSDB_NAMESPACE=xdprocks XDPROCKS)

add_executable(rocksdb_server_exe ${SERVER_SOURCES})
target_link_libraries(rocksdb_server_exe PRIVATE rocksdb ${CMAKE_THREAD_LIBS_INIT} -ldl -lz -lsnappy -lbz2)
target_include_directories(rocksdb_server_exe PRIVATE ${ROCKSDB_INCLUDE_PATH})
target_compile_definitions(rocksdb_server_exe PRIVATE ROCKSDB_NAMESPACE=rocksdb)
