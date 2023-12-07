if(NOT DEFINED REPLICATOR_XDPROCKS_INCLUDE_PATH)
  message(FATAL_ERROR "REPLICATOR_XDPROCKS_INCLUDE_PATH for the xdprocks header files is not specified")
endif()

if(NOT DEFINED REPLICATOR_XDPROCKS_LIBRARY_PATH)
  message(FATAL_ERROR "REPLICATOR_XDPROCKS_LIBRARY_PATH for the xdprocks library is not specified")
endif()

if (REPLICATOR_ROCKSDB_SERVER)
  if(NOT DEFINED REPLICATOR_ROCKSDB_INCLUDE_PATH)
    message(FATAL_ERROR "REPLICATOR_ROCKSDB_INCLUDE_PATH for the rocksdb header files is not specified")
  endif()

  if(NOT DEFINED REPLICATOR_ROCKSDB_LIBRARY_PATH)
    message(FATAL_ERROR "REPLICATOR_ROCKSDB_LIBRARY_PATH for the rocksdb library is not specified")
  endif()
endif()


include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/pliops)

link_directories(${REPLICATOR_XDPROCKS_LIBRARY_PATH} ${REPLICATOR_ROCKSDB_LIBRARY_PATH})

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
target_link_libraries(client_exe PRIVATE -pthread xdprocks ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(client_exe PRIVATE ${REPLICATOR_XDPROCKS_INCLUDE_PATH})
target_compile_definitions(client_exe PRIVATE ROCKSDB_NAMESPACE=xdprocks XDPROCKS)
target_compile_options(client_exe PRIVATE -w -Wno-error=unused-function -Wno-error=reorder)

add_executable(server_exe ${SERVER_SOURCES})
target_link_libraries(server_exe PRIVATE -pthread xdprocks ${CMAKE_THREAD_LIBS_INIT})
target_include_directories(server_exe PRIVATE ${REPLICATOR_XDPROCKS_INCLUDE_PATH})
target_compile_definitions(server_exe PRIVATE ROCKSDB_NAMESPACE=xdprocks XDPROCKS)
target_compile_options(server_exe PRIVATE -w -Wno-error=unused-function -Wno-error=reorder)

if (REPLICATOR_ROCKSDB_SERVER)
  add_executable(rocksdb_server_exe ${SERVER_SOURCES})
  target_link_libraries(rocksdb_server_exe PRIVATE -pthread rocksdb ${CMAKE_THREAD_LIBS_INIT} -ldl -lz -lsnappy -lbz2)
  target_include_directories(rocksdb_server_exe PRIVATE ${REPLICATOR_ROCKSDB_INCLUDE_PATH})
  target_compile_definitions(rocksdb_server_exe PRIVATE ROCKSDB_NAMESPACE=rocksdb)
  target_compile_options(rocksdb_server_exe PRIVATE -w -Wno-error=unused-function -Wno-error=reorder)
endif()
