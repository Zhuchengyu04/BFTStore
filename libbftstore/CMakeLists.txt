cmake_minimum_required(VERSION 3.4)

include(cmake/ProjectBoost.cmake)
include(cmake/ProjectRocksDB.cmake)
include(cmake/ProjectSecp256k1.cmake)
include(cmake/ProjectSnappy.cmake)
include(cmake/ProjectTBB.cmake)

file(GLOB SRC_LIST  "src/.cpp")


add_subdirectory(gf-arithmetic)
add_subdirectory(erasure-codes)
add_subdirectory(lohi-generator)
add_subdirectory(log-table-generator)

add_library(bftstore ${SRC_LIST} ${HEADERS})

add_compile_options(-Wno-unused-variable)
add_compile_options(-Wno-unused-parameter)
add_compile_options(-Wno-unused-function)
add_compile_options(-Wno-missing-field-initializers)
add_compile_options(-Wno-unknown-pragmas)
add_compile_options(-fno-omit-frame-pointer)
add_compile_options(-Wno-dev)
add_compile_options(-Wno-return-type)

target_include_directories(bftstore SYSTEM BEFORE PUBLIC include ${BOOST_INCLUDE_DIR})

target_link_libraries(bftstore PUBLIC erasure TBB RocksDB Secp256k1 Boost::Filesystem)