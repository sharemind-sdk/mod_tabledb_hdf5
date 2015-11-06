#
# This file is a part of the Sharemind framework.
# Copyright (C) Cybernetica AS
#
# All rights are reserved. Reproduction in whole or part is prohibited
# without the written consent of the copyright owner. The usage of this
# code is subject to the appropriate license agreement.
#
CMAKE_MINIMUM_REQUIRED(VERSION 2.8.12)
PROJECT(SHAREMIND_MOD_TABLEDB_HDF5)

INCLUDE("${CMAKE_CURRENT_SOURCE_DIR}/config.local" OPTIONAL)
INCLUDE("${CMAKE_CURRENT_BINARY_DIR}/config.local" OPTIONAL)

ADD_COMPILE_OPTIONS(
  "-std=c++11" "-Wall" "-Wextra" "-O2" "-fPIC"
  "$<$<NOT:$<STREQUAL:$<CONFIGURATION>,Release>>:-ggdb>"
  "$<$<NOT:$<STREQUAL:$<CONFIGURATION>,Release>>:-fno-omit-frame-pointer>"
  "$<$<STREQUAL:$<CONFIGURATION>,Release>:-DNDEBUG>"
  "$<$<STREQUAL:$<CONFIGURATION>,Release>:-fomit-frame-pointer>"
)

SET(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS filesystem system thread REQUIRED)
FIND_PACKAGE(HDF5 REQUIRED)
FIND_PACKAGE(LogHard REQUIRED)
FIND_PACKAGE(SharemindCxxHeaders REQUIRED)
FIND_PACKAGE(SharemindLibDataStoreManager REQUIRED)
FIND_PACKAGE(SharemindLibDbCommon REQUIRED)
FIND_PACKAGE(SharemindLibmodapi REQUIRED)
FIND_PACKAGE(SharemindLibProcessFacility REQUIRED)
FIND_PACKAGE(SharemindMiner REQUIRED)
FIND_PACKAGE(SharemindModTableDb REQUIRED)
INCLUDE_DIRECTORIES(
    ${Boost_INCLUDE_DIRS}
    ${HDF5_INCLUDE_DIRS}
    ${LogHard_INCLUDE_DIRS}
    ${SharemindCxxHeaders_INCLUDE_DIRS}
    ${SharemindLibDataStoreManager_INCLUDE_DIRS}
    ${SharemindLibDbCommon_INCLUDE_DIRS}
    ${SharemindLibmodapi_INCLUDE_DIRS}
    ${SharemindLibProcessFacility_INCLUDE_DIRS}
    ${SharemindMiner_Dsa_INCLUDE_DIRS}
    ${SharemindMiner_LibConsensusService_INCLUDE_DIRS}
    ${SharemindModTableDb_INCLUDE_DIRS}
)

ADD_DEFINITIONS(
    "-D__STDC_LIMIT_MACROS"
    "-D__STDC_CONSTANT_MACROS"
    "-D__STDC_FORMAT_MACROS"
    ${LogHard_DEFINITIONS}
    ${SharemindCxxHeaders_DEFINITIONS}
    ${SharemindLibDataStoreManager_DEFINITIONS}
    ${SharemindLibDbCommon_DEFINITIONS}
    ${SharemindLibmodapi_DEFINITIONS}
    ${SharemindLibProcessFacility_DEFINITIONS}
    ${SharemindMiner_Dsa_DEFINITIONS}
    ${SharemindMiner_LibConsensusService_DEFINITIONS}
    ${SharemindModTableDb_DEFINITIONS}
)

FILE(GLOB_RECURSE TABLEDB_HDF5_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
FILE(GLOB_RECURSE TABLEDB_HDF5_HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h")

ADD_LIBRARY(sharemind_mod_tabledb_hdf5 MODULE ${TABLEDB_HDF5_SOURCES} ${TABLEDB_HDF5_HEADERS})
TARGET_LINK_LIBRARIES(sharemind_mod_tabledb_hdf5
    ${HDF5_LIBRARIES}
    ${Boost_LIBRARIES}
    ${LogHard_LIBRARIES}
    ${SharemindCxxHeaders_LIBRARIES}
    ${SharemindLibProcessFacility_LIBRARIES}
    ${SharemindMiner_Dsa_LIBRARIES}
    ${SharemindMiner_LibConsensusService_LIBRARIES}
    ${SharemindModTableDb_LIBRARIES}
)

INSTALL(TARGETS sharemind_mod_tabledb_hdf5 LIBRARY DESTINATION "lib")
