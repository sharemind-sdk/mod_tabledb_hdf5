#
# This file is a part of the Sharemind framework.
# Copyright (C) Cybernetica AS
#
# All rights are reserved. Reproduction in whole or part is prohibited
# without the written consent of the copyright owner. The usage of this
# code is subject to the appropriate license agreement.
#
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(SHAREMIND_MOD_TABLEDB_HDF5)

# Find packages:
SET(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS filesystem thread REQUIRED)
FIND_PACKAGE(HDF5 REQUIRED)
FIND_PACKAGE(SharemindLibmodapi REQUIRED)
INCLUDE_DIRECTORIES(
    ${Boost_INCLUDE_DIRS}
    ${HDF5_INCLUDE_DIRS}
    ${SharemindLibmodapi_INCLUDE_DIRS}
)
INCLUDE_DIRECTORIES(BEFORE "${CMAKE_CURRENT_SOURCE_DIR}/../../")

ADD_DEFINITIONS(
    "-DSHAREMIND_LOGLEVEL_${LOGLEVEL}"
    "-D__STDC_LIMIT_MACROS"
    "-D__STDC_CONSTANT_MACROS"
    "-D__STDC_FORMAT_MACROS"
    ${SharemindLibmodapi_DEFINITIONS}
)

SET(TABLEDB_HDF5_SOURCES
    TdbHdf5Connection.cpp
    TdbHdf5ConnectionConf.cpp
    TdbHdf5Manager.cpp
    TdbHdf5Module.cpp
    mod_tabledb_hdf5.cpp
)

SET(TABLEDB_HDF5_HEADERS
    TdbHdf5Connection.h
    TdbHdf5ConnectionConf.h
    TdbHdf5Manager.h
    TdbHdf5Module.h
)

# compile the library
ADD_LIBRARY(sharemind_mod_tabledb_hdf5 MODULE ${TABLEDB_HDF5_SOURCES} ${TABLEDB_HDF5_HEADERS})
SET_TARGET_PROPERTIES(sharemind_mod_tabledb_hdf5 PROPERTIES
                      COMPILE_FLAGS "-O2 -ggdb")

# Use these libraries during linking
TARGET_LINK_LIBRARIES(sharemind_mod_tabledb_hdf5

################################################################################
##  THE FOLLOWING LIBRARIES ARE CURRENTLY PROVIDED HERE ONLY BECAUSE OUR      ##
##  INTERFACES ARE EPICALLY BROKEN. THIS LINK-LEVEL DEPENDENCY HACK MUST BE   ##
##  REMOVED, BECAUSE IT MAY CAUSE RUNTIME DYNAMIC LINKER ERRORS.              ##
################################################################################

    ${HDF5_LIBRARIES}
    ${Boost_LIBRARIES}
)

# Install the library and headers
INSTALL(TARGETS sharemind_mod_tabledb_hdf5
        LIBRARY DESTINATION "lib")
