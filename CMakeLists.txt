#
# This file is a part of the Sharemind framework.
# Copyright (C) Cybernetica AS
#
# All rights are reserved. Reproduction in whole or part is prohibited
# without the written consent of the copyright owner. The usage of this
# code is subject to the appropriate license agreement.
#

CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
# The C language is only required for FIND_PACKAGE(HDF5 COMPONENTS C):
PROJECT(SharemindModTableDbHdf5 VERSION 0.2.3 LANGUAGES C CXX)

INCLUDE("${CMAKE_CURRENT_SOURCE_DIR}/config.local" OPTIONAL)
INCLUDE("${CMAKE_CURRENT_BINARY_DIR}/config.local" OPTIONAL)

FIND_PACKAGE(SharemindCMakeHelpers 1.4 REQUIRED)


SET(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS filesystem system REQUIRED)
FIND_PACKAGE(HDF5 COMPONENTS C REQUIRED)
FIND_PACKAGE(LogHard 0.3.0 REQUIRED)
FIND_PACKAGE(SharemindCxxHeaders 0.5.0 REQUIRED)
FIND_PACKAGE(SharemindLibConfiguration 0.2.0 REQUIRED)
FIND_PACKAGE(SharemindLibConsensusService 0.1.0 REQUIRED)
FIND_PACKAGE(SharemindLibDataStoreManager 0.1.0 REQUIRED)
FIND_PACKAGE(SharemindLibDbCommon 0.1.1 REQUIRED)
FIND_PACKAGE(SharemindLibProcessFacility 0.1.0 REQUIRED)
FIND_PACKAGE(SharemindModTableDb 0.2.1 REQUIRED)
FIND_PACKAGE(SharemindModuleApis 1.0.0 REQUIRED)


# The module:
SharemindSetCxx14CompileOptions()
FILE(GLOB_RECURSE SharemindModTableDbHdf5_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
FILE(GLOB_RECURSE SharemindModTableDbHdf5_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h")
SharemindAddSharedLibrary("sharemind_mod_tabledb_hdf5"
    MODULE
    SOURCES
        ${SharemindModTableDbHdf5_SOURCES}
        ${SharemindModTableDbHdf5_HEADERS}
    INCLUDE_DIRECTORIES
        ${Boost_INCLUDE_DIRS}
        ${HDF5_INCLUDE_DIRS}
        ${LogHard_INCLUDE_DIRS}
        ${SharemindCxxHeaders_INCLUDE_DIRS}
        ${SharemindLibConfiguration_INCLUDE_DIRS}
        ${SharemindLibConsensusService_INCLUDE_DIRS}
        ${SharemindLibDataStoreManager_INCLUDE_DIRS}
        ${SharemindLibDbCommon_INCLUDE_DIRS}
        ${SharemindLibProcessFacility_INCLUDE_DIRS}
        ${SharemindModTableDb_INCLUDE_DIRS}
        ${SharemindModuleApis_INCLUDE_DIRS}
    COMPILE_DEFINITIONS
        "H5_USE_18_API"
    LEGACY_DEFINITIONS
        ${LogHard_DEFINITIONS}
        ${SharemindCxxHeaders_DEFINITIONS}
        ${SharemindLibConfiguration_DEFINITIONS}
        ${SharemindLibConsensusService_DEFINITIONS}
        ${SharemindLibDataStoreManager_DEFINITIONS}
        ${SharemindLibDbCommon_DEFINITIONS}
        ${SharemindLibProcessFacility_DEFINITIONS}
        ${SharemindModTableDb_DEFINITIONS}
    LINK_LIBRARIES
        ${Boost_LIBRARIES}
        ${HDF5_LIBRARIES}
        ${LogHard_LIBRARIES}
        ${SharemindCxxHeaders_LIBRARIES}
        ${SharemindLibConfiguration_LIBRARIES}
        ${SharemindLibConsensusService_LIBRARIES}
        ${SharemindLibDataStoreManager_LIBRARIES}
        ${SharemindLibDbCommon_LIBRARIES}
        ${SharemindLibProcessFacility_LIBRARIES}
        ${SharemindModTableDb_LIBRARIES}
)


# Packaging:
SharemindSetupPackaging()
SET(BV
    "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}"
)

SharemindAddComponentPackage("lib"
    NAME "libsharemind-mod-tabledb-hdf5"
    DESCRIPTION "Sharemind TableDB HDF5 module"
    DEB_SECTION "libs"
    DEB_DEPENDS
        "libboost-filesystem${BV}"
        "libboost-system${BV}"
        "libc6 (>= 2.19)"
        "libhdf5-10"
        "| libhdf5-100"
        "libloghard (>= 0.3.0)"
        "libsharemind-mod-tabledb (>= 0.1.0)"
        "libsharemind-configuration (>= 0.2.0)"
        "libstdc++6 (>= 4.8.0)"
)
SharemindAddComponentPackage("debug"
    NAME "libsharemind-mod-tabledb-hdf5-dbg"
    DESCRIPTION "Sharemind TableDB HDF5 module debug symbols"
    DEB_SECTION "debug"
    DEB_DEPENDS
        "libsharemind-mod-tabledb-hdf5 (= ${SharemindModTableDbHdf5_DEB_lib_PACKAGE_VERSION})"
)
SharemindPackagingFinalize()
