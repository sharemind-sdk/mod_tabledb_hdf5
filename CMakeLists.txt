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
PROJECT(SharemindModTableDbHdf5 VERSION 0.1.0 LANGUAGES C CXX)

INCLUDE("${CMAKE_CURRENT_SOURCE_DIR}/config.local" OPTIONAL)
INCLUDE("${CMAKE_CURRENT_BINARY_DIR}/config.local" OPTIONAL)

FIND_PACKAGE(SharemindCMakeHelpers REQUIRED)

SharemindSetProjectVersion()


SET(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS filesystem system REQUIRED)
FIND_PACKAGE(HDF5 COMPONENTS C REQUIRED)
FIND_PACKAGE(LogHard REQUIRED)
FIND_PACKAGE(SharemindCxxHeaders REQUIRED)
FIND_PACKAGE(SharemindLibConfiguration REQUIRED)
FIND_PACKAGE(SharemindLibConsensusService REQUIRED)
FIND_PACKAGE(SharemindLibDataStoreManager REQUIRED)
FIND_PACKAGE(SharemindLibDbCommon REQUIRED)
FIND_PACKAGE(SharemindLibProcessFacility REQUIRED)
FIND_PACKAGE(SharemindModTableDb REQUIRED)
FIND_PACKAGE(SharemindModuleApis REQUIRED)


# The module:
SharemindSetCxx11CompileOptions()
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
SET(BV "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.0")
SharemindAddComponentPackage("lib"
    NAME "libsharemind-mod-tabledb-hdf5"
    DESCRIPTION "Sharemind TableDB HDF5 module"
    DEB_SECTION "libs"
    DEB_DEPENDS
        "libboost-filesystem${BV}"
        "libboost-system${BV}"
        "libc6 (>= 2.19)"
        "libhdf5-8"
        "| libhdf5-100"
        "libloghard (>= 0.1.0)"
        "libsharemind-mod-tabledb (>= 0.1.0)"
        "libsharemind-configuration (>= 0.1.0)"
        "libstdc++6 (>= 4.8.0)"
)
SharemindAddComponentPackage("debug"
    NAME "libsharemind-mod-tabledb-hdf5-dbg"
    DESCRIPTION "Sharemind TableDB HDF5 module debug symbols"
    DEB_SECTION "debug"
    DEB_DEPENDS
        "libsharemind-mod-tabledb-hdf5 (= ${SharemindModTableDbHdf5_VERSION})"
)
SharemindPackagingFinalize()
