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
PROJECT(SharemindModTableDbHdf5 VERSION 0.3.0 LANGUAGES C CXX)

INCLUDE("${CMAKE_CURRENT_SOURCE_DIR}/config.local" OPTIONAL)
INCLUDE("${CMAKE_CURRENT_BINARY_DIR}/config.local" OPTIONAL)

FIND_PACKAGE(SharemindCMakeHelpers 1.6 REQUIRED)

FIND_PACKAGE(Boost 1.62 COMPONENTS filesystem system REQUIRED)
FIND_PACKAGE(HDF5 COMPONENTS C REQUIRED)
FIND_PACKAGE(LogHard 0.5.0 REQUIRED)
FIND_PACKAGE(SharemindCxxHeaders 0.8.0 REQUIRED)
FIND_PACKAGE(SharemindLibConfiguration 0.5.0 REQUIRED)
FIND_PACKAGE(SharemindLibConsensusService 0.2.0 REQUIRED)
FIND_PACKAGE(SharemindDataStoreApi 0.1.0 REQUIRED)
FIND_PACKAGE(SharemindLibDbCommon 0.2.0 REQUIRED)
FIND_PACKAGE(SharemindLibProcessFacility 0.2.0 REQUIRED)
FIND_PACKAGE(SharemindModTableDb 0.4.0 REQUIRED)
FIND_PACKAGE(SharemindModuleApis 1.1.0 REQUIRED)


# The module:
SharemindSetCxx14CompileOptions()
FILE(GLOB_RECURSE SharemindModTableDbHdf5_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
FILE(GLOB_RECURSE SharemindModTableDbHdf5_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h")
SharemindAddSharedLibrary(ModTableDbHdf5
    MODULE
    OUTPUT_NAME "sharemind_mod_tabledb_hdf5"
    SOURCES
        ${SharemindModTableDbHdf5_SOURCES}
        ${SharemindModTableDbHdf5_HEADERS}
)
TARGET_INCLUDE_DIRECTORIES(ModTableDbHdf5 PRIVATE ${HDF5_INCLUDE_DIRS})
TARGET_COMPILE_DEFINITIONS(ModTableDbHdf5 PRIVATE "H5_USE_18_API")
TARGET_LINK_LIBRARIES(ModTableDbHdf5
    PRIVATE
        Boost::boost
        Boost::filesystem
        Boost::system
        ${HDF5_LIBRARIES}
        LogHard::LogHard
        Sharemind::CxxHeaders
        Sharemind::DataStoreApi
        Sharemind::LibConfiguration
        Sharemind::LibConsensusService
        Sharemind::LibDbCommon
        Sharemind::LibProcessFacility
        Sharemind::ModTableDb
        Sharemind::ModuleApis
    )

# Configuration files:
INSTALL(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/packaging/configs/sharemind/"
        DESTINATION "/etc/sharemind/"
        COMPONENT "lib"
        EXCLUDE_FROM_ALL)

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
        "| libhdf5-103"
        "libloghard (>= 0.5.0)"
        "libsharemind-mod-tabledb (>= 0.4.0)"
        "libsharemind-configuration (>= 0.5.0)"
        "libstdc++6 (>= 4.8.0)"
  DEB_EXTRA_CONTROL_FILES
      "${CMAKE_CURRENT_SOURCE_DIR}/packaging/debian/conffiles"
)
SharemindAddComponentPackage("debug"
    NAME "libsharemind-mod-tabledb-hdf5-dbg"
    DESCRIPTION "Sharemind TableDB HDF5 module debug symbols"
    DEB_SECTION "debug"
    DEB_DEPENDS
        "libsharemind-mod-tabledb-hdf5 (= ${SharemindModTableDbHdf5_DEB_lib_PACKAGE_VERSION})"
)
SharemindPackagingFinalize()
