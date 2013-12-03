#
# This file is a part of the Sharemind framework.
# Copyright (C) Cybernetica AS
#
# All rights are reserved. Reproduction in whole or part is prohibited
# without the written consent of the copyright owner. The usage of this
# code is subject to the appropriate license agreement.
#

# The tabledb_hdf5 module

# Find packages:
SET(Boost_USE_STATIC_RUNTIME OFF)

FOREACH(dir IN LISTS CMAKE_PREFIX_PATH)
  INCLUDE_DIRECTORIES(BEFORE SYSTEM "${dir}/include")
ENDFOREACH()

FIND_PACKAGE(Boost COMPONENTS filesystem thread REQUIRED)
INCLUDE_DIRECTORIES(
    ${Boost_INCLUDE_DIRS}
    ${HDF5_INCLUDE_DIRS}
)

ADD_DEFINITIONS("-DSHAREMIND_LOGLEVEL_${LOGLEVEL}"
                "-D__STDC_LIMIT_MACROS"
                "-D__STDC_CONSTANT_MACROS"
                "-D__STDC_FORMAT_MACROS")

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
    ${Boost_FILESYSTEM_LIBRARY}
)

# Install the library and headers
INSTALL(TARGETS sharemind_mod_tabledb_hdf5
        LIBRARY DESTINATION "lib")
