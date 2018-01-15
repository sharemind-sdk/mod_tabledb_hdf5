/*
 * Copyright (C) 2015 Cybernetica
 *
 * Research/Commercial License Usage
 * Licensees holding a valid Research License or Commercial License
 * for the Software may use this file according to the written
 * agreement between you and Cybernetica.
 *
 * GNU General Public License Usage
 * Alternatively, this file may be used under the terms of the GNU
 * General Public License version 3.0 as published by the Free Software
 * Foundation and appearing in the file LICENSE.GPL included in the
 * packaging of this file.  Please review the following information to
 * ensure the GNU General Public License version 3.0 requirements will be
 * met: http://www.gnu.org/copyleft/gpl-3.0.html.
 *
 * For further information, please contact us at sharemind@cyber.ee.
 */

#include <cassert>
#include <new>
#include <sstream>
#include <string>
#include <boost/scope_exit.hpp>
#include <sharemind/AssertReturn.h>
#include <sharemind/compiler-support/GccIsNothrowDestructible.h>
#include <sharemind/datastoreapi.h>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>
#include <sharemind/mod_tabledb/TdbTypesUtil.h>
#include <sharemind/module-apis/api_0x1.h>
#include "TdbHdf5Connection.h"
#include "TdbHdf5Module.h"


namespace {

using namespace sharemind;

template <std::size_t NumArgs,
          bool NeedReturnValue = false,
          std::size_t NumRefs = 0u,
          std::size_t NumCRefs = 0u>
struct SyscallArgs {
    static bool check(std::size_t numArgs,
                      SharemindModuleApi0x1Reference const * refs,
                      SharemindModuleApi0x1CReference const * crefs,
                      SharemindCodeBlock * returnValue)
    {
        if (numArgs != NumArgs)
            return false;
        if (NeedReturnValue && !returnValue)
            return false;
        if (refs) {
            std::size_t i = 0u;
            for (; refs[i].pData; ++i);
            if (i != NumRefs)
                return false;
        } else if (NumRefs != 0u) {
            return false;
        }
        if (crefs) {
            std::size_t i = 0u;
            for (; crefs[i].pData; ++i);
            if (i != NumCRefs)
                return false;
        } else if (NumCRefs != 0u) {
            return false;
        }
        return true;
    }
};

#define MOD_TABLEDB_HDF5_SYSCALL(name) \
    SHAREMIND_MODULE_API_0x1_SYSCALL(name, args, num_args, refs, crefs, \
                                     returnValue, c)
#define CHECKARGS(...) \
    SyscallArgs<__VA_ARGS__>::check(num_args, refs, crefs, returnValue)
#define GETMODULEHANDLE \
    *sharemind::assertReturn( \
        static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle))

namespace {

template <typename Ref>
bool haveNtcsRefs(Ref * refs, std::size_t howManyToCheck) noexcept {
    for (; howManyToCheck; --howManyToCheck, ++refs)
        if (refs->size == 0
            || static_cast<const char *>(refs->pData)[refs->size - 1u] != '\0')
            return false;
    return true;
}

template <typename T>
std::string refToString(T const & ref)
{ return std::string(static_cast<char const *>(ref.pData), ref.size - 1u); }

} // anonymous namespace

MOD_TABLEDB_HDF5_SYSCALL(tdb_open) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, false, 0u, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    try {
        auto const dsName(refToString(crefs[0u]));

        auto & m = GETMODULEHANDLE;

        if (!m.openConnection(c, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_close) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, false, 0u, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    try {
        auto const dsName(refToString(crefs[0u]));

        auto & m = GETMODULEHANDLE;

        if (!m.closeConnection(c, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_create) {
    assert(c);
    if (!CHECKARGS(2u, false, 0u, 4u) && !CHECKARGS(2u, false, 1u, 4u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 4u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        const uint64_t typeSize = args[0u].uint64[0u];
        const uint64_t ncols = args[1u].uint64[0u];

        if (ncols == 0)
            return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

        auto & m = GETMODULEHANDLE;

        // Get the connection
        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Set some parameters
        std::vector<SharemindTdbString *> namesVec;

        BOOST_SCOPE_EXIT_ALL(&namesVec) {
            for (auto * const stringPtr : namesVec)
                SharemindTdbString_delete(stringPtr);
            namesVec.clear();
        };

        std::ostringstream oss;
        for (uint64_t i = 0; i < ncols; ++i) {
            oss << i;
            auto const str(oss.str());
            auto * const s = SharemindTdbString_new2(str.c_str(), str.size());
            try {
                namesVec.emplace_back(s);
            } catch (...) {
                SharemindTdbString_delete(s);
                throw;
            }

            oss.str(""); oss.clear();
        }

        auto * const type =
                SharemindTdbType_new2(
                    static_cast<const char *>(crefs[2u].pData),
                    crefs[2u].size - 1u,
                    static_cast<const char *>(crefs[3u].pData),
                    crefs[3u].size - 1u,
                    typeSize);

        BOOST_SCOPE_EXIT_ALL(type) {
            SharemindTdbType_delete(type);
        };

        const std::vector<SharemindTdbType *> typesVec(ncols, type);

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblCreate,
                                       std::cref(tblName),
                                       std::cref(namesVec),
                                       std::cref(typesVec));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_create2) {
    assert(c);
    if (!CHECKARGS(1u, false, 0u, 2u) && !CHECKARGS(1u, false, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const uint64_t vmapId = args[0].uint64[0];

        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        // Get the parameter map
        SharemindTdbVectorMap * const pmap = m.getVectorMap(c, vmapId);
        if (!pmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        size_t size = 0;

        // Parse the "names" parameter
        SharemindTdbString ** names;
        if (pmap->get_string_vector(pmap, "names", &names, &size)
            != TDB_VECTOR_MAP_OK)
        {
            m.logger().error() << "Failed to get \"names\" string vector "
                                  "parameter.";
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        const std::vector<SharemindTdbString *> namesVec(names, names + size);

        // Parse the "types" parameter
        SharemindTdbType ** types;
        if (pmap->get_type_vector(pmap, "types", &types, &size)
            != TDB_VECTOR_MAP_OK)
        {
            m.logger().error() << "Failed to get \"types\" type vector "
                                  "parameter.";
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        const std::vector<SharemindTdbType *> typesVec(types, types + size);

        // Get the connection
        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblCreate,
                                       std::cref(tblName),
                                       std::cref(namesVec),
                                       std::cref(typesVec));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;
        } else if (ecode != SHAREMIND_TDB_OK) {
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }
        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_delete) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, false, 0u, 2u) && !CHECKARGS(0u, false, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblDelete,
                                       std::cref(tblName));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_exists) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 2u) && !CHECKARGS(0u, true, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size == sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        bool exists = false;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblExists,
                                       std::cref(tblName),
                                       std::ref(exists));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        returnValue->uint64[0] = exists;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_col_count) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 2u) && !CHECKARGS(0u, true, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColCount,
                                       std::cref(tblName),
                                       std::ref(count));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        returnValue->uint64[0] = count;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_col_names) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 2u) && !CHECKARGS(0u, true, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        std::vector<SharemindTdbString *> namesVec;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColNames,
                                       std::cref(tblName),
                                       std::ref(namesVec));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        // Register cleanup in case we fail to hand over the ownership for the
        // strings
        bool cleanup = true;

        BOOST_SCOPE_EXIT_ALL(&cleanup, &namesVec) {
            if (cleanup) {
                for (auto * const stringPtr : namesVec)
                    SharemindTdbString_delete(stringPtr);
                namesVec.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m.newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, &m, c, vmapId) {
            if (cleanup && !m.deleteVectorMap(c, vmapId))
                m.logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        // Make a copy of the string pointers
        SharemindTdbString ** names = new SharemindTdbString * [namesVec.size()];
        std::copy(namesVec.begin(), namesVec.end(), names);

        // Set the result "names"
        if (rmap->set_string_vector(rmap, "names", names, namesVec.size()) != TDB_VECTOR_MAP_OK) {
            m.logger().error() << "Failed to set \"names\" string vector result.";
            delete[] names;
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        cleanup = false;

        returnValue->uint64[0] = vmapId;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_col_types) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 2u) && !CHECKARGS(0u, true, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        std::vector<SharemindTdbType *> typesVec;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColTypes,
                                       std::cref(tblName),
                                       std::ref(typesVec));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        // Register cleanup in case we fail to hand over the ownership for the
        // types
        bool cleanup = true;

        BOOST_SCOPE_EXIT_ALL(&cleanup, &typesVec) {
            if (cleanup) {
                for (auto * const typePtr : typesVec)
                    SharemindTdbType_delete(typePtr);
                typesVec.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m.newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, &m, c, vmapId) {
            if (cleanup && !m.deleteVectorMap(c, vmapId))
                m.logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        // Make a copy of the type pointers
        SharemindTdbType ** types = new SharemindTdbType * [typesVec.size()];
        std::copy(typesVec.begin(), typesVec.end(), types);

        // Set the result "names"
        if (rmap->set_type_vector(rmap, "types", types, typesVec.size()) != TDB_VECTOR_MAP_OK) {
            m.logger().error() << "Failed to set \"types\" types vector result.";
            delete[] types;
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        cleanup = false;

        returnValue->uint64[0] = vmapId;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_tbl_row_count) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 2u) && !CHECKARGS(0u, true, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblRowCount,
                                       std::cref(tblName),
                                       std::ref(count));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        returnValue->uint64[0] = count;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_insert_row) {
    assert(c);
    if (!CHECKARGS(1u, false, 0u, 5u)
        && !CHECKARGS(1u, false, 1u, 5u)
        && !CHECKARGS(2u, false, 0u, 5u)
        && !CHECKARGS(2u, false, 1u, 5u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 4u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        const uint64_t typeSize = args[0u].uint64[0u];
        const bool valueAsColumn = num_args == 2u ? args[1u].uint64[0u] : false;

        uint64_t bufSize = 0;
        // If the buffer size equal the type size, we assume it is a scalar
        // value and the workaround does not apply to it.
        if (crefs[4u].size == typeSize) {
            bufSize = crefs[4u].size;
        } else {
            // TODO: the following is a workaround! We are always allocating one
            // byte too much as VM does not allow us to allocate 0 sized memory block.
            bufSize = crefs[4u].size - 1;
        }

        auto & m = GETMODULEHANDLE;

        // Get the connection
        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Construct some parameters
        const std::unique_ptr<SharemindTdbValue> val (new SharemindTdbValue);

        val->type =
                SharemindTdbType_new2(
                    static_cast<const char *>(crefs[2u].pData),
                    crefs[2u].size - 1u,
                    static_cast<const char *>(crefs[3u].pData),
                    crefs[3u].size - 1u,
                    typeSize);

        BOOST_SCOPE_EXIT_ALL(&val) {
            SharemindTdbType_delete(val->type);
        };

        val->buffer = const_cast<void *>(crefs[4u].pData);
        val->size = bufSize;

        const std::vector<std::vector<SharemindTdbValue *> > valuesBatch { {  val.get () } };
        const std::vector<bool> valueAsColumnBatch { valueAsColumn };

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::insertRow,
                                       std::cref(tblName),
                                       std::cref(valuesBatch),
                                       std::cref(valueAsColumnBatch));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_insert_row2) {
    assert(c);
    if (!CHECKARGS(1u, false, 0u, 2u) && !CHECKARGS(1u, false, 1u, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const uint64_t vmapId = args[0].uint64[0];

        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        // Get the parameter map
        SharemindTdbVectorMap * const pmap = m.getVectorMap(c, vmapId);
        if (!pmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        size_t batchCount = 0;
        if (pmap->batch_count(pmap, &batchCount) != TDB_VECTOR_MAP_OK) {
            m.logger().error() << "Failed to get parameter vector map batch "
                                  "count.";
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        // Aggregate the parameters
        using ValuesBatchVector =
                std::vector<std::vector<SharemindTdbValue *> >;
        ValuesBatchVector valuesBatch(batchCount);
        std::vector<bool> valueAsColumnBatch;
        valueAsColumnBatch.reserve(batchCount);

        // Process each parameter batch
        for (size_t i = 0; i < batchCount; ++i) {
            if (pmap->set_batch(pmap, i) != TDB_VECTOR_MAP_OK) {
                m.logger().error() << "Failed to iterate parameter vector map "
                                      "batches.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            // Parse the "values" parameter
            size_t size = 0;
            SharemindTdbValue ** values;
            if (pmap->get_value_vector(pmap, "values", &values, &size)
                != TDB_VECTOR_MAP_OK)
            {
                m.logger().error() << "Failed to get \"values\" value vector "
                                      "parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            std::vector<SharemindTdbValue *> & valuesVec = valuesBatch[i];
            valuesVec.reserve(size);
            valuesVec.insert(valuesVec.begin(), values, values + size);

            // Check if the optional parameter "valueAsColumn" is set
            bool rv = false;
            if ((pmap->is_index_vector(pmap, "valueAsColumn", &rv)
                 == TDB_VECTOR_MAP_OK)
                && rv)
            {
                // Parse the "valueAsColumn" parameter
                SharemindTdbIndex ** valueAsColumn;
                if (pmap->get_index_vector(pmap,
                                           "valueAsColumn",
                                           &valueAsColumn,
                                           &size) != TDB_VECTOR_MAP_OK)
                {
                    m.logger().error() << "Failed to get \"valueAsColumn\" "
                                          "index vector parameter.";
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                if (size < 1) {
                    m.logger().error() << "Empty \"valueAsColumn\" index "
                                          "vector parameter!";
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                valueAsColumnBatch.push_back((*valueAsColumn)->idx);
            } else {
                // Set the default value
                valueAsColumnBatch.push_back(false);
            }
        }

        // Get the connection
        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::insertRow,
                                       std::cref(tblName),
                                       std::cref(valuesBatch),
                                       std::cref(valueAsColumnBatch));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;
        } else if (ecode != SHAREMIND_TDB_OK) {
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }
        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_read_col) {
    assert(c);
    if (!CHECKARGS(1u, true, 0u, 2u)
        && !CHECKARGS(1u, true, 1u, 2u)
        && !CHECKARGS(0u, true, 0u, 3u)
        && !CHECKARGS(0u, true, 1u, 3u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!haveNtcsRefs(crefs, 2u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto const tblName(refToString(crefs[1u]));

        auto & m = GETMODULEHANDLE;

        // Get the connection
        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        SharemindTdbError ecode = SHAREMIND_TDB_UNKNOWN_ERROR;

        typedef std::vector<std::vector<SharemindTdbValue *> > ValuesBatchVector;
        ValuesBatchVector valuesBatch;

        if (SyscallArgs<1u, true, 0u, 2u>::check(num_args, nullptr, crefs, returnValue)) {
            // Read the column by column index
            const uint64_t colId = args[0u].uint64[0u];

            // Construct some parameters
            SharemindTdbIndex * const idx = SharemindTdbIndex_new(colId);

            BOOST_SCOPE_EXIT_ALL(idx) {
                SharemindTdbIndex_delete(idx);
            };

            const std::vector<SharemindTdbIndex *> colIdBatch(1, idx);

            // Execute the transaction
            typedef SharemindTdbError (TdbHdf5Connection::*ExecFunc)(const std::string &,
                                                                     const std::vector<SharemindTdbIndex *> &,
                                                                     std::vector<std::vector<SharemindTdbValue *> > &);

            TdbHdf5Transaction transaction(*conn,
                                           static_cast<ExecFunc>(&TdbHdf5Connection::readColumn),
                                           std::cref(tblName),
                                           std::cref(colIdBatch),
                                           std::ref(valuesBatch));
            ecode = m.executeTransaction(transaction, c);
        } else {
            assert((SyscallArgs<0u, true, 0u, 3u>::check(num_args,
                                                         nullptr,
                                                         crefs,
                                                         returnValue)));
            // Construct some parameters
            auto * const idx =
                    SharemindTdbString_new2(
                        static_cast<const char *>(crefs[2u].pData),
                        crefs[2u].size - 1u);

            BOOST_SCOPE_EXIT_ALL(idx) {
                SharemindTdbString_delete(idx);
            };

            const std::vector<SharemindTdbString *> colIdBatch(1, idx);

            // Execute the transaction
            typedef SharemindTdbError (TdbHdf5Connection::*ExecFunc)(const std::string &,
                                                                     const std::vector<SharemindTdbString *> &,
                                                                     std::vector<std::vector<SharemindTdbValue *> > &);

            TdbHdf5Transaction transaction(*conn,
                                           static_cast<ExecFunc>(&TdbHdf5Connection::readColumn),
                                           std::cref(tblName),
                                           std::cref(colIdBatch),
                                           std::ref(valuesBatch));
            ecode = m.executeTransaction(transaction, c);
        }

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        assert(valuesBatch.size() == 1u);

        // Register cleanup in case we fail to hand over the ownership for the
        // values
        bool cleanup = true;

        BOOST_SCOPE_EXIT_ALL(&cleanup, &valuesBatch) {
            if (cleanup) {
                for (auto const & batch : valuesBatch)
                    for (auto * const valuePtr : batch)
                        SharemindTdbValue_delete(valuePtr);
                valuesBatch.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m.newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, &m, c, vmapId) {
            if (cleanup && !m.deleteVectorMap(c, vmapId))
                m.logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        for (auto it = valuesBatch.begin(); it != valuesBatch.end(); ++it) {
            std::vector<SharemindTdbValue *> & valuesVec = *it;

            // Make a copy of the value pointers
            SharemindTdbValue ** values = new SharemindTdbValue * [valuesVec.size()];
            std::copy(valuesVec.begin(), valuesVec.end(), values);

            // Add batch (the first batch already exists)
            if (it != valuesBatch.begin() && rmap->add_batch(rmap) != TDB_VECTOR_MAP_OK) {
                m.logger().error() << "Failed to add batch to result vector map.";
                delete[] values;
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            // Set the result "values"
            if (rmap->set_value_vector(rmap, "values", values, valuesVec.size()) != TDB_VECTOR_MAP_OK) {
                m.logger().error() << "Failed to set \"values\" value vector result.";
                delete[] values;
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            valuesVec.clear();
        }

        cleanup = false;

        returnValue->uint64[0] = vmapId;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

MOD_TABLEDB_HDF5_SYSCALL(tdb_table_names) {
    assert(c);
    (void) args;
    if (!CHECKARGS(0u, true, 0u, 1u) && !CHECKARGS(0u, true, 1u, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (refs && refs[0u].size != sizeof(int64_t)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (!haveNtcsRefs(crefs, 1u))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        auto const dsName(refToString(crefs[0u]));
        auto & m = GETMODULEHANDLE;

        TdbHdf5Connection * const conn = m.getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        std::vector<SharemindTdbString *> namesVec;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblNames,
                                       std::ref(namesVec));
        const SharemindTdbError ecode = m.executeTransaction(transaction, c);

        if (!m.setErrorCode(c, dsName, ecode))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        if (refs) {
            *static_cast<int64_t *>(refs[0u].pData) = ecode;

            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_OK;
        } else {
            if (ecode != SHAREMIND_TDB_OK)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        // Register cleanup in case we fail to hand over the ownership for the
        // strings
        bool cleanup = true;

        BOOST_SCOPE_EXIT_ALL(&cleanup, &namesVec) {
            if (cleanup) {
                for (auto * const stringPtr : namesVec)
                    SharemindTdbString_delete(stringPtr);
                namesVec.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m.newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, &m, c, vmapId) {
            if (cleanup && !m.deleteVectorMap(c, vmapId))
                m.logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        // Make a copy of the string pointers
        SharemindTdbString ** names = new SharemindTdbString * [namesVec.size()];
        std::copy(namesVec.begin(), namesVec.end(), names);

        // Set the result "names"
        if (rmap->set_string_vector(rmap, "names", names, namesVec.size()) != TDB_VECTOR_MAP_OK) {
            m.logger().error() << "Failed to set \"names\" string vector result.";
            delete[] names;
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        cleanup = false;

        returnValue->uint64[0] = vmapId;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

} /* namespace { */

extern "C" {

SHAREMIND_MODULE_API_MODULE_INFO("tabledb_hdf5",
                          0x00010000,
                          0x1);

SHAREMIND_MODULE_API_0x1_INITIALIZER(c) __attribute__ ((visibility("default")));
SHAREMIND_MODULE_API_0x1_INITIALIZER(c) {
    assert(c);

    /*
     * Get facilities
     */
    auto const * flog = c->getModuleFacility(c, "Logger");
    if (!flog || !flog->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    auto const  * fsourcem = c->getModuleFacility(c, "DataSourceManager");
    if (!fsourcem || !fsourcem->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    auto const * fvmaputil = c->getModuleFacility(c, "TdbVectorMapUtil");
    if (!fvmaputil || !fvmaputil->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    SharemindConsensusFacility * consensusService;
    auto const * fconsensus = c->getModuleFacility(c, "ConsensusService");
    if (!fconsensus || !fconsensus->facility) {
        consensusService = nullptr;
    } else {
        consensusService =
                static_cast<SharemindConsensusFacility *>(fconsensus->facility);
    }

    auto const & logger = *static_cast<LogHard::Logger const *>(flog->facility);
    auto & dataSourceManager =
            *static_cast<SharemindDataSourceManager *>(fsourcem->facility);
    auto & mapUtil =
            *static_cast<SharemindTdbVectorMapUtil *>(fvmaputil->facility);

    /*
     * Initialize the module handle
     */
    try {
        c->moduleHandle =
                new sharemind::TdbHdf5Module(logger,
                                             dataSourceManager,
                                             mapUtil,
                                             consensusService);
        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (std::bad_alloc const &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_DEINITIALIZER(c)
        __attribute__ ((visibility("default")));
SHAREMIND_MODULE_API_0x1_DEINITIALIZER(c) {
    assert(c);
    assert(c->moduleHandle);
    using namespace sharemind;
    static_assert(is_nothrow_destructible<TdbHdf5Module>::value, "");
    delete static_cast<TdbHdf5Module *>(c->moduleHandle);
    c->moduleHandle = nullptr;
}

SHAREMIND_MODULE_API_0x1_SYSCALL_DEFINITIONS(

    /* High level database operations */
      { "tdb_open",             &tdb_open }
    , { "tdb_close",            &tdb_close }
    , { "tdb_table_names",      &tdb_table_names }

    /* Table database API */
    , { "tdb_tbl_create",       &tdb_tbl_create }
    , { "tdb_tbl_create2",      &tdb_tbl_create2 }
    , { "tdb_tbl_delete",       &tdb_tbl_delete }
    , { "tdb_tbl_exists",       &tdb_tbl_exists }
    , { "tdb_tbl_col_count",    &tdb_tbl_col_count }
    , { "tdb_tbl_col_names",    &tdb_tbl_col_names }
    , { "tdb_tbl_col_types",    &tdb_tbl_col_types }
    , { "tdb_tbl_row_count",    &tdb_tbl_row_count }
    , { "tdb_insert_row",       &tdb_insert_row }
    , { "tdb_insert_row2",      &tdb_insert_row2 }
    , { "tdb_read_col",         &tdb_read_col }

);

} /* extern "C" { */
