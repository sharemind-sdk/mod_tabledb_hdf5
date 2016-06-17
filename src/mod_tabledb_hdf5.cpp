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
#include <sharemind/datastoreapi.h>
#include <sharemind/libmodapi/api_0x1.h>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>
#include <sharemind/mod_tabledb/TdbTypesUtil.h>
#include "TdbHdf5Connection.h"
#include "TdbHdf5Module.h"


namespace {

using namespace sharemind;

template < size_t NumArgs
         , bool   NeedReturnValue = false
         , size_t NumRefs = 0
         , size_t NumCRefs = 0
         >
struct SyscallArgs {
    static inline bool check(SharemindCodeBlock * args,
                             size_t num_args,
                             const SharemindModuleApi0x1Reference* refs,
                             const SharemindModuleApi0x1CReference* crefs,
                             SharemindCodeBlock * returnValue)
    {
        (void) args;

        if (num_args != NumArgs) {
            return false;
        }

        if (NeedReturnValue && ! returnValue) {
            return false;
        }

        if (refs != 0) {
            size_t i = 0;
            for (; refs[i].pData != 0; ++ i);
            if (i != NumRefs)
                return false;
        }
        else {
            if (NumRefs != 0)
                return false;
        }

        if (crefs != 0) {
            size_t i = 0;
            for (; crefs[i].pData != 0; ++ i);
            if (i != NumCRefs)
                return false;
        }
        else {
            if (NumCRefs != 0)
                return false;
        }

        return true;
    }
};

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_open,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, false, 0u, 1u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->openConnection(c, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_close,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, false, 0u, 1u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->closeConnection(c, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_MODULE_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_create,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<2u, false, 0u, 4u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<2u, false, 1u, 4u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || crefs[3u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[3u].pData)[crefs[3u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string typeDomain(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);
        const std::string typeName(static_cast<const char *>(crefs[3u].pData), crefs[3u].size - 1u);

        const uint64_t typeSize = args[0u].uint64[0u];
        const uint64_t ncols = args[1u].uint64[0u];

        if (ncols == 0)
            return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the connection
        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Set some parameters
        std::vector<SharemindTdbString *> namesVec;

        BOOST_SCOPE_EXIT_ALL(&namesVec) {
            std::vector<SharemindTdbString *>::iterator it;
            for (it = namesVec.begin(); it != namesVec.end(); ++it)
                SharemindTdbString_delete(*it);
            namesVec.clear();
        };

        std::ostringstream oss;
        for (uint64_t i = 0; i < ncols; ++i) {
            oss << i;
            namesVec.push_back(SharemindTdbString_new(oss.str()));
            oss.str(""); oss.clear();
        }

        SharemindTdbType * const type = SharemindTdbType_new(typeDomain, typeName, typeSize);

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
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_delete,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, false, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, false, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblDelete,
                                       std::cref(tblName));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_exists,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size == sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        bool exists = false;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblExists,
                                       std::cref(tblName),
                                       std::ref(exists));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_col_count,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColCount,
                                       std::cref(tblName),
                                       std::ref(count));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_col_names,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        std::vector<SharemindTdbString *> namesVec;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColNames,
                                       std::cref(tblName),
                                       std::ref(namesVec));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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
                std::vector<SharemindTdbString *>::iterator it;
                for (it = namesVec.begin(); it != namesVec.end(); ++it)
                    SharemindTdbString_delete(*it);
                namesVec.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m->newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, m, c, vmapId) {
            if (cleanup && !m->deleteVectorMap(c, vmapId))
                m->logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        // Make a copy of the string pointers
        SharemindTdbString ** names = new SharemindTdbString * [namesVec.size()];
        std::copy(namesVec.begin(), namesVec.end(), names);

        // Set the result "names"
        if (rmap->set_string_vector(rmap, "names", names, namesVec.size()) != TDB_VECTOR_MAP_OK) {
            m->logger().error() << "Failed to set \"names\" string vector result.";
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_col_types,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        std::vector<SharemindTdbType *> typesVec;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblColTypes,
                                       std::cref(tblName),
                                       std::ref(typesVec));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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
                std::vector<SharemindTdbType *>::iterator it;
                for (it = typesVec.begin(); it != typesVec.end(); ++it)
                    SharemindTdbType_delete(*it);
                typesVec.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m->newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, m, c, vmapId) {
            if (cleanup && !m->deleteVectorMap(c, vmapId))
                m->logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        // Make a copy of the type pointers
        SharemindTdbType ** types = new SharemindTdbType * [typesVec.size()];
        std::copy(typesVec.begin(), typesVec.end(), types);

        // Set the result "names"
        if (rmap->set_type_vector(rmap, "types", types, typesVec.size()) != TDB_VECTOR_MAP_OK) {
            m->logger().error() << "Failed to set \"types\" types vector result.";
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_row_count,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn,
                                       &TdbHdf5Connection::tblRowCount,
                                       std::cref(tblName),
                                       std::ref(count));
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_insert_row,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<1u, false, 0u, 5u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<1u, false, 1u, 5u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<2u, false, 0u, 5u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<2u, false, 1u, 5u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || crefs[3u].size == 0u
            || crefs[4u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[3u].pData)[crefs[3u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string typeDomain(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);
        const std::string typeName(static_cast<const char *>(crefs[3u].pData), crefs[3u].size - 1u);

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

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the connection
        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Construct some parameters
        const std::unique_ptr<SharemindTdbValue> val (new SharemindTdbValue);

        val->type = SharemindTdbType_new(typeDomain, typeName, typeSize);

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
        const SharemindTdbError ecode = m->executeTransaction(transaction, c);

        if (!m->setErrorCode(c, dsName, ecode))
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_read_col,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    assert(c);
    if (!SyscallArgs<1u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<1u, true, 1u, 2u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 0u, 3u>::check(args, num_args, refs, crefs, returnValue) &&
            !SyscallArgs<0u, true, 1u, 3u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the connection
        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        SharemindTdbError ecode = SHAREMIND_TDB_UNKNOWN_ERROR;

        typedef std::vector<std::vector<SharemindTdbValue *> > ValuesBatchVector;
        ValuesBatchVector valuesBatch;

        if (SyscallArgs<1u, true, 0u, 2u>::check(args, num_args, nullptr, crefs, returnValue)) {
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
            ecode = m->executeTransaction(transaction, c);
        } else {
            assert((SyscallArgs<0u, true, 0u, 3u>::check(args,
                                                         num_args,
                                                         nullptr,
                                                         crefs,
                                                         returnValue)));
            // Read the column by column name
            const std::string colId(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);

            // Construct some parameters
            SharemindTdbString * const idx = SharemindTdbString_new(colId);

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
            ecode = m->executeTransaction(transaction, c);
        }

        if (!m->setErrorCode(c, dsName, ecode))
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
                std::vector<std::vector<SharemindTdbValue *> >::iterator it;
                std::vector<SharemindTdbValue *>::iterator innerIt;
                for (it = valuesBatch.begin(); it != valuesBatch.end(); ++it) {
                    for (innerIt = it->begin(); innerIt != it->end(); ++innerIt)
                        SharemindTdbValue_delete(*innerIt);
                }
                valuesBatch.clear();
            }
        };

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m->newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Register cleanup for the result vector map
        BOOST_SCOPE_EXIT_ALL(&cleanup, m, c, vmapId) {
            if (cleanup && !m->deleteVectorMap(c, vmapId))
                m->logger().fullDebug() << "Error while cleaning up result vector map.";
        };

        ValuesBatchVector::iterator it;
        for (it = valuesBatch.begin(); it != valuesBatch.end(); it = ++it) {
            std::vector<SharemindTdbValue *> & valuesVec = *it;

            // Make a copy of the value pointers
            SharemindTdbValue ** values = new SharemindTdbValue * [valuesVec.size()];
            std::copy(valuesVec.begin(), valuesVec.end(), values);

            // Add batch (the first batch already exists)
            if (it != valuesBatch.begin() && rmap->add_batch(rmap) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to add batch to result vector map.";
                delete[] values;
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            // Set the result "values"
            if (rmap->set_value_vector(rmap, "values", values, valuesVec.size()) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to set \"values\" value vector result.";
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

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_stmt_exec,
                                 args, num_args, refs, crefs,
                                 returnValue, c)
{
    assert(c);
    if (!SyscallArgs<1u, false, 0u, 3u>::check(args, num_args, refs, crefs, returnValue)
            && !SyscallArgs<1u, false, 1u, 3u>::check(args, num_args, refs, crefs, returnValue)
            && !SyscallArgs<1u, true, 0u, 3u>::check(args, num_args, refs, crefs, returnValue)
            && !SyscallArgs<1u, true, 1u, 3u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (refs && refs[0u].size != sizeof(int64_t))
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    try {
        const uint64_t vmapId = args[0].uint64[0];

        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string stmtType(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the parameter map
        SharemindTdbVectorMap * const pmap = m->getVectorMap(c, vmapId);
        if (!pmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Parse the parameters depending on the statement type
        // TODO can we do this more efficiently?
        if (stmtType.compare("tbl_create") == 0) {
            size_t size = 0;

            // Parse the "names" parameter
            SharemindTdbString ** names;
            if (pmap->get_string_vector(pmap, "names", &names, &size) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: Failed to get \"names\" string vector parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            const std::vector<SharemindTdbString *> namesVec(names, names + size);

            // Parse the "types" parameter
            SharemindTdbType ** types;
            if (pmap->get_type_vector(pmap, "types", &types, &size) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to execute \"" << stmtType <<
                    "\" statement: Failed to get \"types\" type vector parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            const std::vector<SharemindTdbType *> typesVec(types, types + size);

            // Get the connection
            TdbHdf5Connection * const conn = m->getConnection(c, dsName);
            if (!conn)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            // Execute transaction
            TdbHdf5Transaction transaction(*conn,
                                           &TdbHdf5Connection::tblCreate,
                                           std::cref(tblName),
                                           std::cref(namesVec),
                                           std::cref(typesVec));
            const SharemindTdbError ecode = m->executeTransaction(transaction, c);

            if (!m->setErrorCode(c, dsName, ecode))
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            if (refs) {
                *static_cast<int64_t *>(refs[0u].pData) = ecode;

                if (ecode != SHAREMIND_TDB_OK)
                    return SHAREMIND_MODULE_API_0x1_OK;
            } else {
                if (ecode != SHAREMIND_TDB_OK)
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }
        } else if (stmtType.compare("insert_row") == 0) {
            size_t batchCount = 0;
            if (pmap->batch_count(pmap, &batchCount) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: Failed to get parameter vector map batch count.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            // Aggregate the parameters
            typedef std::vector<std::vector<SharemindTdbValue *> > ValuesBatchVector;
            ValuesBatchVector valuesBatch(batchCount);
            std::vector<bool> valueAsColumnBatch;
            valueAsColumnBatch.reserve(batchCount);

            // Process each parameter batch
            for (size_t i = 0; i < batchCount; ++i) {
                if (pmap->set_batch(pmap, i) != TDB_VECTOR_MAP_OK) {
                    m->logger().error() << "Failed to execute \"" << stmtType
                        << "\" statement: Failed to iterate parameter vector map batches.";
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                // Parse the "values" parameter
                size_t size = 0;
                SharemindTdbValue ** values;
                if (pmap->get_value_vector(pmap, "values", &values, &size) != TDB_VECTOR_MAP_OK) {
                    m->logger().error() << "Failed to execute \"" << stmtType
                        << "\" statement: Failed to get \"values\" value vector parameter.";
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                std::vector<SharemindTdbValue *> & valuesVec = valuesBatch[i];
                valuesVec.reserve(size);
                valuesVec.insert(valuesVec.begin(), values, values + size);

                // Check if the optional parameter "valueAsColumn" is set
                bool rv = false;
                if (pmap->is_index_vector(pmap, "valueAsColumn", &rv) == TDB_VECTOR_MAP_OK && rv) {
                    // Parse the "valueAsColumn" parameter
                    SharemindTdbIndex ** valueAsColumn;
                    if (pmap->get_index_vector(pmap, "valueAsColumn", &valueAsColumn, &size) != TDB_VECTOR_MAP_OK) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Failed to get \"valueAsColumn\" index vector parameter.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    if (size < 1) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Empty \"valueAsColumn\" index vector parameter.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    valueAsColumnBatch.push_back((*valueAsColumn)->idx);
                } else {
                    // Set the default value
                    valueAsColumnBatch.push_back(false);
                }
            }

            // Get the connection
            TdbHdf5Connection * const conn = m->getConnection(c, dsName);
            if (!conn)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            // Execute transaction
            TdbHdf5Transaction transaction(*conn,
                                           &TdbHdf5Connection::insertRow,
                                           std::cref(tblName),
                                           std::cref(valuesBatch),
                                           std::cref(valueAsColumnBatch));
            const SharemindTdbError ecode = m->executeTransaction(transaction, c);

            if (!m->setErrorCode(c, dsName, ecode))
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            if (refs) {
                *static_cast<int64_t *>(refs[0u].pData) = ecode;

                if (ecode != SHAREMIND_TDB_OK)
                    return SHAREMIND_MODULE_API_0x1_OK;
            } else {
                if (ecode != SHAREMIND_TDB_OK)
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }
        } else if (stmtType.compare("read_col") == 0) {
            if (!returnValue) {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: This statement requires a return value.";
                return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
            }

            size_t batchCount = 0;
            if (pmap->batch_count(pmap, &batchCount) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: Failed to get parameter vector map batch count.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            // Get the connection
            TdbHdf5Connection * const conn = m->getConnection(c, dsName);
            if (!conn)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            SharemindTdbError ecode = SHAREMIND_TDB_UNKNOWN_ERROR;

            typedef std::vector<std::vector<SharemindTdbValue *> > ValuesBatchVector;
            ValuesBatchVector valuesBatch;

            bool rv = false;
            if (pmap->is_index_vector(pmap, "colId", &rv) == TDB_VECTOR_MAP_OK && rv) {
                // Aggregate the parameters
                typedef std::vector<SharemindTdbIndex *> ColIdBatchVector;
                ColIdBatchVector colIdBatch;
                colIdBatch.reserve(batchCount);

                // Process each parameter batch
                for (size_t i = 0; i < batchCount; ++i) {
                    if (pmap->set_batch(pmap, i) != TDB_VECTOR_MAP_OK) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Failed to iterate parameter vector map batches.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    // Parse the "colId" parameter
                    size_t size = 0;
                    SharemindTdbIndex ** colId;
                    if (pmap->get_index_vector(pmap, "colId", &colId, &size) != TDB_VECTOR_MAP_OK) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Failed to get \"colId\" index vector parameter.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    if (size != 1) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Expecting single index value parameter per batch.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    colIdBatch.push_back(colId[0]);
                }

                // Execute transaction
                typedef SharemindTdbError (TdbHdf5Connection::*ExecFunc)(const std::string &,
                                                                         const std::vector<SharemindTdbIndex *> &,
                                                                         std::vector<std::vector<SharemindTdbValue *> > &);

                TdbHdf5Transaction transaction(*conn,
                                               static_cast<ExecFunc>(&TdbHdf5Connection::readColumn),
                                               std::cref(tblName),
                                               std::cref(colIdBatch),
                                               std::ref(valuesBatch));
                ecode = m->executeTransaction(transaction, c);

                assert(colIdBatch.size() == valuesBatch.size());
            } else if (pmap->is_string_vector(pmap, "colId", &rv) == TDB_VECTOR_MAP_OK && rv) {
                // Aggregate the parameters
                typedef std::vector<SharemindTdbString *> ColIdBatchVector;
                ColIdBatchVector colIdBatch;
                colIdBatch.reserve(batchCount);

                // Process each parameter batch
                for (size_t i = 0; i < batchCount; ++i) {
                    if (pmap->set_batch(pmap, i) != TDB_VECTOR_MAP_OK) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Failed to iterate parameter vector map batches.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    // Parse the "colId" parameter
                    size_t size = 0;
                    SharemindTdbString ** colId;
                    if (pmap->get_string_vector(pmap, "colId", &colId, &size) != TDB_VECTOR_MAP_OK) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Failed to get \"colId\" index vector parameter.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    if (size != 1) {
                        m->logger().error() << "Failed to execute \"" << stmtType
                            << "\" statement: Expecting single index value parameter per batch.";
                        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                    }

                    colIdBatch.push_back(colId[0]);
                }

                // Execute transaction
                typedef SharemindTdbError (TdbHdf5Connection::*ExecFunc)(const std::string &,
                                                                         const std::vector<SharemindTdbString *> &,
                                                                         std::vector<std::vector<SharemindTdbValue *> > &);

                TdbHdf5Transaction transaction(*conn,
                                               static_cast<ExecFunc>(&TdbHdf5Connection::readColumn),
                                               std::cref(tblName),
                                               std::cref(colIdBatch),
                                               std::ref(valuesBatch));
                ecode = m->executeTransaction(transaction, c);

                assert(colIdBatch.size() == valuesBatch.size());
            } else {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: \"colId\" parameter must be either index or string vector.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            if (!m->setErrorCode(c, dsName, ecode))
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
            // values
            bool cleanup = true;

            BOOST_SCOPE_EXIT_ALL(&cleanup, &valuesBatch) {
                if (cleanup) {
                    std::vector<std::vector<SharemindTdbValue *> >::iterator it;
                    std::vector<SharemindTdbValue *>::iterator innerIt;
                    for (it = valuesBatch.begin(); it != valuesBatch.end(); ++it) {
                        for (innerIt = it->begin(); innerIt != it->end(); ++innerIt)
                            SharemindTdbValue_delete(*innerIt);
                    }
                    valuesBatch.clear();
                }
            };

            // Get the result map
            uint64_t vmapId = 0;
            SharemindTdbVectorMap * const rmap = m->newVectorMap(c, vmapId);
            if (!rmap)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            // Register cleanup for the result vector map
            BOOST_SCOPE_EXIT_ALL(&cleanup, m, c, vmapId) {
                if (cleanup && !m->deleteVectorMap(c, vmapId))
                    m->logger().fullDebug() << "Error while cleaning up result vector map.";
            };

            ValuesBatchVector::iterator it;
            for (it = valuesBatch.begin(); it != valuesBatch.end(); ++it) {
                std::vector<SharemindTdbValue *> & valuesVec = *it;

                // Make a copy of the value pointers
                SharemindTdbValue ** values = new SharemindTdbValue * [valuesVec.size()];
                std::copy(valuesVec.begin(), valuesVec.end(), values);

                // Add batch (the first batch already exists)
                if (it != valuesBatch.begin() && rmap->add_batch(rmap) != TDB_VECTOR_MAP_OK) {
                    m->logger().error() << "Failed to add batch to result vector map.";
                    delete[] values;
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                // Set the result "values"
                if (rmap->set_value_vector(rmap, "values", values, valuesVec.size()) != TDB_VECTOR_MAP_OK) {
                    m->logger().error() << "Failed to set \"values\" value vector result.";
                    delete[] values;
                    return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
                }

                valuesVec.clear();
            }

            cleanup = false;

            returnValue->uint64[0] = vmapId;
        } else {
            m->logger().error() << "Failed to execute \"" << stmtType
                << "\": Unknown statement type.";
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

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
    const SharemindModuleApi0x1Facility * flog = c->getModuleFacility(c, "Logger");
    if (!flog || !flog->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    const SharemindModuleApi0x1Facility * fsourcem = c->getModuleFacility(c, "DataSourceManager");
    if (!fsourcem || !fsourcem->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    const SharemindModuleApi0x1Facility * fvmaputil = c->getModuleFacility(c, "TdbVectorMapUtil");
    if (!fvmaputil || !fvmaputil->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    SharemindConsensusFacility * consensusService;
    const SharemindModuleApi0x1Facility * fconsensus = c->getModuleFacility(c, "ConsensusService");
    if (!fconsensus || !fconsensus->facility) {
        consensusService = nullptr;
    } else {
        consensusService = static_cast<SharemindConsensusFacility *>(fconsensus->facility);
    }

    const LogHard::Logger & logger =
            *static_cast<const LogHard::Logger *>(flog->facility);
    SharemindDataSourceManager * dataSourceManager = static_cast<SharemindDataSourceManager *>(fsourcem->facility);
    SharemindTdbVectorMapUtil * mapUtil = static_cast<SharemindTdbVectorMapUtil *>(fvmaputil->facility);

    /*
     * Initialize the module handle
     */
    try {
        c->moduleHandle = new sharemind::TdbHdf5Module(logger,
                                                       *dataSourceManager,
                                                       *mapUtil,
                                                       consensusService);
        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_DEINITIALIZER(c) __attribute__ ((visibility("default")));
SHAREMIND_MODULE_API_0x1_DEINITIALIZER(c) {
    assert(c);
    assert(c->moduleHandle);

    try {
        delete static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);
    } catch (...) {
        const SharemindModuleApi0x1Facility * flog = c->getModuleFacility(c, "Logger");
        if (flog && flog->facility) {
            const LogHard::Logger & logger =
                    *static_cast<const LogHard::Logger *>(flog->facility);
            logger.warning() << "Exception was caught during \"mod_tabledb_hdf5\" module deinitialization";
        }
    }

    c->moduleHandle = 0;
}

SHAREMIND_MODULE_API_0x1_SYSCALL_DEFINITIONS(

    /* High level database operations */
      { "tdb_open",               &tdb_open }
    , { "tdb_close",            &tdb_close }

    /* Table database API */
    , { "tdb_tbl_create",       &tdb_tbl_create }
    , { "tdb_tbl_delete",       &tdb_tbl_delete }
    , { "tdb_tbl_exists",       &tdb_tbl_exists }
    , { "tdb_tbl_col_count",    &tdb_tbl_col_count }
    , { "tdb_tbl_col_names",    &tdb_tbl_col_names }
    , { "tdb_tbl_col_types",    &tdb_tbl_col_types }
    , { "tdb_tbl_row_count",    &tdb_tbl_row_count }
    , { "tdb_insert_row",       &tdb_insert_row }
    , { "tdb_read_col",         &tdb_read_col }

    /* Table database statement API */
    , { "tdb_stmt_exec",        &tdb_stmt_exec }

);

} /* extern "C" { */
