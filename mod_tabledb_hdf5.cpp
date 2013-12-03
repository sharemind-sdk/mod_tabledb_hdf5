/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include <cassert>
#include <new>
#include <string>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/libmodapi/api_0x1.h>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/miner/Facilities/datastoreapi.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>
#include "TdbHdf5Connection.h"
#include "TdbHdf5Module.h"


namespace { SHAREMIND_DEFINE_PREFIXED_LOGS("[TdbHdf5Module] "); }

namespace {

using namespace sharemind;

template < size_t NumArgs
         , bool   NeedReturnValue = false
         , size_t NumRefs = 0
         , size_t NumCRefs = 0
         >
struct SyscallArgs {
    static inline bool check (size_t num_args,
                              const SharemindModuleApi0x1Reference* refs,
                              const SharemindModuleApi0x1CReference* crefs,
                              SharemindCodeBlock * returnValue)
    {
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
    if (!SyscallArgs<0u, false, 0u, 1u>::check(num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c || !c->process_internal)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->openConnection(c->process_internal, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_close,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, false, 0u, 1u>::check(num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c || !c->process_internal)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->closeConnection(c->process_internal, dsName))
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_create,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<1u, false, 0u, 3u>::check(num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c || !c->process_internal)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string type(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);

        const uint64_t num = args[0u].uint64[0u];

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * conn = m->getConnection(c->process_internal, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // TODO ...

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_insert_row,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, true, 0u, 1u>::check(num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c || !c->process_internal)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string config(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // TODO ...

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_stmt_exec,
                                 args, num_args, refs, crefs,
                                 returnValue, c)
{
    if (!SyscallArgs<1u, false, 0u, 3u>::check(num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    if (!c || !c->process_internal)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const uint64_t vmapId = args[0].uint64[0];

        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string stmtType(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the parameter map
        TdbVectorMap * pmap = m->getVectorMap(c->process_internal, vmapId);
        if (!pmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Parse the parameters depending on the statement type
        // TODO can we do this more efficiently?
        if (stmtType.compare("tbl_create") == 0) {
            size_t size = 0;

            // Parse the "names" parameter
            TdbString ** names;
            if (pmap->get_string_vector(pmap, "names", &names, &size) != TDB_VECTOR_MAP_OK) {
                LogError(m->logger()) << "Failed to execute \"" << stmtType
                    << "\" statement: Failed to get \"names\" string vector parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            std::vector<TdbString *> namesVec(names, names + size);

            // Parse the "types" parameter
            TdbType ** types;
            if (pmap->get_type_vector(pmap, "types", &types, &size) != TDB_VECTOR_MAP_OK) {
                LogError(m->logger()) << "Failed to execute \"" << stmtType <<
                    "\" statement: Failed to get \"types\" type vector parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            std::vector<TdbType *> typesVec(types, types + size);

            // TODO use the consensus service

            // Get the connection
            TdbHdf5Connection * conn = m->getConnection(c->process_internal, dsName);
            if (!conn)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            if (!conn->tblCreate(tblName, namesVec, typesVec))
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        } else {
            LogError(m->logger()) << "Failed to execute \"" << stmtType
                << "\": Unknown statement type.";
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
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

    const SharemindModuleApi0x1Facility * fstorem = c->getModuleFacility(c, "DataStoreManager");
    if (!fstorem || !fstorem->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    const SharemindModuleApi0x1Facility * fsourcem = c->getModuleFacility(c, "DataSourceManager");
    if (!fsourcem || !fsourcem->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    const SharemindModuleApi0x1Facility * fvmaputil = c->getModuleFacility(c, "TdbVectorMapUtil");
    if (!fvmaputil || !fvmaputil->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    sharemind::ILogger * logger = static_cast<sharemind::ILogger *>(flog->facility);
    DataStoreManager * dataStoreManager = static_cast<DataStoreManager *>(fstorem->facility);
    DataSourceManager * dataSourceManager = static_cast<DataSourceManager *>(fsourcem->facility);
    TdbVectorMapUtil * mapUtil = static_cast<TdbVectorMapUtil *>(fvmaputil->facility);

    /*
     * Initialize the module handle
     */
    try {
        c->moduleHandle = new sharemind::TdbHdf5Module(*logger, *dataStoreManager, *dataSourceManager, *mapUtil);

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
        /// \todo Log exception
    }

    c->moduleHandle = 0;

    return SHAREMIND_MODULE_API_0x1_OK;
}

SHAREMIND_MODULE_API_0x1_SYSCALL_DEFINITIONS(

    /* High level database operations */
    { "tdb_open",           &tdb_open }
    , { "tdb_close",        &tdb_close }

    /* Table database API */
    //, { "tdb_tbl_create",   &tdb_tbl_create }
    /* ... */
    //, { "tdb_insert_row",   &tdb_insert_row }

    /* Table database statement API */
    , { "tdb_stmt_exec",    &tdb_stmt_exec }

);

} /* extern "C" { */
