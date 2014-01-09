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
#include <sstream>
#include <string>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/libmodapi/api_0x1.h>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/miner/Facilities/datastoreapi.h>
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
    if (!SyscallArgs<0u, false, 0u, 1u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->openConnection(c, dsName))
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
    if (!SyscallArgs<0u, false, 0u, 1u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        if (!m->closeConnection(c, dsName))
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
    if (!SyscallArgs<2u, false, 0u, 4u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || crefs[3u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[3u].pData)[crefs[3u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

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

        std::ostringstream oss;
        for (uint64_t i = 0; i < ncols; ++i) {
            oss << i;
            namesVec.push_back(SharemindTdbString_new(oss.str()));
            oss.str(""); oss.clear();
        }

        SharemindTdbType * const type = SharemindTdbType_new(typeDomain, typeName, typeSize);
        const std::vector<SharemindTdbType *> typesVec(ncols, type);

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblCreate, tblName, const_cast<const std::vector<SharemindTdbString *> &>(namesVec), typesVec);
        const bool success = m->executeTransaction(transaction, c);

        // Clean up parameters
        SharemindTdbType_delete(type);
        std::vector<SharemindTdbString *>::iterator it;
        for (it = namesVec.begin(); it != namesVec.end(); ++it)
            SharemindTdbString_delete(*it);
        namesVec.clear();

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_delete,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, false, 0u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblDelete, tblName);
        const bool success = m->executeTransaction(transaction, c);

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_exists,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        bool exists = false;
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblExists, tblName, exists);
        const bool success = m->executeTransaction(transaction, c);

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        returnValue->uint64[0] = exists;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_col_count,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblColCount, tblName, count);
        const bool success = m->executeTransaction(transaction, c);

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        returnValue->uint64[0] = count;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_tbl_row_count,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<0u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Execute the transaction
        uint64_t count = 0;
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblRowCount, tblName, count);
        const bool success = m->executeTransaction(transaction, c);

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        returnValue->uint64[0] = count;

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
    if (!SyscallArgs<1u, false, 0u, 5u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

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

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);
        const std::string typeDomain(static_cast<const char *>(crefs[2u].pData), crefs[2u].size - 1u);
        const std::string typeName(static_cast<const char *>(crefs[3u].pData), crefs[3u].size - 1u);

        const uint64_t typeSize = args[0u].uint64[0u];

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
        SharemindTdbValue * const val = new SharemindTdbValue;
        val->type = SharemindTdbType_new(typeDomain, typeName, typeSize);
        val->buffer = const_cast<void *>(crefs[4u].pData);
        val->size = bufSize;

        const std::vector<SharemindTdbValue *> valuesVec(1, val);
        const std::vector<std::vector<SharemindTdbValue *> > valuesBatch(1, valuesVec);

        // Execute the transaction
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::insertRow, tblName, valuesBatch);
        const bool success = m->executeTransaction(transaction, c);

        // Clean up parameters
        SharemindTdbType_delete(val->type);
        delete val;

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        return SHAREMIND_MODULE_API_0x1_OK;
    } catch (const std::bad_alloc &) {
        return SHAREMIND_MODULE_API_0x1_OUT_OF_MEMORY;
    } catch (...) {
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;
    }
}

SHAREMIND_MODULE_API_0x1_SYSCALL(tdb_read_col,
                          args, num_args, refs, crefs,
                          returnValue, c)
{
    if (!SyscallArgs<1u, true, 0u, 2u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;

    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

    try {
        const std::string dsName(static_cast<const char *>(crefs[0u].pData), crefs[0u].size - 1u);
        const std::string tblName(static_cast<const char *>(crefs[1u].pData), crefs[1u].size - 1u);

        const uint64_t colId = args[0u].uint64[0u];

        sharemind::TdbHdf5Module * m = static_cast<sharemind::TdbHdf5Module *>(c->moduleHandle);

        // Get the connection
        TdbHdf5Connection * const conn = m->getConnection(c, dsName);
        if (!conn)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Construct some parameters
        SharemindTdbIndex * const idx = SharemindTdbIndex_new(colId);
        const std::vector<SharemindTdbIndex *> colIds(1, idx);

        // Execute the transaction
        std::vector<SharemindTdbValue *> valuesVec;
        TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::readColumn, tblName, colIds, valuesVec);
        const bool success = m->executeTransaction(transaction, c);

        SharemindTdbIndex_delete(idx);

        if (!success)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Get the result map
        uint64_t vmapId = 0;
        SharemindTdbVectorMap * const rmap = m->newVectorMap(c, vmapId);
        if (!rmap)
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

        // Make a copy of the value pointers
        SharemindTdbValue ** values = new SharemindTdbValue * [valuesVec.size()];
        std::copy(valuesVec.begin(), valuesVec.end(), values);

        // Set the result "values"
        if (rmap->set_value_vector(rmap, "values", values, valuesVec.size()) != TDB_VECTOR_MAP_OK) {
            m->logger().error() << "Failed to set \"values\" value vector result.";
            delete[] values;
            return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        }

        returnValue->uint64[0] = vmapId;

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
    if (!SyscallArgs<1u, false, 0u, 3u>::check(args, num_args, refs, crefs, returnValue)) {
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;
    }

    if (crefs[0u].size == 0u
            || crefs[1u].size == 0u
            || crefs[2u].size == 0u
            || static_cast<const char *>(crefs[0u].pData)[crefs[0u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[1u].pData)[crefs[1u].size - 1u] != '\0'
            || static_cast<const char *>(crefs[2u].pData)[crefs[2u].size - 1u] != '\0')
        return SHAREMIND_MODULE_API_0x1_INVALID_CALL;


    if (!c)
        return SHAREMIND_MODULE_API_0x1_SHAREMIND_ERROR;

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
            TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::tblCreate, tblName, namesVec, typesVec);
            const bool success = m->executeTransaction(transaction, c);

            if (!success)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        } else if (stmtType.compare("insert_row") == 0) {
            size_t size = 0;

            // Parse the "values" parameter
            SharemindTdbValue ** values;
            if (pmap->get_value_vector(pmap, "values", &values, &size) != TDB_VECTOR_MAP_OK) {
                m->logger().error() << "Failed to execute \"" << stmtType
                    << "\" statement: Failed to get \"values\" value vector parameter.";
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
            }

            const std::vector<SharemindTdbValue *> valuesVec(values, values + size);
            // TODO batched operations
            const std::vector<std::vector<SharemindTdbValue *> > valuesBatch(1, valuesVec);

            // Get the connection
            TdbHdf5Connection * const conn = m->getConnection(c, dsName);
            if (!conn)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;

            // Execute transaction
            TdbHdf5Transaction transaction(*conn, &TdbHdf5Connection::insertRow, tblName, valuesBatch);
            const bool success = m->executeTransaction(transaction, c);

            if (!success)
                return SHAREMIND_MODULE_API_0x1_GENERAL_ERROR;
        } else {
            m->logger().error() << "Failed to execute \"" << stmtType
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

    const SharemindModuleApi0x1Facility * fconsensus = c->getModuleFacility(c, "ConsensusService");
    if (!fconsensus || !fconsensus->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    const SharemindModuleApi0x1Facility * fprocess = c->getModuleFacility(c, "ProcessFacility");
    if (!fprocess || !fprocess->facility)
        return SHAREMIND_MODULE_API_0x1_MISSING_FACILITY;

    sharemind::ILogger * logger = static_cast<sharemind::ILogger *>(flog->facility);
    SharemindDataStoreManager * dataStoreManager = static_cast<SharemindDataStoreManager *>(fstorem->facility);
    SharemindDataSourceManager * dataSourceManager = static_cast<SharemindDataSourceManager *>(fsourcem->facility);
    SharemindTdbVectorMapUtil * mapUtil = static_cast<SharemindTdbVectorMapUtil *>(fvmaputil->facility);
    SharemindConsensusFacility * consensusService =
        static_cast<SharemindConsensusFacility *>(fconsensus->facility);
    SharemindProcessFacility * processFacility =
        static_cast<SharemindProcessFacility *>(fprocess->facility);

    /*
     * Initialize the module handle
     */
    try {
        c->moduleHandle = new sharemind::TdbHdf5Module(*logger, *dataStoreManager, *dataSourceManager, *mapUtil, *consensusService, *processFacility);

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
            sharemind::ILogger * logger = static_cast<sharemind::ILogger *>(flog->facility);
            logger->warning() << "Exception was caught during \"mod_tabledb_hdf5\" module deinitialization";
        }
    }

    c->moduleHandle = 0;

    return SHAREMIND_MODULE_API_0x1_OK;
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
    , { "tdb_tbl_row_count",    &tdb_tbl_row_count }
    , { "tdb_insert_row",       &tdb_insert_row }
    , { "tdb_read_col",         &tdb_read_col }

    /* Table database statement API */
    , { "tdb_stmt_exec",        &tdb_stmt_exec }

);

} /* extern "C" { */
