 /*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5Module.h"

#include "TdbHdf5ConnectionConf.h"
#include "TdbHdf5Manager.h"


namespace {

template <class T>
void destroy(void * ptr) noexcept { delete static_cast<T *>(ptr); }

} /* namespace { */

namespace sharemind {

namespace {

struct TransactionData {

    TransactionData(TdbHdf5Transaction & strategy_)
        : strategy(strategy_)
        , localResult(SHAREMIND_TDB_UNKNOWN_ERROR)
        , globalResult(SHAREMIND_TDB_UNKNOWN_ERROR)
    {}

    TdbHdf5Transaction & strategy;
    SharemindTdbError localResult;
    SharemindTdbError globalResult;
};

uint16_t toId(const SharemindDatum * datum) {
    return *static_cast<uint16_t *>(datum->data);
}

bool equivalent(const SharemindDatum * proposals, size_t count) {
    uint16_t a = toId(&proposals[0u]);

    for (size_t i = 1u; i < count; i++) {
        if (a != toId(&proposals[i]))
            return false;
    }

    return true;
}

SharemindConsensusResultType execute(const SharemindDatum * proposals,
                                     size_t count,
                                     void * callbackPtr)
{
    (void) proposals;
    (void) count;

    TransactionData * transaction = static_cast<TransactionData *>(callbackPtr);
    transaction->localResult = transaction->strategy.execute();

    return transaction->localResult;
}

void commit(const SharemindDatum * proposals,
            size_t count,
            const SharemindConsensusResultType * results,
            void * callbackPtr)
{
    (void) proposals;

    TransactionData * transaction = static_cast<TransactionData *>(callbackPtr);

    // Get the global result from all of the local results
    SharemindTdbError err = SHAREMIND_TDB_OK;
    for (size_t i = 0; i < count; ++i) {
        if (results[i] != SHAREMIND_TDB_OK) {
            if (err == SHAREMIND_TDB_OK) {
                // TODO Currently, we do not check if what we get is a valid
                // error code.
                err = static_cast<SharemindTdbError>(results[i]);
            } else if (err != results[i]) {
                err = SHAREMIND_TDB_CONSENSUS_ERROR;
                break;
            }
        }
    }

    transaction->globalResult = err;

    // If the operation succeeded locally but not on all miners
    if (transaction->localResult == SHAREMIND_TDB_OK &&
            transaction->globalResult != SHAREMIND_TDB_OK)
    {
        transaction->strategy.rollback();
    }
}

SharemindOperationType databaseOperation = {
    &equivalent,
    &execute,
    &commit,
    "TdbHDF5Transaction"
};

} /* namespace { */

TdbHdf5Module::TdbHdf5Module(ILogger & logger,
                             SharemindDataStoreManager & dataStoreManager,
                             SharemindDataSourceManager & dataSourceManager,
                             SharemindTdbVectorMapUtil & mapUtil,
                             SharemindConsensusFacility & consensusService,
                             SharemindProcessFacility & processFacility)
    : m_logger(logger.wrap("[TdbHdf5Module] "))
    , m_dataStoreManager(dataStoreManager)
    , m_dataSourceManager(dataSourceManager)
    , m_mapUtil(mapUtil)
    , m_consensusService(consensusService)
    , m_processFacility(processFacility)
    , m_dbManager(new TdbHdf5Manager(logger))
{
    m_consensusService.add_operation_type(&m_consensusService, &databaseOperation);
}

bool TdbHdf5Module::setErrorCode(const SharemindModuleApi0x1SyscallContext * ctx,
        const std::string & dsName,
        SharemindTdbError code)
{
    // Get error store
    SharemindDataStore * const errors = m_dataStoreManager.get_datastore(
                                                 &m_dataStoreManager,
                                                 ctx,
                                                 "mod_tabledb/errors");
    if (!errors) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Remove existing error code, if any
    errors->remove(errors, dsName.c_str());

    // Set the new error code
    if (!errors->set(errors,
                     dsName.c_str(),
                     new SharemindTdbError(code),
                     &destroy<SharemindTdbError>))
    {
        m_logger.error() << "Failed to set error code.";
        return false;
    }

    return true;
}

bool TdbHdf5Module::openConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                   const std::string & dsName)
{
    TdbHdf5ConnectionConf * cfg = nullptr;

    {
        std::lock_guard<std::mutex> lock(m_dsConfMutex);

        // Get configuration from file or load a cached configuration
        ConfMap::iterator it = m_dsConf.find(dsName);
        if (it == m_dsConf.end()) {
            SharemindDataSource * src = m_dataSourceManager.get_source(&m_dataSourceManager, dsName.c_str());
            if (!src) {
                m_logger.error() << "Failed to get configuration for data source \"" << dsName << "\".";
                return false;
            }

            cfg = new TdbHdf5ConnectionConf;
            if (!cfg->load(src->conf(src))) {
                m_logger.error() << "Failed to parse configuration for data source \"" << dsName << "\": "
                    << cfg->getLastErrorMessage();
                delete cfg;
                return false;
            }

            #ifndef NDEBUG
            const bool r =
            #endif
                    m_dsConf.insert(dsName, cfg).second;
            assert(r);
        } else {
            cfg = it->second;
        }
    }

    // Get connection store
    SharemindDataStore * const connections = m_dataStoreManager.get_datastore(
                                                 &m_dataStoreManager,
                                                 ctx,
                                                 "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Open the connection
    std::shared_ptr<TdbHdf5Connection> conn = m_dbManager->openConnection(*cfg);
    if (!conn.get())
        return false;

    return connections->set(connections,
                            dsName.c_str(),
                            new std::shared_ptr<TdbHdf5Connection>(conn),
                            &destroy<std::shared_ptr<TdbHdf5Connection> >);
}

bool TdbHdf5Module::closeConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                    const std::string & dsName)
{
    // Get connection store
    SharemindDataStore * const connections = m_dataStoreManager.get_datastore(
                                                 &m_dataStoreManager,
                                                 ctx,
                                                 "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Remove the connection
    connections->remove(connections, dsName.c_str());

    return true;
}

TdbHdf5Connection * TdbHdf5Module::getConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                                 const std::string & dsName) const
{
    // Get connection store
    SharemindDataStore * const connections = m_dataStoreManager.get_datastore(
                                                 &m_dataStoreManager,
                                                 ctx,
                                                 "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return nullptr;
    }

    // Return the connection object
    std::shared_ptr<TdbHdf5Connection> * const conn =
        static_cast<std::shared_ptr<TdbHdf5Connection> *>(connections->get(connections, dsName.c_str()));
    if (!conn) {
        m_logger.error() << "No open connection for data source \"" << dsName << "\".";
        return nullptr;
    }

    return conn->get();
}

SharemindTdbVectorMap * TdbHdf5Module::newVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                                    uint64_t & vmapId) {
    // Get vector map store
    SharemindDataStore * const maps = m_dataStoreManager.get_datastore(
                                          &m_dataStoreManager,
                                          ctx,
                                          "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return nullptr;
    }

    // Add new map to the store
    SharemindTdbVectorMap * const map = m_mapUtil.new_map(&m_mapUtil, maps);
    if (!map) {
        m_logger.error() << "Failed to create new map object.";
        return nullptr;
    }

    // Get map identifier
    vmapId = map->get_id(map);

    return map;
}

bool TdbHdf5Module::deleteVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                    const uint64_t vmapId)
{
    // Get vector map store
    SharemindDataStore * const maps = m_dataStoreManager.get_datastore(
                                          &m_dataStoreManager,
                                          ctx,
                                          "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Remove an existing map from the store
    return m_mapUtil.delete_map(&m_mapUtil, maps, vmapId);
}

SharemindTdbVectorMap * TdbHdf5Module::getVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                                    const uint64_t vmapId) const
{
    // Get vector map store
    SharemindDataStore * const maps = m_dataStoreManager.get_datastore(
                                          &m_dataStoreManager,
                                          ctx,
                                          "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return nullptr;
    }

    // Return an existing map object
    SharemindTdbVectorMap * const map = m_mapUtil.get_map(&m_mapUtil, maps, vmapId);
    if (!map) {
        m_logger.error() << "No map object with given identifier exists.";
        return nullptr;
    }

    return map;
}

SharemindTdbError TdbHdf5Module::executeTransaction(TdbHdf5Transaction & strategy,
        const SharemindModuleApi0x1SyscallContext * context)
{
    TransactionData transaction(strategy);
    uint16_t processId = m_processFacility.get_process_id(context);

    SharemindConsensusFacilityError ret =
        m_consensusService.blocking_propose(&m_consensusService,
                                            "TdbHDF5Transaction",
                                            sizeof(uint16_t),
                                            &processId,
                                            &transaction);

    if (ret == SHAREMIND_CONSENSUS_FACILITY_OK) {
        return transaction.globalResult;
    } else if (ret == SHAREMIND_CONSENSUS_FACILITY_OUT_OF_MEMORY) {
        throw std::bad_alloc();
    } else {
        throw std::runtime_error("Unknown ConsensusService exception.");
    }
}

} // namespace sharemind {
