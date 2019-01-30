/*
 * Copyright (C) 2015-2017 Cybernetica
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

#include "TdbHdf5Module.h"

#include <cstring>
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

bool equivalent(const SharemindConsensusDatum * proposals, size_t count) {
    assert(proposals);
    assert(count > 0u);
    auto const firstData = proposals[0u].data;
    auto const firstSize = proposals[0u].size;
    for (size_t i = 1u; i < count; i++)
        if ((proposals[i].size != firstSize)
            || std::memcmp(firstData, proposals[i].data, firstSize) != 0)
            return false;
    return true;
}

SharemindConsensusResultType execute(const SharemindConsensusDatum * proposals,
                                     size_t count,
                                     void * callbackPtr)
{
    assert(proposals);
    assert(count > 0u);
    assert(callbackPtr);
    (void) proposals;
    (void) count;
    TransactionData & transaction =
            *static_cast<TransactionData *>(callbackPtr);
    transaction.localResult = transaction.strategy.execute();
    return transaction.localResult;
}

void commit(const SharemindConsensusDatum * proposals,
            size_t count,
            const SharemindConsensusResultType * results,
            void * callbackPtr)
{
    assert(proposals);
    (void) proposals;
    assert(count > 0u);
    assert(results);
    assert(callbackPtr);

    // Get the global result from all of the local results
    SharemindTdbError err = SHAREMIND_TDB_OK;
    for (size_t i = 0u; i < count; ++i) {
        if (static_cast<SharemindTdbError>(results[i]) != SHAREMIND_TDB_OK) {
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

    TransactionData & transaction =
            *static_cast<TransactionData *>(callbackPtr);
    transaction.globalResult = err;

    // If the operation succeeded locally but not on all miners
    if (transaction.localResult == SHAREMIND_TDB_OK
            && transaction.globalResult != SHAREMIND_TDB_OK)
        transaction.strategy.rollback();
}

SharemindOperationType const databaseOperation = {
    &equivalent,
    &execute,
    &commit,
    "TdbHDF5Transaction"
};

template <typename Logger>
SharemindDataStore * getDataStore(
        SharemindModuleApi0x1SyscallContext const * const ctx,
        char const * const name,
        Logger & logger) noexcept
{
    SharemindDataStoreFactory * const f =
            static_cast<SharemindDataStoreFactory *>(
                ctx->processFacility(ctx, "DataStoreFactory"));
    if (!f) {
        logger.error() << "Failed to get process data store factory!";
        return nullptr;
    }
    if (SharemindDataStore * const store = f->get_datastore(f, name))
        return store;
    logger.error() << "Failed to get process data store: " << name << '!';
    return nullptr;
}

template <typename Logger>
SharemindDataStore * getConnections(
        SharemindModuleApi0x1SyscallContext const * const ctx,
        Logger & logger) noexcept
{ return getDataStore(ctx, "mod_tabledb_hdf5/connections", logger); }

template <typename Logger>
SharemindDataStore * getVectorMaps(
        SharemindModuleApi0x1SyscallContext const * const ctx,
        Logger & logger) noexcept
{ return getDataStore(ctx, "mod_tabledb/vector_maps", logger); }

} /* namespace { */

TdbHdf5Module::TdbHdf5Module(const LogHard::Logger & logger,
                             SharemindDataSourceManager & dataSourceManager,
                             SharemindTdbVectorMapUtil & mapUtil,
                             SharemindConsensusFacility * consensusService)
    : m_logger(logger, "[TdbHdf5Module]")
    , m_dataSourceManager(dataSourceManager)
    , m_mapUtil(mapUtil)
    , m_consensusService(consensusService)
    , m_dbManager(logger)
{
    if (m_consensusService)
        m_consensusService->add_operation_type(m_consensusService, &databaseOperation);
}

bool TdbHdf5Module::setErrorCode(const SharemindModuleApi0x1SyscallContext * ctx,
        const std::string & dsName,
        SharemindTdbError code)
{
    // Get error store
    SharemindDataStore * const errors =
            getDataStore(ctx, "mod_tabledb/errors", m_logger);
    if (!errors)
        return false;

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
    // Get connection store
    SharemindDataStore * const connections = getConnections(ctx, m_logger);
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Check if we already have the connection
    if (static_cast<std::shared_ptr<TdbHdf5Connection> *>(
                connections->get(connections, dsName.c_str())))
    {
        return true;
    }

    TdbHdf5ConnectionConf * cfg = nullptr;

    {
        std::lock_guard<std::mutex> lock(m_dsConfMutex);

        // Get configuration from file or load a cached configuration
        auto const it(m_dsConf.find(dsName));
        if (it == m_dsConf.cend()) {
            SharemindDataSource * src = m_dataSourceManager.get_source(&m_dataSourceManager, dsName.c_str());
            if (!src) {
                m_logger.error() << "Failed to get configuration for data source \"" << dsName << "\".";
                return false;
            }

            std::unique_ptr<TdbHdf5ConnectionConf> configuration;
            try {
                configuration = std::make_unique<TdbHdf5ConnectionConf>(
                                    src->conf(src));
            } catch (...) {
                auto const loggerLock(m_logger.retrieveBackendLock());
                m_logger.error()
                        << "Failed to parse configuration for data source \""
                        << dsName << "\":";
                m_logger.printCurrentException();
                return false;
            }

            cfg = configuration.get();
            auto const rv(m_dsConf.emplace(dsName, std::move(configuration)));
            assert(rv.second);
        } else {
            cfg = it->second.get();
        }
    }

    // Open the connection
    std::shared_ptr<TdbHdf5Connection> conn = m_dbManager.openConnection(*cfg);
    if (!conn.get())
        return false;

    // Store the connection
    std::shared_ptr<TdbHdf5Connection> * connPtr = new std::shared_ptr<TdbHdf5Connection>(conn);
    if (!connections->set(connections,
                          dsName.c_str(),
                          connPtr,
                          &destroy<std::shared_ptr<TdbHdf5Connection> >))
    {
        delete connPtr;
        return false;
    }

    return true;
}

bool TdbHdf5Module::closeConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                    const std::string & dsName)
{
    // Get connection store
    SharemindDataStore * const connections = getConnections(ctx, m_logger);
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
    SharemindDataStore * const connections = getConnections(ctx, m_logger);
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
    SharemindDataStore * const maps = getVectorMaps(ctx, m_logger);
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
    SharemindDataStore * const maps = getVectorMaps(ctx, m_logger);
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
    SharemindDataStore * const maps = getVectorMaps(ctx, m_logger);
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

SharemindTdbError TdbHdf5Module::executeTransaction(
        TdbHdf5Transaction & strategy,
        const SharemindModuleApi0x1SyscallContext * c)
{
    assert(c);
    if (m_consensusService) {
        auto * const f = c->processFacility(c, "ProcessFacility");
        if (!f)
            return SHAREMIND_TDB_MISSING_FACILITY;
        using CPF = SharemindProcessFacility;
        CPF const & pf = *static_cast<CPF *>(f);

        // Local transactions will always succeed:
        auto const guidData = pf.globalId(&pf);
        if (guidData) {
            TransactionData transaction(strategy);
            auto const guidSize = pf.globalIdSize(&pf);
            assert(guidSize > 0u);

            /** \bug This transaction may actually be run on a subset of servers
                     participating in the consensus service, but we currently
                     require ALL of the participating parties to agree on the
                     transaction, which will fail in the strict subset case. */
            SharemindConsensusFacilityError ret =
                m_consensusService->blocking_propose(m_consensusService,
                                                    "TdbHDF5Transaction",
                                                    guidSize,
                                                    guidData,
                                                    &transaction);
            if (ret == SHAREMIND_CONSENSUS_FACILITY_OK) {
                return transaction.globalResult;
            } else if (ret == SHAREMIND_CONSENSUS_FACILITY_OUT_OF_MEMORY) {
                throw std::bad_alloc();
            } else {
                throw std::runtime_error("Unknown ConsensusService exception.");
            }
        }
    }
    return strategy.execute();
}

} // namespace sharemind {
