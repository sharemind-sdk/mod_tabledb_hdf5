 /*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5Module.h"

#include <boost/version.hpp>
#include <boost/shared_ptr.hpp>
#if BOOST_VERSION >= 105300
#include <boost/thread/lock_guard.hpp>
#else
#include <boost/thread/locks.hpp>
#endif
#include "TdbHdf5ConnectionConf.h"
#include "TdbHdf5Manager.h"


namespace {

template <class T>
void destroy(void * ptr) throw() { delete static_cast<T *>(ptr); }

} /* namespace { */

namespace sharemind {

TdbHdf5Module::TdbHdf5Module(ILogger & logger, SharemindDataStoreManager & dataStoreManager, SharemindDataSourceManager & dataSourceManager, SharemindTdbVectorMapUtil & mapUtil)
    : m_logger(logger.wrap("[TdbHdf5Module] "))
    , m_dataStoreManager(dataStoreManager)
    , m_dataSourceManager(dataSourceManager)
    , m_mapUtil(mapUtil)
    , m_dbManager(new TdbHdf5Manager(logger))
{
    // Intentionally empty
}

bool TdbHdf5Module::openConnection(const void * process, const std::string & dsName) {
    TdbHdf5ConnectionConf * cfg = NULL;

    {
        boost::lock_guard<boost::mutex> lock(m_dsConfMutex);

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

            assert(m_dsConf.insert(const_cast<std::string &>(dsName), cfg).second);
        } else {
            cfg = it->second;
        }
    }

    // Get connection store
    SharemindDataStore * connections = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                               process,
                                                               "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Open the connection
    boost::shared_ptr<TdbHdf5Connection> conn = m_dbManager->openConnection(*cfg);
    if (!conn.get())
        return false;

    return connections->set(connections,
                            dsName.c_str(),
                            new boost::shared_ptr<TdbHdf5Connection>(conn),
                            &destroy<boost::shared_ptr<TdbHdf5Connection> >);
}

bool TdbHdf5Module::closeConnection(const void * process, const std::string & dsName) {
    // Get connection store
    SharemindDataStore * connections = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                               process,
                                                               "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Remove the connection
    connections->remove(connections, dsName.c_str());

    return true;
}

TdbHdf5Connection * TdbHdf5Module::getConnection(const void * process, const std::string & dsName) const {
    // Get connection store
    SharemindDataStore * connections = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                               process,
                                                               "mod_tabledb_hdf5/connections");
    if (!connections) {
        m_logger.error() << "Failed to get process data store.";
        return NULL;
    }

    // Return the connection object
    boost::shared_ptr<TdbHdf5Connection> * conn =
        static_cast<boost::shared_ptr<TdbHdf5Connection> *>(connections->get(connections, dsName.c_str()));
    if (!conn) {
        m_logger.error() << "No open connection for data source \"" << dsName << "\".";
        return NULL;
    }

    return conn->get();
}

SharemindTdbVectorMap * TdbHdf5Module::newVectorMap(const void * process) {
    // Get vector map store
    SharemindDataStore * maps = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                        process,
                                                        "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return NULL;
    }

    // Add new map to the store
    SharemindTdbVectorMap * map = m_mapUtil.new_map(&m_mapUtil, maps);
    if (!map) {
        m_logger.error() << "Failed to create new map object.";
        return NULL;
    }

    return map;
}

bool TdbHdf5Module::deleteVectorMap(const void * process, const uint64_t vmapId) {
    // Get vector map store
    SharemindDataStore * maps = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                        process,
                                                        "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return false;
    }

    // Remove an existing map from the store
    return m_mapUtil.delete_map(&m_mapUtil, maps, vmapId);
}

SharemindTdbVectorMap * TdbHdf5Module::getVectorMap(const void * process, const uint64_t vmapId) const {
    // Get vector map store
    SharemindDataStore * maps = m_dataStoreManager.get_datastore(&m_dataStoreManager,
                                                        process,
                                                        "mod_tabledb/vector_maps");
    if (!maps) {
        m_logger.error() << "Failed to get process data store.";
        return NULL;
    }

    // Return an existing map object
    SharemindTdbVectorMap * map = m_mapUtil.get_map(&m_mapUtil, maps, vmapId);
    if (!map) {
        m_logger.error() << "No map object with given identifier exists.";
        return NULL;
    }

    return map;
}

} // namespace sharemind {
