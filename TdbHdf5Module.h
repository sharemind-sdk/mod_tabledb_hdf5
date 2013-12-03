 /*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MODULE_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MODULE_H

#include <string>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/miner/Facilities/datastoreapi.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>


namespace sharemind  {

class ILogger;
class TdbHdf5Connection;
class TdbHdf5ConnectionConf;
class TdbHdf5Manager;

class __attribute__ ((visibility("internal"))) TdbHdf5Module {
private: /* Types: */

    typedef boost::ptr_map<std::string, TdbHdf5ConnectionConf> ConfMap;

public: /* Methods: */

    TdbHdf5Module(ILogger & logger, DataStoreManager & dataStoreManager, DataSourceManager & dsManager, TdbVectorMapUtil & mapUtil);

    bool openConnection(const void * process, const std::string & dsName);
    bool closeConnection(const void * process, const std::string & dsName);
    TdbHdf5Connection * getConnection(const void * process, const std::string & dsName) const;

    TdbVectorMap * newVectorMap(const void * process);
    bool deleteVectorMap(const void * process, const uint64_t vmapId);
    TdbVectorMap * getVectorMap(const void * process, const uint64_t vmapId) const;

    inline ILogger & logger() { return m_logger; }
    inline const ILogger & logger() const { return m_logger; }

    inline DataStoreManager & dataStoreManager() { return m_dataStoreManager; }
    inline const DataStoreManager & dataStoreManager() const { return m_dataStoreManager; }

    inline TdbVectorMapUtil & vectorMapUtil() { return m_mapUtil; }
    inline const TdbVectorMapUtil & vectorMapUtil() const { return m_mapUtil; }

private: /* Fields: */

    /* Cached references: */
    ILogger & m_logger;
    DataStoreManager & m_dataStoreManager;
    DataSourceManager & m_dataSourceManager;
    TdbVectorMapUtil & m_mapUtil;

    boost::shared_ptr<TdbHdf5Manager> m_dbManager;

    ConfMap m_dsConf;
    boost::mutex m_dsConfMutex;

}; /* class TdbHdf5Module { */

} /* namespace sharemind { */

#endif /* SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MODULE_H */
