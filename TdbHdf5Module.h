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
#include <sharemind/common/Logger/ILogger.h>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/libmodapi/api_0x1.h>
#include <sharemind/miner/Facilities/datastoreapi.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>


namespace sharemind  {

class TdbHdf5Connection;
class TdbHdf5ConnectionConf;
class TdbHdf5Manager;

class __attribute__ ((visibility("internal"))) TdbHdf5Module {
private: /* Types: */

    typedef boost::ptr_map<std::string, TdbHdf5ConnectionConf> ConfMap;

public: /* Methods: */

    TdbHdf5Module(ILogger & logger, SharemindDataStoreManager & dataStoreManager, SharemindDataSourceManager & dsManager, SharemindTdbVectorMapUtil & mapUtil);

    bool openConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                        const std::string & dsName);
    bool closeConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                         const std::string & dsName);
    TdbHdf5Connection * getConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                      const std::string & dsName) const;

    SharemindTdbVectorMap * newVectorMap(const SharemindModuleApi0x1SyscallContext * ctx);
    bool deleteVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                         const uint64_t vmapId);
    SharemindTdbVectorMap * getVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                         const uint64_t vmapId) const;

    inline ILogger::Wrapped & logger() { return m_logger; }
    inline const ILogger::Wrapped & logger() const { return m_logger; }

    inline SharemindDataStoreManager & dataStoreManager() { return m_dataStoreManager; }
    inline const SharemindDataStoreManager & dataStoreManager() const { return m_dataStoreManager; }

    inline SharemindTdbVectorMapUtil & vectorMapUtil() { return m_mapUtil; }
    inline const SharemindTdbVectorMapUtil & vectorMapUtil() const { return m_mapUtil; }

private: /* Fields: */

    mutable ILogger::Wrapped m_logger;

    /* Cached references: */
    SharemindDataStoreManager & m_dataStoreManager;
    SharemindDataSourceManager & m_dataSourceManager;
    SharemindTdbVectorMapUtil & m_mapUtil;

    boost::shared_ptr<TdbHdf5Manager> m_dbManager;

    ConfMap m_dsConf;
    boost::mutex m_dsConfMutex;

}; /* class TdbHdf5Module { */

} /* namespace sharemind { */

#endif /* SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MODULE_H */
