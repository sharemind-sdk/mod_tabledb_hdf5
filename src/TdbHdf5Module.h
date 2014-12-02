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

#include <functional>
#include <LogHard/Logger.h>
#include <memory>
#include <mutex>
#include <sharemind/dbcommon/datasourceapi.h>
#include <sharemind/libmodapi/api_0x1.h>
#include <sharemind/miner/datastoreapi.h>
#include <sharemind/miner/libconsensusservice.h>
#include <sharemind/miner/libprocessfacility.h>
#include <sharemind/mod_tabledb/tdberror.h>
#include <sharemind/mod_tabledb/tdbvectormapapi.h>
#include <sharemind/ScopedObjectMap.h>
#include <string>
#include "TdbHdf5ConnectionConf.h"


namespace sharemind  {

class TdbHdf5Connection;
class TdbHdf5Manager;

class TdbHdf5Transaction {

public: /* Methods: */

    template <typename F, typename ... Args>
    TdbHdf5Transaction(TdbHdf5Connection & connection,
                       F&& exec,
                       Args && ... args)
        : m_exec(std::bind(std::forward<F>(exec),
                           std::ref(connection),
                           std::forward<Args>(args) ...))
    { }

    SharemindTdbError execute() {
        return m_exec();
    }

    void rollback() {
        // TODO
    }

private: /* Fields: */

    std::function<SharemindTdbError ()> m_exec;
};

class __attribute__ ((visibility("internal"))) TdbHdf5Module {
private: /* Types: */

    typedef ScopedObjectMap<std::string, TdbHdf5ConnectionConf> ConfMap;

public: /* Methods: */

    TdbHdf5Module(const LogHard::Logger & logger,
                  SharemindDataStoreManager & dataStoreManager,
                  SharemindDataSourceManager & dsManager,
                  SharemindTdbVectorMapUtil & mapUtil,
                  SharemindConsensusFacility & consensusService);

    bool setErrorCode(const SharemindModuleApi0x1SyscallContext * ctx,
            const std::string & dsName,
            SharemindTdbError code);

    bool openConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                        const std::string & dsName);
    bool closeConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                         const std::string & dsName);
    TdbHdf5Connection * getConnection(const SharemindModuleApi0x1SyscallContext * ctx,
                                      const std::string & dsName) const;

    SharemindTdbVectorMap * newVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                         uint64_t & vmapId);
    bool deleteVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                         const uint64_t vmapId);
    SharemindTdbVectorMap * getVectorMap(const SharemindModuleApi0x1SyscallContext * ctx,
                                         const uint64_t vmapId) const;

    SharemindTdbError executeTransaction(
                TdbHdf5Transaction & strategy,
                const SharemindModuleApi0x1SyscallContext * context);

    inline const LogHard::Logger & logger() const noexcept { return m_logger; }

    inline SharemindDataStoreManager & dataStoreManager() { return m_dataStoreManager; }
    inline const SharemindDataStoreManager & dataStoreManager() const { return m_dataStoreManager; }

    inline SharemindTdbVectorMapUtil & vectorMapUtil() { return m_mapUtil; }
    inline const SharemindTdbVectorMapUtil & vectorMapUtil() const { return m_mapUtil; }

private: /* Fields: */

    const LogHard::Logger m_logger;

    /* Cached references: */
    SharemindDataStoreManager & m_dataStoreManager;
    SharemindDataSourceManager & m_dataSourceManager;
    SharemindTdbVectorMapUtil & m_mapUtil;
    SharemindConsensusFacility & m_consensusService;

    std::shared_ptr<TdbHdf5Manager> m_dbManager;

    ConfMap m_dsConf;
    std::mutex m_dsConfMutex;

}; /* class TdbHdf5Module { */

} /* namespace sharemind { */

#endif /* SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MODULE_H */
