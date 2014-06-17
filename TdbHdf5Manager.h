/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <boost/filesystem/path.hpp>
#include <boost/property_tree/ptree.hpp>
#include <sharemind/common/KeyValueCache.h>
#include <sharemind/common/Logger/ILogger.h>


namespace sharemind {

class TdbHdf5Connection;
class TdbHdf5ConnectionConf;

class __attribute__ ((visibility("internal"))) TdbHdf5Manager
    : public KeyValueCache<boost::filesystem::path, TdbHdf5Connection> {
public: /* Methods: */

    TdbHdf5Manager(ILogger & logger);

    std::shared_ptr<TdbHdf5Connection> openConnection(const TdbHdf5ConnectionConf & config);

private: /* Methods: */

    TdbHdf5Connection * alloc(const boost::filesystem::path & key) const;

private: /* Fields: */

    mutable ILogger::Wrapped m_logger;

    ILogger & m_loggerRef;

}; /* class TdbHdf5Manager { */

} /* namespace sharemind { */

#endif // SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H
