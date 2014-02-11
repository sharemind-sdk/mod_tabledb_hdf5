/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5Manager.h"

#include <boost/filesystem.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/system/error_code.hpp>
#include "TdbHdf5Connection.h"


namespace fs = boost::filesystem;

namespace sharemind {

TdbHdf5Manager::TdbHdf5Manager(ILogger & logger)
    : m_logger(logger.wrap("[TdbHdf5Manager] "))
    , m_loggerRef(logger)
{
    // Intentionally empty
}

boost::shared_ptr<TdbHdf5Connection> TdbHdf5Manager::openConnection(const TdbHdf5ConnectionConf & config) {
    // Get the canonical path for the connection
    // TODO workaround for older boost filesystem versions?
    fs::path canonicalPath;
    try {
        const fs::path dbPath(config.getPath());

        // Check if path exists
        if (fs::exists(dbPath)) {
            // Get the canonical path for the database (e.g. with no dots or symlinks)
            canonicalPath = fs::canonical(fs::path(config.getPath()));

            // Check if the given path is a directory
            if (!fs::is_directory(canonicalPath)) {
                m_logger.error() << "Database path " << dbPath
                    << " exists, but is not a directory.";
                return boost::shared_ptr<TdbHdf5Connection>();
            }
        } else {
            // Create the path to the data source
            m_logger.fullDebug() << "Database path does not exist. Creating path "
                << dbPath << ".";

            if (!create_directories(dbPath)) {
                m_logger.error() << "Failed to create path " << dbPath << ".";
                return boost::shared_ptr<TdbHdf5Connection>();
            }

            // Get the canonical path for the database (e.g. with no dots or symlinks)
            canonicalPath = fs::canonical(fs::path(config.getPath()));
        }
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while while performing file system"
            << " operations: " << e.what();
        return boost::shared_ptr<TdbHdf5Connection>();
    }

    // Return the connection object from the cache or construct a new one
    return get(canonicalPath);
}

TdbHdf5Connection * TdbHdf5Manager::alloc(const boost::filesystem::path & key) const {
    try {
        return new TdbHdf5Connection(m_loggerRef, key);
    } catch (...) {
        return nullptr;
    }
}

} /* namespace sharemind { */
