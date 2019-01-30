/*
 * Copyright (C) 2015 Cybernetica
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

#include "TdbHdf5Manager.h"

#include "TdbHdf5Connection.h"
#include "TdbHdf5ConnectionConf.h"

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <memory>


namespace fs = boost::filesystem;

namespace sharemind {

std::shared_ptr<TdbHdf5Connection> TdbHdf5Manager::openConnection(const TdbHdf5ConnectionConf & config) {
    // Get the canonical path for the connection
    // TODO workaround for older boost filesystem versions?
    fs::path canonicalPath;
    try {
        const fs::path dbPath(config.databasePath());

        // Check if path exists
        if (fs::exists(dbPath)) {
            // Get the canonical path for the database (e.g. with no dots or symlinks)
            canonicalPath = fs::canonical(fs::path(config.databasePath()));

            // Check if the given path is a directory
            if (!fs::is_directory(canonicalPath)) {
                m_logger.error() << "Database path " << dbPath.string()
                    << " exists, but is not a directory.";
                return std::shared_ptr<TdbHdf5Connection>();
            }
        } else {
            // Create the path to the data source
            m_logger.fullDebug() << "Database path does not exist. Creating path "
                                 << dbPath.string() << ".";

            if (!create_directories(dbPath)) {
                m_logger.error() << "Failed to create path " << dbPath.string()
                                 << '.';
                return std::shared_ptr<TdbHdf5Connection>();
            }

            // Get the canonical path for the database (e.g. with no dots or symlinks)
            canonicalPath = fs::canonical(fs::path(config.databasePath()));
        }
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while while performing file system"
            << " operations: " << e.what();
        return std::shared_ptr<TdbHdf5Connection>();
    }

    // Return the connection object from the cache or construct a new one
    return m_connectionCache.get(
                canonicalPath,
                [this](boost::filesystem::path const & key)
                { return new TdbHdf5Connection(m_previousLogger, key); });
}

} /* namespace sharemind { */
