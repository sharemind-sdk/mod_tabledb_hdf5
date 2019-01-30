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

#include <boost/filesystem/operations.hpp>
#include "TdbHdf5Connection.h"
#include "TdbHdf5ConnectionConf.h"


namespace fs = boost::filesystem;

namespace sharemind {

TdbHdf5Manager::TdbHdf5Manager(LogHard::Logger logger)
    : m_previousLogger(std::move(logger))
    , m_logger(m_previousLogger, "[TdbHdf5Manager]")
{}

TdbHdf5Manager::TdbHdf5Manager(TdbHdf5Manager &&) noexcept = default;

TdbHdf5Manager::~TdbHdf5Manager() noexcept = default;

std::shared_ptr<TdbHdf5Connection> TdbHdf5Manager::openConnection(
        TdbHdf5ConnectionConf const & config)
{
    try {
        fs::path dbPath(config.databasePath());

        // Check if path exists
        if (fs::exists(dbPath)) {
            // Check if the given path is a directory:
            if (!fs::is_directory(dbPath)) {
                m_logger.error() << "Database path " << dbPath.string()
                    << " exists, but is not a directory!";
                return std::shared_ptr<TdbHdf5Connection>();
            }
        } else {
            // Create the path to the data source
            m_logger.fullDebug()
                    << "Database path does not exist. Creating path "
                    << dbPath.string() << '.';

            if (!fs::create_directories(dbPath)) {
                m_logger.error()
                        << "Failed to create path " << dbPath.string() << '.';
                return std::shared_ptr<TdbHdf5Connection>();
            }
        }

        // Return the connection object from the cache or construct a new one
        return m_connectionCache.get(
                    fs::canonical(std::move(dbPath)),
                    [this](boost::filesystem::path const & key)
                    { return new TdbHdf5Connection(m_previousLogger, key); });
    } catch (fs::filesystem_error const &) {
        {
            auto const logLock(m_logger.retrieveBackendLock());
            m_logger.error()
                    << "Error while while performing file system operations:";
            m_logger.printCurrentException();
        }
        return std::shared_ptr<TdbHdf5Connection>();
    }
}

} /* namespace sharemind { */
