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

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H

#include <boost/filesystem/path.hpp>
#include <LogHard/Logger.h>
#include <memory>
#include "KeyValueCache.h"


namespace sharemind {

class TdbHdf5Connection;
class TdbHdf5ConnectionConf;

class __attribute__ ((visibility("internal"))) TdbHdf5Manager {

public: /* Methods: */

    TdbHdf5Manager(LogHard::Logger logger);

    TdbHdf5Manager(TdbHdf5Manager &&) noexcept;
    TdbHdf5Manager(TdbHdf5Manager const &) = delete;

    ~TdbHdf5Manager() noexcept;

    TdbHdf5Manager & operator=(TdbHdf5Manager &&) noexcept = delete;
    TdbHdf5Manager & operator=(TdbHdf5Manager const &) = delete;

    std::shared_ptr<TdbHdf5Connection> openConnection(
                TdbHdf5ConnectionConf const & config);

private: /* Fields: */

    LogHard::Logger const m_previousLogger;
    LogHard::Logger const m_logger;
    KeyValueCache<boost::filesystem::path, TdbHdf5Connection> m_connectionCache;

}; /* class TdbHdf5Manager { */

} /* namespace sharemind { */

#endif // SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5MANAGER_H
