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

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H

#include <boost/filesystem/path.hpp>
#include <H5Rpublic.h>
#include <H5Ipublic.h>
#include <exception>
#include <LogHard/Logger.h>
#include <map>
#include <sharemind/Exception.h>
#include <sharemind/mod_tabledb/tdberror.h>
#include <sharemind/mod_tabledb/tdbtypes.h>
#include <string>
#include <vector>
#include "TdbHdf5ConnectionConf.h"


namespace sharemind {

class __attribute__ ((visibility("internal"))) TdbHdf5Connection {

public: /* Types: */

    SHAREMIND_DEFINE_EXCEPTION(std::exception, Exception);
    SHAREMIND_DEFINE_EXCEPTION(Exception, InitializationException);
    SHAREMIND_DEFINE_EXCEPTION_CONST_MSG(InitializationException,
                                         FailedToSetHdf5LoggingHandlerException,
                                         "Failed to set HDF5 logging handler.");

    typedef uint64_t size_type;

private: /* Types: */

    struct ColumnIndex {
        const char *    name;
        hobj_ref_t      dataset_ref;
        hsize_t         dataset_column;
    };

    typedef std::map<std::string, hid_t> TableFileMap;

public: /* Methods: */

    TdbHdf5Connection(const LogHard::Logger & logger,
                      const boost::filesystem::path & path);
    ~TdbHdf5Connection();

    /*
     * General database functions
     */
    SharemindTdbError tblNames(std::vector<SharemindTdbString *> & names);

    /*
     * General database table functions
     */

    SharemindTdbError tblCreate(const std::string & tbl,
            const std::vector<SharemindTdbString *> & names,
            const std::vector<SharemindTdbType *> & types);
    SharemindTdbError tblDelete(const std::string & tbl);
    SharemindTdbError tblExists(const std::string & tbl, bool & status);

    SharemindTdbError tblColCount(const std::string & tbl, size_type & count);
    SharemindTdbError tblColNames(const std::string & tbl,
            std::vector<SharemindTdbString *> & names);
    SharemindTdbError tblColTypes(const std::string & tbl,
            std::vector<SharemindTdbType *> & types);
    SharemindTdbError tblRowCount(const std::string & tbl, size_type & count);

    /*
     * Table data manipulation functions
     */

    SharemindTdbError insertRow(const std::string & tbl,
            const std::vector<std::vector<SharemindTdbValue *> > & valuesBatch,
            const std::vector<bool> & valuesAsColumnBatch);

    SharemindTdbError readColumn(const std::string & tbl,
            const std::vector<SharemindTdbString *> & colIdBatch,
            std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);
    SharemindTdbError readColumn(const std::string & tbl,
            const std::vector<SharemindTdbIndex *> & colIdBatch,
            std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);

private: /* Methods: */

    /*
     * Filesystem operations
     */

    boost::filesystem::path nameToPath(const std::string & tbl);
    bool pathRemove(const boost::filesystem::path & path);
    bool pathExists(const boost::filesystem::path & path, bool & status);

    /*
     * Parameter validation
     */

    bool validateColumnNames(const std::vector<SharemindTdbString *> & names) const;
    bool validateTableName(const std::string & tbl) const;
    bool validateValues(const std::vector<SharemindTdbValue *> & values) const;

    /*
     * Database operations
     */

    SharemindTdbError readColumn(const hid_t fileId,
            const std::vector<SharemindTdbIndex *> & colNrBatch,
            std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);
    SharemindTdbError readDatasetColumn(const hid_t fileId, const hobj_ref_t ref,
            const std::vector<std::pair<hsize_t, std::vector<SharemindTdbValue *> *> > & paramBatch);

    SharemindTdbError objRefToType(const hid_t fileId, const hobj_ref_t ref, hid_t & aId, SharemindTdbType & type);

    SharemindTdbError getColumnCount(const hid_t fileId, hsize_t & ncols);
    SharemindTdbError getRowCount(const hid_t fileId, hsize_t & nrows);
    SharemindTdbError setRowCount(const hid_t fileId, const hsize_t nrows);

    bool closeTableFile(const std::string & tbl);
    hid_t openTableFile(const std::string & tbl);

private: /* Fields: */

    const LogHard::Logger m_logger;

    boost::filesystem::path m_path;

    TableFileMap m_tableFiles;

}; /* class TdbHdf5Connection { */

} /* namespace sharemind { */

#endif // SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H
