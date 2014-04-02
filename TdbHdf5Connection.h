/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H

#include <boost/filesystem/path.hpp>
#include <H5Rpublic.h>
#include <H5Ipublic.h>
#include <map>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/common/Logger/ILogger.h>
#include <sharemind/mod_tabledb/tdbtypes.h>
#include <string>
#include <vector>
#include "TdbHdf5ConnectionConf.h"


namespace sharemind {

class __attribute__ ((visibility("internal"))) TdbHdf5Connection {

public: /* Types: */

    class Exception: public std::runtime_error {

    public: /* Methods: */

        inline Exception(const std::string & msg)
            : std::runtime_error(msg) {}

    };

    class ConfigurationException: public Exception {

    public: /* Methods: */

        inline ConfigurationException(const std::string & msg)
            : Exception(msg) {}

    };

    class InitializationException: public Exception {

    public: /* Methods: */

        inline InitializationException(const std::string & msg)
            : Exception(msg) {}

    };

    typedef uint64_t size_type;

private: /* Types: */

    struct ColumnIndex {
        const char *    name;
        hobj_ref_t      dataset_ref;
        hsize_t         dataset_column;
    };

    typedef std::map<std::string, hid_t> TableFileMap;

public: /* Methods: */

    TdbHdf5Connection(ILogger & logger, const boost::filesystem::path & path);
    ~TdbHdf5Connection();

    /*
     * General database table functions
     */

    bool tblCreate(const std::string & tbl, const std::vector<SharemindTdbString *> & names, const std::vector<SharemindTdbType *> & types);
    bool tblDelete(const std::string & tbl);
    bool tblExists(const std::string & tbl, bool & status);

    bool tblColCount(const std::string & tbl, size_type & count);
    bool tblColNames(const std::string & tbl, std::vector<SharemindTdbString *> & names);
    bool tblColTypes(const std::string & tbl, std::vector<SharemindTdbType *> & types);
    bool tblRowCount(const std::string & tbl, size_type & count);

    /*
     * Table data manipulation functions
     */

    bool insertRow(const std::string & tbl, const std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);

    bool readColumn(const std::string & tbl, const std::vector<SharemindTdbString *> & colIdBatch, std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);
    bool readColumn(const std::string & tbl, const std::vector<SharemindTdbIndex *> & colIdBatch, std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);

private: /* Methods: */

    static bool isVariableLengthType(SharemindTdbType * const type);
    static bool cleanupType(const hid_t aId, SharemindTdbType & type);

    /*
     * Filesystem operations
     */

    boost::filesystem::path nameToPath(const std::string & tbl);
    bool pathRemove(const boost::filesystem::path & path);
    bool pathExists(const boost::filesystem::path & path, bool & status);
    bool pathIsHdf5(const boost::filesystem::path & path);

    /*
     * Parameter validation
     */

    bool validateColumnNames(const std::vector<SharemindTdbString *> & names) const;
    bool validateTableName(const std::string & tbl) const;
    bool validateValues(const std::vector<SharemindTdbValue *> & values) const;

    /*
     * Database operations
     */

    bool readColumn(const hid_t fileId, const std::vector<SharemindTdbIndex *> & colNrBatch, std::vector<std::vector<SharemindTdbValue *> > & valuesBatch);
    bool readDatasetColumn(const hid_t fileId, const hobj_ref_t ref,
            const std::vector<std::pair<hsize_t, std::vector<SharemindTdbValue *> *> > & paramBatch);

    bool objRefToType(const hid_t fileId, const hobj_ref_t ref, hid_t & aId, SharemindTdbType & type);

    bool getColumnCount(const hid_t fileId, hsize_t & ncols);
    bool getRowCount(const hid_t fileId, hsize_t & nrows);
    bool setRowCount(const hid_t fileId, const hsize_t nrows);

    bool closeTableFile(const std::string & tbl);
    hid_t openTableFile(const std::string & tbl);

private: /* Fields: */

    mutable ILogger::Wrapped m_logger;

    boost::filesystem::path m_path;

    TableFileMap m_tableFiles;

}; /* class TdbHdf5Connection { */

} /* namespace sharemind { */

#endif // SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H
