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

#include <map>
#include <string>
#include <vector>
#include <boost/filesystem/path.hpp>
#include <boost/thread/mutex.hpp>
#include <H5Ipublic.h>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/common/Logger/ILogger.h>
#include <sharemind/mod_tabledb/tdbtypes.h>
#include "TdbHdf5ConnectionConf.h"


namespace sharemind {

class __attribute__ ((visibility("internal"))) TdbHdf5Connection {
public: /* Types: */

    typedef uint64_t size_type;

private: /* Types: */

    typedef std::map<std::string, hid_t> TableFileMap;

public: /* Methods: */

    TdbHdf5Connection(ILogger & logger, const boost::filesystem::path & path);
    ~TdbHdf5Connection();

    /*
     * General database table functions
     */

    bool tblCreate(const std::string & tbl, const std::vector<TdbString *> & names, const std::vector<TdbType *> & types);
    bool tblDelete(const std::string & tbl);
    bool tblExists(const std::string & tbl, bool & status);

    bool tblSize(const std::string & tbl, size_type & rows, size_type & cols);

    /*
     * Table data manipulation functions
     */

    bool readColumn(const std::string & tbl, const std::string & colId, std::vector<TdbValue *> & vals);
    bool readColumn(const std::string & tbl, const size_type colId, std::vector<TdbValue *> & vals);
    //bool insertRow(const std::string & tbl, const std::string & rowId, std::vector<TdbValue *> & vals);
    bool insertRow(const std::string & tbl, const std::pair<uint64_t, uint64_t> & rowId, std::vector<TdbValue *> & vals);

private: /* Methods: */

    bool validateTableName(const std::string & tbl) const;

    boost::filesystem::path nameToPath(const std::string & tbl) const;

    hid_t openTableFile(const std::string & tbl);

private: /* Fields: */

    mutable ILogger::Wrapped m_logger;

    boost::filesystem::path m_path;

    TableFileMap m_tableFiles;
    //std::map<std::string, std::set<std::string> > m_sortedTableKeys;
    //std::map<std::string, std::vector<std::string> > m_tableKeys;

    mutable boost::mutex m_mutex; // TODO do we need a recursive mutex?

}; /* class TdbHdf5Connection { */

} /* namespace sharemind { */

#endif // SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTION_H
