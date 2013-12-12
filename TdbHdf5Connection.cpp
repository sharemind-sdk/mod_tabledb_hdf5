/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5Connection.h"

#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/scope_exit.hpp>
#include <H5Epublic.h>
#include <H5Fpublic.h>
#include <H5Gpublic.h>
#include <H5Ppublic.h>
#include <H5Spublic.h>
#include <H5Tpublic.h>


namespace fs = boost::filesystem;

namespace std {
    template<> struct less<SharemindTdbType *> {
        bool operator() (SharemindTdbType * const lhs, SharemindTdbType * const rhs) {
            const int cmp = strcmp(lhs->domain, rhs->domain);
            return cmp == 0 ? strcmp(lhs->name, rhs->name) < 0 : cmp < 0;
        }
    };
} /* namespace std { */

namespace {
const char * const  COL_INDEX_DATASET  = "/meta/column_index";
const size_t        CHUNK_SIZE         = 4096;
const size_t        ERR_MSG_MAX        = 64;
const char * const  FILE_EXT           = "h5";
const char * const  META_GROUP         = "/meta";
const size_t        VL_STR_REF_SIZE    = 16;
} /* namespace { */

namespace {

herr_t err_walk_cb(unsigned n, const H5E_error_t * err_desc, void * client_data) {
    assert(err_desc);
    assert(client_data);

    sharemind::ILogger::Wrapped * logger = static_cast<sharemind::ILogger::Wrapped *>(client_data);

    char maj_msg[ERR_MSG_MAX];
    if (H5Eget_msg(err_desc->maj_num, NULL, maj_msg, ERR_MSG_MAX) < 0)
        return -1;

    char min_msg[ERR_MSG_MAX];
    if (H5Eget_msg(err_desc->min_num, NULL, min_msg, ERR_MSG_MAX) < 0)
        return -1;

    logger->fullDebug() << "HDF5 Error[" << n << "]:" << err_desc->func_name << " - " << maj_msg << ": " << min_msg;

    return 0;
}

herr_t err_handler(hid_t, void * client_data) {
    // Have to make a copy of the stack here. Otherwise HDF5 resets the stack at
    // some point.
    const hid_t estack = H5Eget_current_stack();
    if (estack < 0)
        return -1;
    return H5Ewalk(estack, H5E_WALK_DOWNWARD, err_walk_cb, client_data);
}

} /* namespace { */

namespace sharemind {

BOOST_STATIC_ASSERT(sizeof(TdbHdf5Connection::size_type) >= sizeof(hsize_t));

TdbHdf5Connection::TdbHdf5Connection(ILogger & logger, const fs::path & path)
    : m_logger(logger.wrap("[TdbHdf5Connection] "))
    , m_path(path)
{
    // TODO Check the given path.
    // TODO Register some observers for the path?

    // Register a custom log handler
#if defined SHAREMIND_LOGLEVEL_FULLDEBUG
    if (H5Eset_auto(H5E_DEFAULT, err_handler, &m_logger) < 0) {
        m_logger.error() << "Failed to set HDF5 logging handler.";
        // TODO throw exception
    }
#else
    if (H5Eset_auto(H5E_DEFAULT, NULL, NULL) < 0) {
        m_logger.error() << "Failed to disable HDF5 logging.";
        // TODO throw exception
    }
#endif
}

TdbHdf5Connection::~TdbHdf5Connection() {
    TableFileMap::const_iterator it;
    for (it = m_tableFiles.begin(); it != m_tableFiles.end(); ++it) {
        if (H5Fclose(it->second) < 0)
            m_logger.warning() << "Error while closing handle to table file \""
                << nameToPath(it->first) << "\".";
    }

    m_tableFiles.clear();
}

bool TdbHdf5Connection::tblCreate(const std::string & tbl, const std::vector<SharemindTdbString *> & names, const std::vector<SharemindTdbType *> & types) {
    if (!validateTableName(tbl))
        return false;

    // Check column names
    if (!validateColumnNames(names))
        return false;

    // Set the cleanup flag
    bool success = false;

    fs::path tblPath = nameToPath(tbl);

    // Check if table file exists
    bool exists = false;
    if (!pathExists(tblPath, exists))
        return false;

    if (exists) {
        m_logger.error() << "Table \"" << tbl << "\" already exists.";
        return false;
    }

    // Create a new file handle
    // H5F_ACC_EXCL - Fail if file already exists.
    hid_t fileId = H5Fcreate(tblPath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
    if (fileId < 0) {
        m_logger.error() << "Failed to create table \"" << tbl << "\" file with path " << tblPath << ".";
        return false;
    }

    // Set cleanup handler for the file
    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)(&tblPath)(&fileId)) {
        if (!success) {
            // Close the file
            if (H5Fclose(fileId) < 0)
                m_logger.fullDebug() << "Error while closing table \"" << tbl << "\" file.";

            // Delete the file
            try {
                fs::remove(tblPath);
            } catch (const fs::filesystem_error & e) {
                m_logger.fullDebug() << "Error while removing table \"" << tbl << "\" file: " << e.what();
            }
        }
    } BOOST_SCOPE_EXIT_END

    // Check the provided types
    typedef std::vector<std::pair<std::string, size_type> > ColInfoVector;
    ColInfoVector colInfoVector;
    typedef std::map<SharemindTdbType *, size_t> TypeMap;
    TypeMap typeMap;

    {
        std::vector<SharemindTdbType *>::const_iterator it;
        for (it = types.begin(); it != types.end(); ++it) {
            SharemindTdbType * const type = *it;

            std::pair<TypeMap::iterator, bool> rv = typeMap.insert(TypeMap::value_type(type, 1));
            if (!rv.second) {
                // The length of public string type can vary
                if (!isStringType(type) && rv.first->first->size != type->size) {
                    m_logger.error() << "Inconsistent type data given for type \"" << type->domain << "::" << type->name << "\".";
                    return false;
                }
                ++rv.first->second;
            }

            std::ostringstream oss; // TODO figure out something better than this
            oss << type->domain << "::" << type->name;
            colInfoVector.push_back(ColInfoVector::value_type(oss.str(), rv.first->second - 1));
        }
    }

    const size_t ntypes = typeMap.size();

    // Create the corresponding HDF5 types
    std::vector<std::pair<SharemindTdbType *, hid_t> > memTypes;
    memTypes.reserve(ntypes);

    std::vector<size_t> colSizes;
    colSizes.reserve(ntypes);

    // Set cleanup handler for the opened HDF5 types
    BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&memTypes)) {
        std::vector<std::pair<SharemindTdbType *, hid_t> >::const_iterator it;
        for (it = memTypes.begin(); it != memTypes.end(); ++it)
            if (H5Tclose(it->second) < 0)
                m_logger.fullDebug() << "Error while cleaning up type data for table \"" << tbl << "\".";
        memTypes.clear();
    } BOOST_SCOPE_EXIT_END

    {
        std::map<SharemindTdbType *, size_t>::const_iterator it;
        for (it = typeMap.begin(); it != typeMap.end(); ++it) {
            SharemindTdbType * const type = it->first;

            hid_t tId = -1;

            if (isStringType(type)) {
                // Create a variable length string type
                tId = H5Tcopy(H5T_C_S1);
                if (tId < 0) {
                    m_logger.error() << "Error while setting up type data for table \"" << tbl << "\".";
                    return false;
                }

                if (H5Tset_size(tId, H5T_VARIABLE) < 0) {
                    m_logger.error() << "Error while setting up type data for table \"" << tbl << "\".";

                    if (H5Tclose(tId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up type data for table \"" << tbl << "\".";
                    return false;
                }
            } else {
                // Create a fixed length opaque type
                tId = H5Tcreate(H5T_OPAQUE, type->size);
                if (tId < 0) {
                    m_logger.error() << "Error while setting up type data for table \"" << tbl << "\".";
                    return false;
                }

                // Set a type tag
                std::ostringstream oss; // TODO figure out something better than this
                oss << type->domain << "::" << type->name;
                const std::string tag(oss.str());

                if (H5Tset_tag(tId, tag.c_str()) < 0) {
                    m_logger.error() << "Error while setting up type data for table \"" << tbl << "\".";

                    if (H5Tclose(tId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up type data for table \"" << tbl << "\".";
                    return false;
                }
            }

            assert(tId >= 0);
            memTypes.push_back(std::make_pair(it->first, tId));
            colSizes.push_back(it->second);
        }
    }

    // Create a dataset for each unique column type
    {
        // Set dataset creation properties
        hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&plistId)) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset creation property list for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        assert(memTypes.size() == ntypes);
        assert(colSizes.size() == ntypes);

        std::vector<std::pair<SharemindTdbType *, hid_t> >::const_iterator typeIt = memTypes.begin();
        std::vector<size_t>::const_iterator sizeIt = colSizes.begin();

        for (size_t i = 0; i < ntypes; ++i, ++typeIt, ++sizeIt) {
            SharemindTdbType * const type = typeIt->first;
            const hid_t & tId = typeIt->second;

            const size_t size = isStringType(type) ? VL_STR_REF_SIZE : type->size;

            std::ostringstream oss; // TODO figure out something better than this
            oss << type->domain << "::" << type->name;
            const std::string tag(oss.str());

            // TODO take CHUNK_SIZE from configuration?
            // TODO what about the chunk shape?
            // Set chunk size
            // HDF5 API doesn't give us the size of the variable length string
            // references. However, through simple experimentation we know that
            // the string references currently take up 16 bytes.
            const hsize_t chunkSize = CHUNK_SIZE / size;
            hsize_t dimsChunk[2];
            dimsChunk[0] = chunkSize; dimsChunk[1] = 1; // TODO are vertical chunks OK?
            if (H5Pset_chunk(plistId, 2, dimsChunk) < 0)
                return false;

            // TODO set compression? Probably only useful for some public types
            // (variable length strings cannot be compressed as far as I know).

            // Create a simple two dimensional data space
            hsize_t dims[2];
            dims[0] = 0; dims[1] = *sizeIt;

            const hsize_t maxdims[2] = { H5S_UNLIMITED, H5S_UNLIMITED };

            hid_t sId = H5Screate_simple(2, dims, maxdims);
            if (sId < 0) {
                m_logger.error() << "Failed to create a data space for table \"" << tbl << "\" type \"" << tag << "\".";
                return false;
            }

            // Create the dataset
            hid_t dId = H5Dcreate2(fileId, tag.c_str(), tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
            if (dId < 0) {
                m_logger.error() << "Failed to create dataset for table \"" << tbl << "\" type \"" << tag << "\".";

                if (H5Sclose(sId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up data space for table \"" << tbl << "\" type \"" << tag << "\".";
                return false;
            }

            // Close the data space and the dataset
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up data space for table \"" << tbl << "\" type \"" << tag << "\".";

            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset for table \"" << tbl << "\" type \"" << tag << "\".";
        }
    }

    // Create a dataset for the column index meta info
    {
        // Create the column index data type
        // struct ColumnIndex {
        //     const char * name;
        //     const char * dataset_name;
        //     hsize_t dataset_column;
        // };

        assert(names.size() == types.size());
        const size_t size = names.size();

        hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(ColumnIndex));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&tId)) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        // const char * name
        hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";

            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&nameTId)) {
            if (H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "name", HOFFSET(ColumnIndex, name), nameTId) < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";
            return false;
        }

        // const char * dataset_name
        hid_t dsNameTId = H5Tcopy(H5T_C_S1);
        if (dsNameTId < 0 || H5Tset_size(dsNameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";

            if (dsNameTId >= 0 && H5Tclose(dsNameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&dsNameTId)) {
            if (H5Tclose(dsNameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "dataset_name", HOFFSET(ColumnIndex, dataset_name), dsNameTId) < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";
            return false;
        }

        // size_t dataset_column;
        if (H5Tinsert(tId, "dataset_column", HOFFSET(ColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create column meta info data type for table \"" << tbl << "\".";
            return false;
        }

        // Create the 1 dimensional data space
        const hsize_t dims = size;
        const hsize_t maxdims = H5S_UNLIMITED;
        hid_t sId = H5Screate_simple(1, &dims, &maxdims);
        if (sId < 0) {
            m_logger.error() << "Failed to create column meta info data space creation property list for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&sId)) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info data space for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        // Create a meta data group
        hid_t gId = H5Gcreate(fileId, META_GROUP, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (gId < 0) {
            m_logger.error() << "Failed to create meta info group for table \"" << tbl << "\".";
            return false;
        }

        if (H5Gclose(gId) < 0)
            m_logger.fullDebug() << "Error while cleaning up meta info group for table \"" << tbl << "\".";

        // Create the dataset creation property list
        hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);
        if (plistId < 0) {
            m_logger.error() << "Failed to create column meta info dataset creation property list for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&plistId)) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset creation property list for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        const hsize_t dimsChunk = CHUNK_SIZE / (sizeof(size_type) + VL_STR_REF_SIZE + VL_STR_REF_SIZE);
        if (H5Pset_chunk(plistId, 1, &dimsChunk) < 0) {
            m_logger.error() << "Failed to set column meta info dataset creation property list info for table \"" << tbl << "\".";
            return false;
        }

        // Create the dataset
        hid_t dId = H5Dcreate2(fileId, COL_INDEX_DATASET, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to create column meta info dataset for table \"" << tbl << "\".";
            return false;
        }

        // Write column index data
        if (size > 0) {
            // Serialize the column index data
            ColumnIndex colIdx[size];

            std::vector<SharemindTdbString *>::const_iterator nameIt = names.begin();
            ColInfoVector::const_iterator mIt = colInfoVector.begin();

            for (size_t i = 0; i < size; ++i, ++nameIt, ++mIt) {
                colIdx[i].name = (*nameIt)->str;
                colIdx[i].dataset_name = mIt->first.c_str();
                colIdx[i].dataset_column = mIt->second;
            }

            // Write the column index data
            if (H5Dwrite(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, colIdx) < 0) {
                m_logger.error() << "Failed to write column meta info dataset for table \"" << tbl << "\".";
                return false;
            }
        }

        if (H5Dclose(dId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info dataset for table \"" << tbl << "\".";
    }

    // TODO add column type info as a separate dataset?

    // Flush the buffers to reduce the chance of file corruption
    if (H5Fflush(fileId, H5F_SCOPE_LOCAL) < 0)
        m_logger.fullDebug() << "Error while flushing buffers for table \"" << tbl << "\".";

    success = true;

    return true;
}

bool TdbHdf5Connection::tblDelete(const std::string & tbl) {
    if (!validateTableName(tbl))
        return false;

    // Get table path
    const fs::path tblPath = nameToPath(tbl);

    // Delete the table file
    try {
        if (!remove(tblPath)) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return false;
        }
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while deleting table \"" << tbl << "\" file " << tblPath << ".";
        return false;
    }

    return true;
}

bool TdbHdf5Connection::tblExists(const std::string & tbl, bool & status) {
    if (!validateTableName(tbl))
        return false;

    // Get table path
    const fs::path tblPath = nameToPath(tbl);

    // Check if the file exists
    if (!pathExists(tblPath, status))
        return false;

    // Check if the file has the right format
    if (status && !pathIsHdf5(tblPath)) {
        m_logger.error() << "Table \"" << tbl << "\" file " << tblPath << " is not a valid table file.";
        return false;
    }

    return true;
}

bool TdbHdf5Connection::tblSize(const std::string & tbl, size_type & rows, size_type & cols) {
    if (!validateTableName(tbl))
        return false;

    // Open the table file
    hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table \"" << tbl << "\".";
        return false;
    }

    // Read the column meta info
    {
        // Get dataset
        hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to open column meta info dataset for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&dId)) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up type data for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        // Get data space
        hid_t sId = H5Dget_space(dId);
        if (sId < 0) {
            m_logger.error() << "Failed to open column meta info data space for table \"" << tbl << "\".";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tbl)(&sId)) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up data space for table \"" << tbl << "\".";
        } BOOST_SCOPE_EXIT_END

        // TODO check rank before getting dims?

        // Get size of data space
        hsize_t dims;
        if (H5Sget_simple_extent_dims(sId, &dims, NULL) < 0) {
            m_logger.error() << "Failed to column count from column meta info for table \"" << tbl << "\".";
            return false;
        }

        cols = dims;
    }

    // TODO Add an attribute with the current row count?
    (void) rows;

    return true;
}

bool TdbHdf5Connection::readColumn(const std::string & tbl, const std::string & colId, std::vector<SharemindTdbValue *> & vals) {
    (void)tbl; (void)colId; (void)vals;
    return false;
}

bool TdbHdf5Connection::readColumn(const std::string & tbl, const size_type colId, std::vector<SharemindTdbValue *> & vals) {
    (void)tbl; (void)colId; (void)vals;
    return false;
}

bool TdbHdf5Connection::insertRow(const std::string & tbl, const std::vector<SharemindTdbValue *> & vals) {
    (void) vals;
    if (!validateTableName(tbl))
        return false;

    // Open the table file
    hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table \"" << tbl << "\".";
        return false;
    }

    // Get the size of the table
    size_type rowCount = 0, colCount = 0;
    if (!tblSize(tbl, rowCount, colCount))
        return false;

    // Get column types

    // Check if the given data matches the table schema
    // TODO We could ask to have all given values as scalars. However, this
    // would not be optimal in the cases where we have only a few different
    // column types.

    // Write row data

    return true;
}

bool TdbHdf5Connection::isStringType(SharemindTdbType * const type) {
    return strcmp(type->domain, "public") == 0 && strcmp(type->name, "string") == 0;
}

bool TdbHdf5Connection::pathExists(const fs::path & path, bool & status) const {
    try {
        status = exists(path);
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while checking if file " << path << " exists: " << e.what();
        return false;
    }

    return true;
}

bool TdbHdf5Connection::pathIsHdf5(const fs::path & path) const {
    // Check if the file has the right format
    htri_t isHdf5 = H5Fis_hdf5(path.c_str());
    if (isHdf5 < 0) {
        m_logger.error() << "Error while checking file " << path << " format.";
        return false;
    }

    return isHdf5 > 0;
}

bool TdbHdf5Connection::validateColumnNames(const std::vector<SharemindTdbString *> & names) const {
    std::vector<SharemindTdbString *>::const_iterator it;
    for (it = names.begin(); it != names.end(); ++it) {
        SharemindTdbString * const str = *it;
        assert(str);

        if (strlen(str->str) == 0) {
            m_logger.error() << "Column name must be a non-empty string.";
            return false;
        }
    }

    return true;
}

bool TdbHdf5Connection::validateTableName(const std::string & tbl) const {
    if (tbl.empty()) {
        m_logger.error() << "Table name must be a non-empty string.";
        return false;
    }

    // Check the table name for some special symbols e.g. '/'
    // TODO check if table name is valid with some static regular expressions?

    return true;
}

boost::filesystem::path TdbHdf5Connection::nameToPath(const std::string & tbl) {
    assert(!tbl.empty());

    // TODO Should we try to eliminate the copying?
    fs::path p(m_path);
    p /= tbl;
    p.replace_extension(FILE_EXT);

    return p;
}

bool TdbHdf5Connection::closeTableFile(const std::string & tbl) {
    assert(tbl.empty());

    TableFileMap::iterator it = m_tableFiles.find(tbl);
    if (it == m_tableFiles.end())
        return false;

    if (H5Fclose(it->second) < 0)
        m_logger.fullDebug() << "Error while closing table \"" << tbl << "\" file.";

    m_tableFiles.erase(it);
    return true;
}

hid_t TdbHdf5Connection::openTableFile(const std::string & tbl) {
    assert(!tbl.empty());

    // Check if we already have a file handle for the table
    TableFileMap::const_iterator it = m_tableFiles.find(tbl);
    if (it != m_tableFiles.end())
        return it->second;

    // Open a new handle for the table
    const fs::path tblPath = nameToPath(tbl);
    hid_t id = H5Fopen(tblPath.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
    if (id < 0)
        return -1;

    m_tableFiles.insert(TableFileMap::value_type(tbl, id));

    return id;
}

} /* namespace sharemind { */
