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
#include <H5Apublic.h>
#include <H5Epublic.h>
#include <H5Fpublic.h>
#include <H5Gpublic.h>
#include <H5Opublic.h>
#include <H5Ppublic.h>
#include <H5Spublic.h>
#include <H5Tpublic.h>
#include <sharemind/mod_tabledb/TdbTypesUtil.h>


namespace fs = boost::filesystem;

namespace std {
    template<> struct less<SharemindTdbType *> {
        bool operator() (SharemindTdbType * const lhs, SharemindTdbType * const rhs) {
            int cmp = 0;
            return !(cmp = strcmp(lhs->domain, rhs->domain))
                    && !(cmp = strcmp(lhs->name, rhs->name))
                    ? lhs->size < rhs->size
                    : cmp < 0;
        }
    };
} /* namespace std { */

namespace {
const char * const  COL_INDEX_DATASET       = "/meta/column_index";
const char * const  COL_INDEX_TYPE          = "/meta/column_index_type";
const size_t        CHUNK_SIZE              = 4096;
const char * const  DATASET_TYPE_ATTR       = "type";
const char * const  DATASET_TYPE_ATTR_TYPE  = "/meta/dataset_type";
const size_t        ERR_MSG_MAX             = 64;
const char * const  FILE_EXT                = "h5";
const char * const  META_GROUP              = "/meta";
const char * const  ROW_COUNT_ATTR          = "row_count";
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

BOOST_STATIC_ASSERT(sizeof(TdbHdf5Connection::size_type) == sizeof(hsize_t));

TdbHdf5Connection::TdbHdf5Connection(ILogger & logger, const fs::path & path)
    : m_logger(logger.wrap("[TdbHdf5Connection] "))
    , m_path(path)
{
    // TODO need two kinds of rollback:
    // 1) Per operation rollback stack - so that the operation leaves the
    // database in a consistent state.
    // 2) Rollback function for the last database operation. When we succeed,
    // but other miners fail.

    // TODO Check the given path.
    // TODO Some stuff probably should be allocated on the heap.
    // TODO locking?
    // TODO batch operations support
    // TODO check for uniqueness of column names
    // TODO Need to refactor the huge functions into smaller ones.

    // Register a custom log handler
    if (H5Eset_auto(H5E_DEFAULT, err_handler, &m_logger) < 0) {
        m_logger.error() << "Failed to set HDF5 logging handler.";
        throw InitializationException("Failed to set HDF5 logging handler.");
    }
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
    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)) {
        if (!success)
            m_logger.fullDebug() << "Failed to create table \"" << tbl << "\".";
    } BOOST_SCOPE_EXIT_END

    // Do some simple checks on the parameters
    if (names.empty()) {
        m_logger.error() << "No column names given.";
        return false;
    }

    if (types.empty()) {
        m_logger.error() << "No column types given.";
        return false;
    }

    if (names.size() != types.size()) {
        m_logger.error() << "Differing number of column names and column types.";
        return false;
    }

    if (!validateTableName(tbl))
        return false;

    // Check column names
    if (!validateColumnNames(names))
        return false;

    fs::path tblPath = nameToPath(tbl);

    // Check if table file exists
    bool exists = false;
    if (!pathExists(tblPath, exists))
        return false;

    if (exists) {
        m_logger.error() << "Table already exists.";
        return false;
    }

    // Remove dangling file handler, if any (file was deleted while the handler
    // was open).
    m_tableFiles.erase(tbl);

    // Create a new file handle
    // H5F_ACC_EXCL - Fail if file already exists.
    const hid_t fileId = H5Fcreate(tblPath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
    if (fileId < 0) {
        m_logger.error() << "Failed to create table file with path " << tblPath << ".";
        return false;
    }

    // Set cleanup handler for the file
    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tblPath)(&fileId)) {
        if (!success) {
            // Close the file
            if (H5Fclose(fileId) < 0)
                m_logger.fullDebug() << "Error while closing table file.";

            // Delete the file
            try {
                fs::remove(tblPath);
            } catch (const fs::filesystem_error & e) {
                m_logger.fullDebug() << "Error while removing table file: " << e.what();
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
            if (!rv.second)
                ++rv.first->second;

            std::ostringstream oss; // TODO figure out something better than this
            oss << type->domain << "::" << type->name << "::" << type->size;
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
    BOOST_SCOPE_EXIT((&m_logger)(&memTypes)) {
        std::vector<std::pair<SharemindTdbType *, hid_t> >::const_iterator it;
        for (it = memTypes.begin(); it != memTypes.end(); ++it)
            if (H5Tclose(it->second) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type.";
        memTypes.clear();
    } BOOST_SCOPE_EXIT_END

    {
        std::map<SharemindTdbType *, size_t>::const_iterator it;
        for (it = typeMap.begin(); it != typeMap.end(); ++it) {
            SharemindTdbType * const type = it->first;

            hid_t tId = H5I_INVALID_HID;

            if (isVariableLengthType(type)) {
                // Create a variable length type
                tId = H5Tvlen_create(H5T_NATIVE_CHAR);
                if (tId < 0) {
                    m_logger.error() << "Failed to create dataset type.";
                    return false;
                }
            } else {
                // Create a fixed length opaque type
                tId = H5Tcreate(H5T_OPAQUE, type->size);
                if (tId < 0) {
                    m_logger.error() << "Failed to create dataset type.";
                    return false;
                }

                // Set a type tag
                std::ostringstream oss; // TODO figure out something better than this
                oss << type->domain << "::" << type->name << "::" << type->size;
                const std::string tag(oss.str());

                if (H5Tset_tag(tId, tag.c_str()) < 0) {
                    m_logger.error() << "Failed to set dataset type tag.";

                    if (H5Tclose(tId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up dataset type.";
                    return false;
                }
            }

            assert(tId >= 0);
            memTypes.push_back(std::make_pair(it->first, tId));
            colSizes.push_back(it->second);
        }
    }

    // Create some meta info objects
    {
        // Create a meta data group
        const hid_t gId = H5Gcreate(fileId, META_GROUP, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (gId < 0) {
            m_logger.error() << "Failed to create meta info group.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&gId)) {
            if (H5Gclose(gId) < 0)
                m_logger.fullDebug() << "Error while cleaning up meta info group.";
        } BOOST_SCOPE_EXIT_END

        // Create a data space for the row count attribute
        const hsize_t aDims = 1;
        const hid_t aSId = H5Screate_simple(1, &aDims, NULL);
        if (aSId < 0) {
            m_logger.error() << "Failed to create row count attribute data space.";
            return false;
        }

        // Set cleanup handler for the row count attribute data space
        BOOST_SCOPE_EXIT((&m_logger)(&aSId)) {
            if (H5Sclose(aSId) < 0)
                m_logger.fullDebug() << "Error while cleaning up row count attribute data space.";
        } BOOST_SCOPE_EXIT_END

        // Add a type attribute to the dataset
        const hid_t aId = H5Acreate(gId, ROW_COUNT_ATTR, H5T_NATIVE_HSIZE, aSId, H5P_DEFAULT, H5P_DEFAULT);
        if (aId < 0) {
            m_logger.error() << "Failed to create row count attribute.";
            return false;
        }

        // Set cleanup handler for the attribute
        BOOST_SCOPE_EXIT((&m_logger)(&aId)) {
            if (H5Aclose(aId) < 0)
                m_logger.fullDebug() << "Error while cleaning up row count attribute.";
        } BOOST_SCOPE_EXIT_END

        // Write the type attribute
        const hsize_t rowCount = 0;
        if (H5Awrite(aId, H5T_NATIVE_HSIZE, &rowCount) < 0) {
            m_logger.error() << "Failed to write row count attribute.";
            return false;
        }
    }

    // Create a dataset for each unique column type
    {
        // Set dataset creation properties
        const hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);

        BOOST_SCOPE_EXIT((&m_logger)(&plistId)) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset creation property list.";
        } BOOST_SCOPE_EXIT_END

        // Create the type attribute data type
        const hid_t aTId = H5Tcreate(H5T_COMPOUND, sizeof(SharemindTdbType));
        if (aTId < 0) {
            m_logger.error() << "Failed to create dataset type attribute type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&aTId)) {
            if (H5Tclose(aTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        } BOOST_SCOPE_EXIT_END

        // const char * domain
        const hid_t domainTId = H5Tcopy(H5T_C_S1);
        if (domainTId < 0 || H5Tset_size(domainTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";

            if (domainTId >= 0 && H5Tclose(domainTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&domainTId)) {
            if (H5Tclose(domainTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(aTId, "domain", HOFFSET(SharemindTdbType, domain), domainTId) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return false;
        }

        // const char * name
        const hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";

            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&nameTId)) {
            if (H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(aTId, "name", HOFFSET(SharemindTdbType, name), nameTId) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return false;
        }

        // hsize_t size
        if (H5Tinsert(aTId, "size", HOFFSET(SharemindTdbType, size), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return false;
        }

        // Commit the dataset type attribute type
        if (H5Tcommit(fileId, DATASET_TYPE_ATTR_TYPE, aTId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            m_logger.error() << "Failed to commit dataset type attribute type.";
            return false;
        }

        assert(memTypes.size() == ntypes);
        assert(colSizes.size() == ntypes);

        std::vector<std::pair<SharemindTdbType *, hid_t> >::const_iterator typeIt = memTypes.begin();
        std::vector<size_t>::const_iterator sizeIt = colSizes.begin();

        for (size_t i = 0; i < ntypes; ++i, ++typeIt, ++sizeIt) {
            SharemindTdbType * const type = typeIt->first;
            const hid_t & tId = typeIt->second;

            const size_t size = isVariableLengthType(type) ? sizeof(hvl_t) : type->size;

            std::ostringstream oss; // TODO figure out something better than this
            oss << type->domain << "::" << type->name << "::" << type->size;
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

            const hid_t sId = H5Screate_simple(2, dims, maxdims);
            if (sId < 0) {
                m_logger.error() << "Failed to create a data space type \"" << tag << "\".";
                return false;
            }

            // Set cleanup handler for the data space
            BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
                if (H5Sclose(sId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up data space.";
            } BOOST_SCOPE_EXIT_END

            // Create the dataset
            const hid_t dId = H5Dcreate(fileId, tag.c_str(), tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
            if (dId < 0) {
                m_logger.error() << "Failed to create dataset type \"" << tag << "\".";
                return false;
            }

            // Set cleanup handler for the dataset
            BOOST_SCOPE_EXIT((&m_logger)(&dId)) {
                if (H5Dclose(dId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset.";
            } BOOST_SCOPE_EXIT_END

            // Create a data space for the type attribute
            const hsize_t aDims = 1;
            const hid_t aSId = H5Screate_simple(1, &aDims, NULL);
            if (aSId < 0) {
                m_logger.error() << "Failed to create dataset type attribute data space.";
                return false;
            }

            // Set cleanup handler for the type attribute data space
            BOOST_SCOPE_EXIT((&m_logger)(&aSId)) {
                if (H5Sclose(aSId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute data space.";
            } BOOST_SCOPE_EXIT_END

            // Add a type attribute to the dataset
            const hid_t aId = H5Acreate(dId, DATASET_TYPE_ATTR, aTId, aSId, H5P_DEFAULT, H5P_DEFAULT);
            if (aId < 0) {
                m_logger.error() << "Failed to create dataset type attribute.";
                return false;
            }

            // Set cleanup handler for the attribute
            BOOST_SCOPE_EXIT((&m_logger)(&aId)) {
                if (H5Aclose(aId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
            } BOOST_SCOPE_EXIT_END

            // Write the type attribute
            if (H5Awrite(aId, aTId, type) < 0) {
                m_logger.error() << "Failed to write dataset type attribute.";
                return false;
            }
        }
    }

    // Create a dataset for the column index meta info
    {
        // Create the column index data type
        // struct ColumnIndex {
        //     const char * name;
        //     hobj_ref_t   dataset_ref;
        //     hsize_t      dataset_column;
        // };

        assert(names.size() == types.size());
        const size_t size = names.size();

        const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(ColumnIndex));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tId)) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        } BOOST_SCOPE_EXIT_END

        // const char * name
        const hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";

            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&nameTId)) {
            if (H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "name", HOFFSET(ColumnIndex, name), nameTId) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return false;
        }

        // hobj_ref_t dataset_ref
        if (H5Tinsert(tId, "dataset_ref", HOFFSET(ColumnIndex, dataset_ref), H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return false;
        }

        // hsize_t dataset_column
        if (H5Tinsert(tId, "dataset_column", HOFFSET(ColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return false;
        }

        // Create the 1 dimensional data space
        const hsize_t dims = size;
        const hsize_t maxdims = H5S_UNLIMITED;
        const hid_t sId = H5Screate_simple(1, &dims, &maxdims);
        if (sId < 0) {
            m_logger.error() << "Failed to create column meta info data space creation property list.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
        } BOOST_SCOPE_EXIT_END

        // Commit the column meta info data type
        if (H5Tcommit(fileId, COL_INDEX_TYPE, tId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            m_logger.error() << "Failed to commit column meta info data type.";
            return false;
        }

        // Create the dataset creation property list
        const hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);
        if (plistId < 0) {
            m_logger.error() << "Failed to create column meta info dataset creation property list.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&plistId)) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset creation property list.";
        } BOOST_SCOPE_EXIT_END

        const hsize_t dimsChunk = CHUNK_SIZE / (sizeof(hobj_ref_t) + sizeof(hvl_t) + sizeof(size_type));
        if (H5Pset_chunk(plistId, 1, &dimsChunk) < 0) {
            m_logger.error() << "Failed to set column meta info dataset creation property list info.";
            return false;
        }

        // Create the dataset
        const hid_t dId = H5Dcreate(fileId, COL_INDEX_DATASET, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to create column meta info dataset.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&dId)) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        } BOOST_SCOPE_EXIT_END

        // Write column index data
        if (size > 0) {
            // Serialize the column index data
            ColumnIndex colIdx[size];

            std::vector<SharemindTdbString *>::const_iterator nameIt = names.begin();
            ColInfoVector::const_iterator mIt = colInfoVector.begin();

            for (size_t i = 0; i < size; ++i, ++nameIt, ++mIt) {
                colIdx[i].name = (*nameIt)->str;
                colIdx[i].dataset_column = mIt->second;

                if (H5Rcreate(&colIdx[i].dataset_ref, fileId, mIt->first.c_str(), H5R_OBJECT, -1) < 0) {
                    m_logger.error() << "Failed to create column meta info type reference.";
                    return false;
                }
            }

            // Write the column index data
            if (H5Dwrite(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, colIdx) < 0) {
                m_logger.error() << "Failed to write column meta info dataset.";
                return false;
            }
        }
    }

    // Flush the buffers to reduce the chance of file corruption
    if (H5Fflush(fileId, H5F_SCOPE_LOCAL) < 0)
        m_logger.fullDebug() << "Error while flushing buffers.";

    // Add the file handler to the map
    const bool r = m_tableFiles.insert(TableFileMap::value_type(tbl, fileId)).second;
    assert(r);

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
        m_logger.error() << "Error while deleting table \"" << tbl << "\" file " << tblPath << ": " << e.what() << ".";
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

bool TdbHdf5Connection::tblColCount(const std::string & tbl, size_type & count) {
    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)) {
        if (!success)
            m_logger.error() << "Failed to get column count for table \"" << tbl << "\".";
    } BOOST_SCOPE_EXIT_END

    if (!validateTableName(tbl))
        return false;

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return false;
    }

    // Read column meta info
    hsize_t ncols = 0;
    if (!getColumnCount(fileId, ncols))
        return false;

    count = ncols;

    success = true;

    return true;
}

bool TdbHdf5Connection::tblRowCount(const std::string & tbl, size_type & count) {
    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)) {
        if (!success)
            m_logger.error() << "Failed to get row count for table \"" << tbl << "\".";
    } BOOST_SCOPE_EXIT_END

    if (!validateTableName(tbl))
        return false;

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return false;
    }

    // Read row meta info
    hsize_t nrows = 0;
    if (!getRowCount(fileId, nrows))
        return false;

    count = nrows;

    success = true;

    return true;
}

bool TdbHdf5Connection::insertRow(const std::string & tbl, const std::vector<std::vector<SharemindTdbValue *> > & valuesBatch) {
    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)) {
        if (!success)
            m_logger.error() << "Failed to insert row(s) into table \"" << tbl << "\".";
    } BOOST_SCOPE_EXIT_END

    if (valuesBatch.size() > 1) {
        m_logger.error() << "Currently we don't support batched operations.";
        return false;
    }

    if (valuesBatch.empty()) {
        m_logger.error() << "Empty batch of parameters given.";
        return false;
    }

    const std::vector<SharemindTdbValue *> & values = valuesBatch.back();

    // Do some simple checks on the parameters
    if (values.empty()) {
        m_logger.error() << "No values given.";
        return false;
    }

    if (!validateTableName(tbl))
        return false;

    if (!validateValues(values))
        return false;

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return false;
    }

    // Get table row count
    hsize_t rowCount = 0;
    if (!getRowCount(fileId, rowCount))
        return false;

    // Get table column count
    hsize_t colCount = 0;
    if (!getColumnCount(fileId, colCount))
        return false;

    // Get column types
    typedef std::map<hobj_ref_t, std::pair<SharemindTdbType *, hid_t> > RefTypeMap;
    RefTypeMap refTypes;

    typedef std::map<SharemindTdbType *, size_type> TypeCountMap;
    TypeCountMap typeCounts;

    BOOST_SCOPE_EXIT((&m_logger)(&refTypes)) {
        std::map<hobj_ref_t, std::pair<SharemindTdbType *, hid_t> >::const_iterator it;
        for (it = refTypes.begin(); it != refTypes.end(); ++it) {
            SharemindTdbType * const type = it->second.first;
            const hid_t aId = it->second.second;

            if (!cleanupType(aId, *type))
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute object.";

            if (H5Aclose(aId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";

            delete type;
        }
        refTypes.clear();
    } BOOST_SCOPE_EXIT_END

    {
        // Create a type for reading the dataset references
        const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(hobj_ref_t));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tId)) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "dataset_ref", 0, H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return false;
        }

        // Open the column meta info dataset
        const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.fullDebug() << "Failed to open column meta info dataset.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&dId)) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        } BOOST_SCOPE_EXIT_END

        // Read dataset references from the column meta info dataset
        hobj_ref_t * dsetRefs = new hobj_ref_t[colCount];
        if (H5Dread(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, dsetRefs) < 0) {
            m_logger.fullDebug() << "Failed to read column meta info dataset.";
            return false;
        }

        BOOST_SCOPE_EXIT((&dsetRefs)) {
            delete[] dsetRefs;
        } BOOST_SCOPE_EXIT_END

        // Resolve references to types
        for (size_type i = 0; i < colCount; ++i) {
            RefTypeMap::const_iterator it = refTypes.find(dsetRefs[i]);
            if (it == refTypes.end()) {
                hid_t aId = H5I_INVALID_HID;

                // Read the type attribute
                SharemindTdbType * const type = new SharemindTdbType;
                if (!objRefToType(fileId, dsetRefs[i], aId, *type)) {
                    m_logger.error() << "Failed to get type info from dataset reference.";
                    delete type;
                    return false;
                }

                const bool r = refTypes.insert(RefTypeMap::value_type(dsetRefs[i], RefTypeMap::mapped_type(type, aId))).second;
                assert(r);
                const bool r2 = typeCounts.insert(TypeCountMap::value_type(type, 1)).second;
                assert(r2);
            } else {
                SharemindTdbType * const type = it->second.first;
                ++typeCounts[type];
            }
        }
    }

    // Aggregate the values by the value types
    typedef std::map<SharemindTdbType *, std::vector<SharemindTdbValue *> > TypeValueMap;
    TypeValueMap typeValues;

    size_type valCount = 0;

    {
        std::vector<SharemindTdbValue *>::const_iterator it;
        for (it = values.begin(); it != values.end(); ++it) {
            SharemindTdbValue * const val = *it;
            SharemindTdbType * const type = val->type;

            assert(val->size);

            TypeCountMap::const_iterator tIt = typeCounts.find(type);
            if (tIt == typeCounts.end()) {
                m_logger.error() << "Given values do not match the table schema.";
                return false;
            }

            if (isVariableLengthType(type)) {
                // For variable length types we do not support arrays
                valCount += 1;
            } else {
                if (type->size != tIt->first->size) {
                    m_logger.error() << "Given values do not match the table schema.";
                    return false;
                }

                assert(val->size % type->size == 0);
                valCount += val->size / type->size;
            }

            typeValues[type].push_back(val);
        }
    }

    // Check if we have values for all the columns
    if (valCount != colCount) {
        m_logger.error() << "Given number of values differs from the number of columns.";
        return false;
    }

    // Check the if we have the correct number of values for each type
    {
        TypeValueMap::const_iterator it;
        for (it = typeValues.begin(); it != typeValues.end(); ++it) {
            SharemindTdbType * const type = it->first;
            const std::vector<SharemindTdbValue *> & values = it->second;

            size_type count = 0;

            if (isVariableLengthType(type)) {
                // For variable length types we do not support arrays
                count = values.size();
            } else {
                std::vector<SharemindTdbValue *>::const_iterator vIt;
                for (vIt = values.begin(); vIt != values.end(); ++vIt)
                    count += (*vIt)->size / (*vIt)->type->size;
            }

            TypeCountMap::const_iterator tIt = typeCounts.find(type);
            assert(tIt != typeCounts.end());

            if (count != tIt->second) {
                m_logger.error() << "Invalid number of values for type \""
                    << type->domain << "::" << type->name << "\".";
                return false;
            }
        }
    }

    // Set cleanup handler to restore the initial state if when something goes wrong
    typedef std::map<hobj_ref_t, std::pair<hsize_t, hsize_t> > CleanupMap;
    CleanupMap cleanup;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&cleanup)(&fileId)) {
        if (!success) {
            std::map<hobj_ref_t, std::pair<hsize_t, hsize_t> >::const_iterator it;
            for (it = cleanup.begin(); it != cleanup.end(); ++it) {
                const hobj_ref_t dsetRef = it->first;

                // Get dataset from reference
                const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &dsetRef);
                if (oId < 0) {
                    m_logger.error() << "Error while restoring initial state: Failed to open dataset reference.";
                    break;
                }

                // Set the size of the dataset back to the original
                const hsize_t dims[] = { it->second.first, it->second.second };
                if (H5Dset_extent(oId, dims) < 0) {
                    m_logger.error() << "Error while restoring initial state: Failed to clean up changes to the table.";

                    if (H5Oclose(oId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up dataset object.";

                    break;
                }

                if (H5Oclose(oId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset object.";
            }

            cleanup.clear();
        }
    } BOOST_SCOPE_EXIT_END

    // For each dataset, write the data
    {
        RefTypeMap::const_iterator it;
        for (it = refTypes.begin(); it != refTypes.end(); ++it) {
            const hobj_ref_t dsetRef = it->first;
            SharemindTdbType * const type = it->second.first;

            // Get the number of columns for this type
            TypeCountMap::const_iterator tIt = typeCounts.find(type);
            assert(tIt != typeCounts.end());

            const size_type dsetCols = tIt->second;
            // TODO sanity checks for row and column counts

            // Get dataset from reference. We already checked earlier if this is
            // a valid dataset reference.
            const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &dsetRef);
            if (oId < 0) {
                m_logger.error() << "Failed to get dataset from dataset reference.";
                return false;
            }

            BOOST_SCOPE_EXIT((&m_logger)(&oId)) {
                if (H5Oclose(oId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset.";
            } BOOST_SCOPE_EXIT_END

            // Get dataset type
            const hid_t tId = H5Dget_type(oId);
            if (tId < 0) {
                m_logger.error() << "Failed to get dataset type for type \"" << type->domain << "::" << type->name << "\".";
                return false;
            }

            BOOST_SCOPE_EXIT((&m_logger)(&tId)) {
                if (H5Tclose(tId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type.";
            } BOOST_SCOPE_EXIT_END

            // Create a simple memory data space
            const hsize_t mDims = dsetCols;
            const hid_t mSId = H5Screate_simple(1, &mDims, NULL);
            if (mSId < 0) {
                m_logger.error() << "Failed to create memory data space for type \"" << type->domain << "::" << type->name << "\".";
                return false;
            }

            BOOST_SCOPE_EXIT((&m_logger)(&mSId)) {
                if (H5Sclose(mSId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up memory data space.";
            } BOOST_SCOPE_EXIT_END

            // Extend the dataset
            const hsize_t dims[] = { rowCount + 1, dsetCols };
            if (H5Dset_extent(oId, dims) < 0) {
                m_logger.error() << "Failed to extend dataset for type \"" << type->domain << "::" << type->name << "\".";
                return false;
            }

            // Register this dataset for cleanup
            cleanup.insert(CleanupMap::value_type(dsetRef, std::pair<hsize_t, hsize_t>(rowCount, dsetCols)));

            // Get dataset data space
            const hid_t sId = H5Dget_space(oId);
            if (tId < 0) {
                m_logger.error() << "Failed to get dataset data space for type \"" << type->domain << "::" << type->name << "\".";
                return false;
            }

            BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
                if (H5Sclose(sId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset data space.";
            } BOOST_SCOPE_EXIT_END

            // Select a hyperslab in the data space to write to
            const hsize_t start[] = { rowCount, 0 };
            const hsize_t count[] = { 1, dsetCols };
            if (H5Sselect_hyperslab(sId, H5S_SELECT_SET, start, NULL, count, NULL) < 0) {
                m_logger.error() << "Failed to do selection in data space for type \"" << type->domain << "::" << type->name << "\".";
                return false;
            }

            // Serialize the values
            TypeValueMap::const_iterator tvIt = typeValues.find(type);
            assert(tvIt != typeValues.end());
            const std::vector<SharemindTdbValue *> & values = tvIt->second;

            // Aggregate the values into a single buffer
            void * buffer = NULL;
            bool delBuffer = false;

            if (isVariableLengthType(type)) {
                assert(dsetCols == values.size());

                buffer = ::operator new(dsetCols * sizeof(hvl_t));
                delBuffer = true;

                hvl_t * cursor = static_cast<hvl_t *>(buffer);
                std::vector<SharemindTdbValue *>::const_iterator vIt;
                for (vIt = values.begin(); vIt != values.end(); ++vIt) {
                    SharemindTdbValue * const val = *vIt;
                    cursor->len = val->size;
                    cursor->p = val->buffer;
                    ++cursor;
                }
            } else {
                if (values.size() == 1) {
                    // Since we don't have to aggregate anything, we can use the
                    // existing buffer.
                    buffer = values.back()->buffer;
                } else {
                    buffer = ::operator new(dsetCols * type->size);
                    delBuffer = true;

                    size_t offset = 0;
                    std::vector<SharemindTdbValue *>::const_iterator vIt;
                    for (vIt = values.begin(); vIt != values.end(); ++vIt) {
                        SharemindTdbValue * const val = *vIt;
                        memcpy(static_cast<char *>(buffer) + offset, val->buffer, val->size);
                        offset += val->size;
                    }
                }
            }

            assert(buffer);

            BOOST_SCOPE_EXIT((&buffer)(&delBuffer)) {
                if (delBuffer)
                    ::operator delete(buffer);
            } BOOST_SCOPE_EXIT_END

            // Write the values
            if (H5Dwrite(oId, tId, mSId, sId, H5P_DEFAULT, buffer) < 0) {
                m_logger.error() << "Failed to write values for type \""
                    << type->domain << "::" << type->name << "\".";
                return false;
            }
        }
    }

    // Update row count
    if (!setRowCount(fileId, rowCount + 1)) {
        m_logger.error() << "Failed to update row count.";
        return false;
    }

    // Flush the buffers to reduce the chance of file corruption
    if (H5Fflush(fileId, H5F_SCOPE_LOCAL) < 0)
        m_logger.fullDebug() << "Error while flushing buffers.";

    success = true;

    return true;
}

bool TdbHdf5Connection::readColumn(const std::string & tbl, const std::vector<SharemindTdbString *> & colIdBatch, std::vector<SharemindTdbValue *> & valueBatch) {
    (void)tbl; (void)colIdBatch; (void)valueBatch;
    return false;
}

bool TdbHdf5Connection::readColumn(const std::string & tbl, const std::vector<SharemindTdbIndex *> & colIdBatch, std::vector<SharemindTdbValue *> & valueBatch) {
    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)) {
        if (!success)
            m_logger.error() << "Failed to read column(s) in table \"" << tbl << "\".";
    } BOOST_SCOPE_EXIT_END

    if (colIdBatch.size() > 1) {
        m_logger.error() << "Currently we don't support batched operations.";
        return false;
    }

    if (colIdBatch.empty()) {
        m_logger.error() << "Empty batch of parameters given.";
        return false;
    }

    const SharemindTdbIndex * const colId = colIdBatch.back();

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return false;

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return false;
    }

    // Get table column count
    hsize_t colCount = 0;
    if (!getColumnCount(fileId, colCount))
        return false;

    // Check if column number is valid
    if (colId->idx >= colCount) {
        m_logger.error() << "Column number out of range.";
        return false;
    }

    // Get table row count
    hsize_t rowCount = 0;
    if (!getRowCount(fileId, rowCount))
        return false;

    // Declare a partial column index type
    struct PartialColumnIndex {
        hobj_ref_t dataset_ref;
        hsize_t dataset_column;
    };

    // Get the column meta info
    PartialColumnIndex idx;

    {
        // Create a type for reading the partial index
        const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(PartialColumnIndex));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tId)) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "dataset_ref", HOFFSET(PartialColumnIndex, dataset_ref), H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return false;
        }

        if (H5Tinsert(tId, "dataset_column", HOFFSET(PartialColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return false;
        }

        // Create a simple memory data space
        const hsize_t mDims = 1;
        const hid_t mSId = H5Screate_simple(1, &mDims, NULL);
        if (mSId < 0) {
            m_logger.error() << "Failed to create column meta info memory data space.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&mSId)) {
            if (H5Sclose(mSId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info memory data space.";
        } BOOST_SCOPE_EXIT_END

        // Open the column meta info dataset
        const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to open column meta info dataset.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&dId)) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        } BOOST_SCOPE_EXIT_END

        // Open the column meta info data space
        const hid_t sId = H5Dget_space(dId);
        if (sId < 0) {
            m_logger.error() << "Failed to get column meta info data space.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
        } BOOST_SCOPE_EXIT_END

        // Select a hyperslab in the data space for reading
        const hsize_t start = colId->idx;
        const hsize_t count = 1;
        if (H5Sselect_hyperslab(sId, H5S_SELECT_SET, &start, NULL, &count, NULL) < 0) {
            m_logger.error() << "Failed to do selection in column meta info data space.";
            return false;
        }

        // Read column meta info from the dataset
        if (H5Dread(dId, tId, mSId, sId, H5P_DEFAULT, &idx) < 0) {
            m_logger.error() << "Failed to read column meta info dataset.";
            return false;
        }
    }

    // Read the column
    if (!readColumn(fileId, idx.dataset_ref, idx.dataset_column, valueBatch))
        return false;

    success = true;

    return true;
}

bool TdbHdf5Connection::isVariableLengthType(SharemindTdbType * const type) {
    return !type->size;
}

bool TdbHdf5Connection::pathExists(const fs::path & path, bool & status) {
    try {
        status = exists(path);
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while checking if file " << path << " exists: " << e.what();
        return false;
    }

    return true;
}

bool TdbHdf5Connection::pathIsHdf5(const fs::path & path) {
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

bool TdbHdf5Connection::validateValues(const std::vector<SharemindTdbValue *> & values) const {
    std::vector<SharemindTdbValue *>::const_iterator it;
    for (it = values.begin(); it != values.end(); ++it) {
        SharemindTdbValue * const val = *it;
        assert(val);

        SharemindTdbType * const type = val->type;
        assert(type);

        // Check if the value is non-empty
        if (val->size == 0) {
            m_logger.error() << "Invalid value of type \"" << type->domain
                << "::" << type->name << "\": Value size must be greater than zero.";
            return false;
        }

        // Variable length types are handled differently
        if (isVariableLengthType(type))
            continue;

        // Check if the value buffer length is a multiple of the type size
        // length
        if (val->size % type->size != 0) {
            m_logger.error() << "Invalid value of type \"" << type->domain
                << "::" << type->name << "\": Value size must be a multiple of its type size.";
            return false;
        }
    }

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

bool TdbHdf5Connection::readColumn(const hid_t fileId, const hobj_ref_t ref, const hsize_t col, std::vector<SharemindTdbValue *> & values) {
    // Get dataset from reference
    const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &ref);
    if (oId < 0) {
        m_logger.error() << "Failed to dereference object.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&oId)) {
        if (H5Oclose(oId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset.";
    } BOOST_SCOPE_EXIT_END

    // Check reference object type
    H5O_type_t rType = H5O_TYPE_UNKNOWN;
    if (H5Rget_obj_type(oId, H5R_OBJECT, &ref, &rType) < 0) {
        m_logger.error() << "Failed to get reference object type.";
        return false;
    }

    if (rType != H5O_TYPE_DATASET) {
        m_logger.error() << "Invalid dataset reference object.";
        return false;
    }

    // Get data space
    const hid_t sId = H5Dget_space(oId);
    if (sId < 0) {
        m_logger.error() << "Failed to get dataset data space.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
        if (H5Sclose(sId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset data space.";
    } BOOST_SCOPE_EXIT_END

    // Get data space rank
    const int rank = H5Sget_simple_extent_ndims(sId);
    if (rank < 0) {
        m_logger.error() << "Failed to get dataset data space rank.";
        return false;
    }

    // Check data space rank
    if (rank != 2) {
        m_logger.error() << "Invalid rank for dataset data space.";
        return false;
    }

    // Get size of data space
    hsize_t dims[2];
    if (H5Sget_simple_extent_dims(sId, dims, NULL) < 0) {
        m_logger.error() << "Failed to get dataset data space size.";
        return false;
    }

    // TODO check dims[0] against row count attribute?

    // Check if column offset is in range
    if (col >= dims[1]) {
        m_logger.error() << "Invalid dataset column number: out of range.";
        return false;
    }

    // Open the type attribute
    const hid_t aId = H5Aopen(oId, DATASET_TYPE_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to open dataset type attribute.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aId)) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
    } BOOST_SCOPE_EXIT_END

    // Open type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0) {
        m_logger.error() << "Failed to get dataset type attribute type.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aTId)) {
        if (H5Tclose(aTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
    } BOOST_SCOPE_EXIT_END

    const hid_t aSId = H5Aget_space(aId);
    if (aSId < 0) {
        m_logger.error() << "Failed to get dataset type attribute data space.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aSId)) {
        if (H5Sclose(aSId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute data space.";
    } BOOST_SCOPE_EXIT_END

    // Read the type attribute
    SharemindTdbType * const type = new SharemindTdbType;
    if (H5Aread(aId, aTId, type) < 0) {
        m_logger.error() << "Failed to read dataset type attribute.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aTId)(&aSId)(&type)) {
        if (H5Dvlen_reclaim(aTId, aSId, H5P_DEFAULT, type) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute object.";
        delete type;
    } BOOST_SCOPE_EXIT_END

    // Check if we have anything to read
    if (dims[0] == 0) {
        // TODO check if this is handled correctly
        SharemindTdbValue * const val = SharemindTdbValue_new(type->domain, type->name, type->size, NULL, 0);
        values.push_back(val);
    } else {
        void * buffer = NULL;
        size_type bufferSize = 0;

        // Read the column data
        if (isVariableLengthType(type)) {
            buffer = ::operator new(dims[0] * sizeof(hvl_t));
        } else {
            bufferSize = dims[0] * type->size;
            buffer = ::operator new(bufferSize);
        }

        assert(buffer);

        // Select a hyperslab in the data space to read from
        const hsize_t start[] = { 0, col };
        const hsize_t count[] = { dims[0], 1 };
        if (H5Sselect_hyperslab(sId, H5S_SELECT_SET, start, NULL, count, NULL) < 0) {
            m_logger.error() << "Failed to do selection in dataset data space.";
            return false;
        }

        // Get dataset type
        const hid_t tId = H5Dget_type(oId);
        if (tId < 0) {
            m_logger.error() << "Failed to get dataset type.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&tId)) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up type for column data";
        } BOOST_SCOPE_EXIT_END

        // Create a simple memory data space
        const hsize_t mDims = dims[0];
        const hid_t mSId = H5Screate_simple(1, &mDims, NULL);
        if (mSId < 0) {
            m_logger.error() << "Failed to create memory data space for column data.";
            return false;
        }

        BOOST_SCOPE_EXIT((&m_logger)(&mSId)) {
            if (H5Sclose(mSId) < 0)
                m_logger.fullDebug() << "Error while cleaning up memory data space for column data.";
        } BOOST_SCOPE_EXIT_END

        // Read the dataset data
        if (H5Dread(oId, tId, mSId, sId, H5P_DEFAULT, buffer) < 0) {
            m_logger.error() << "Failed to read the dataset.";
            return false;
        }

        if (isVariableLengthType(type)) {
            hvl_t * hvlBuffer = static_cast<hvl_t *>(buffer);
            for (hsize_t i = 0; i < dims[0]; ++i) {
                SharemindTdbValue * const val = new SharemindTdbValue;
                val->type = SharemindTdbType_new(type->domain, type->name, type->size);
                bufferSize = hvlBuffer[i].len;
                val->buffer = ::operator new(bufferSize);
                memcpy(val->buffer, hvlBuffer[i].p, bufferSize);
                val->size = bufferSize;

                values.push_back(val);
            }

            // Release the memory allocated for the variable length types
            if (H5Dvlen_reclaim(tId, mSId, H5P_DEFAULT, buffer) < 0)
                m_logger.fullDebug() << "Error while cleaning up column data.";

            // Free the variable length type array
            ::operator delete(buffer);
        } else {
            SharemindTdbValue * const val = new SharemindTdbValue;
            val->type = SharemindTdbType_new(type->domain, type->name, type->size);
            val->buffer = buffer;
            val->size = bufferSize;

            values.push_back(val);
        }
    }

    return true;
}

bool TdbHdf5Connection::objRefToType(const hid_t fileId, const hobj_ref_t ref, hid_t & aId, SharemindTdbType & type) {
    // Get the dataset from the reference
    const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &ref);
    if (oId < 0) {
        m_logger.error() << "Failed to dereference object.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&oId)) {
        if (H5Oclose(oId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset.";
    } BOOST_SCOPE_EXIT_END

    // Check the reference object type
    H5O_type_t rType = H5O_TYPE_UNKNOWN;
    if (H5Rget_obj_type(oId, H5R_OBJECT, &ref, &rType) < 0) {
        m_logger.error() << "Failed to get reference object type.";
        return false;
    }

    if (rType != H5O_TYPE_DATASET) {
        m_logger.error() << "Invalid dataset reference object.";
        return false;
    }

    // Open the type attribute
    aId = H5Aopen(oId, DATASET_TYPE_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to open dataset type attribute.";
        return false;
    }

    bool closeAttr = true;

    BOOST_SCOPE_EXIT((&closeAttr)(&m_logger)(&aId)) {
        if (closeAttr && H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
    } BOOST_SCOPE_EXIT_END

    // Open the type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0) {
        m_logger.error() << "Failed to get dataset type attribute type.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aTId)) {
        if (H5Tclose(aTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
    } BOOST_SCOPE_EXIT_END

    // Read the type attribute
    if (H5Aread(aId, aTId, &type) < 0) {
        m_logger.error() << "Failed to read dataset type attribute type.";
        return false;
    }

    closeAttr = false;

    return true;
}

bool TdbHdf5Connection::cleanupType(const hid_t aId, SharemindTdbType & type) {
    // Open the type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0)
        return false;

    BOOST_SCOPE_EXIT((&aTId)) {
        H5Tclose(aTId);
    } BOOST_SCOPE_EXIT_END

    // Open the type attribute data space
    const hid_t aSId = H5Aget_space(aId);
    if (aSId < 0)
        return false;

    BOOST_SCOPE_EXIT((&aSId)) {
        H5Sclose(aSId);
    } BOOST_SCOPE_EXIT_END

    // Release the memory allocated for the vlen types
    if (H5Dvlen_reclaim(aTId, aSId, H5P_DEFAULT, &type) < 0)
        return false;

    return true;
}

bool TdbHdf5Connection::getColumnCount(const hid_t fileId, hsize_t & ncols) {
    // Get dataset
    const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
    if (dId < 0) {
        m_logger.error() << "Failed to open column meta info dataset.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&dId)) {
        if (H5Dclose(dId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
    } BOOST_SCOPE_EXIT_END

    // Get data space
    const hid_t sId = H5Dget_space(dId);
    if (sId < 0) {
        m_logger.error() << "Failed to open column meta info data space.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&sId)) {
        if (H5Sclose(sId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
    } BOOST_SCOPE_EXIT_END

    // Get data space rank
    const int rank = H5Sget_simple_extent_ndims(sId);
    if (rank < 0) {
        m_logger.error() << "Failed to get column meta info data space rank.";
        return false;
    }

    // Check data space rank
    if (rank != 1) {
        m_logger.error() << "Invalid rank for column meta info data space.";
        return false;
    }

    // Get size of data space
    if (H5Sget_simple_extent_dims(sId, &ncols, NULL) < 0) {
        m_logger.error() << "Failed to get column count from column meta info.";
        return false;
    }

    return true;
}

bool TdbHdf5Connection::getRowCount(const hid_t fileId, hsize_t & nrows) {
    // Open meta info group
    const hid_t gId = H5Gopen(fileId, META_GROUP, H5P_DEFAULT);
    if (gId < 0) {
        m_logger.error() << "Failed to get row count: Failed to open meta info group.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&gId)) {
        if (H5Gclose(gId) < 0)
            m_logger.fullDebug() << "Error while cleaning up meta info group.";
    } BOOST_SCOPE_EXIT_END

    // Open the row count attribute
    const hid_t aId = H5Aopen(gId, ROW_COUNT_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to get row count: Failed to open row meta info attribute.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aId)) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up row meta info.";
    } BOOST_SCOPE_EXIT_END

    if (H5Aread(aId, H5T_NATIVE_HSIZE, &nrows) < 0) {
        m_logger.error() << "Failed to get row count: Failed to read row meta info attribute.";
        return false;
    }

    return true;
}

bool TdbHdf5Connection::setRowCount(const hid_t fileId, const hsize_t nrows) {
    // Open meta info group
    const hid_t gId = H5Gopen(fileId, META_GROUP, H5P_DEFAULT);
    if (gId < 0) {
        m_logger.error() << "Failed to set row count: Failed to open meta info group.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&gId)) {
        if (H5Gclose(gId) < 0)
            m_logger.fullDebug() << "Error while cleaning up meta info group.";
    } BOOST_SCOPE_EXIT_END

    // Open the row count attribute
    const hid_t aId = H5Aopen(gId, ROW_COUNT_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to set row count: Failed to open row meta info attribute.";
        return false;
    }

    BOOST_SCOPE_EXIT((&m_logger)(&aId)) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up row meta info.";
    } BOOST_SCOPE_EXIT_END

    // Write the new row count
    if (H5Awrite(aId, H5T_NATIVE_HSIZE, &nrows) < 0) {
        m_logger.error() << "Failed to set row count: Failed to write row count attribute.";
        return false;
    }

    return true;
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
        return H5I_INVALID_HID;

    // TODO Check rollback journal and do rollback, if necessary.

    // TODO Do some sanity checks when opening the table (if all the datasets
    // have the same number of rows).

    m_tableFiles.insert(TableFileMap::value_type(tbl, id));

    return id;
}

} /* namespace sharemind { */
