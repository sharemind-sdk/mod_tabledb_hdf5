/*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5Connection.h"

#include <boost/filesystem.hpp>
#include <boost/scope_exit.hpp>
#include <H5Fpublic.h>
#include <H5Ppublic.h>
#include <H5Spublic.h>
#include <H5Tpublic.h>
#include <sharemind/common/Logger/Debug.h>
#include <sharemind/common/Logger/ILogger.h>

#define COL_INDEX_DATASET "/meta/column_index"
#define ROW_INDEX_DATASET "/meta/row_index"


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
static const size_t TYPE_NAME_STR_SIZE  = 64;
static const size_t CHUNK_SIZE          = 4096;
static const size_t COL_ID_STR_SIZE     = 32;
static const size_t ROW_ID_STR_SIZE     = 16;
static const char * HDF5_EXT            = "h5";
} /* namespace { */

namespace sharemind {

BOOST_STATIC_ASSERT(sizeof(TdbHdf5Connection::size_type) >= sizeof(hsize_t));

struct ColumnIndex {
    char name[COL_ID_STR_SIZE];
    char dataset_name[TYPE_NAME_STR_SIZE];
    hsize_t dataset_column;
};

TdbHdf5Connection::TdbHdf5Connection(ILogger & logger, const fs::path & path)
    : m_logger(logger.wrap("[TdbHdf5Connection] "))
    , m_path(path)
{
    // TODO Check the given path.
    // TODO Register some observers for the path?
    // TODO Register custom log handler.
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
    (void) tbl; (void) names; (void) types;
    /*
    if (!validateTableName(tbl))
        return false;

    // Check the table name for some special symbols e.g. '/'
    // TODO check if table name is valid with some static regular expressions?

    // Check column names
    // TODO

    // Set the cleanup flag
    bool success = false;

    fs::path tblPath = nameToPath(tbl);

    // Create a new file handle
    // H5F_ACC_EXCL - Fail if file already exists.
    hid_t fileId = H5Fcreate(tblPath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
    if (fileId < 0) {
        m_logger.error() << "Failed to create table \"" << tbl << "\" file with path \"" << tblPath << "\".";
        return false;
    }

    // Set cleanup handler for the file
    BOOST_SCOPE_EXIT((&success)(&m_logger)(&tbl)(&tblPath)(&fileId)) {
        // TODO remove logging here?
        if (!success) {
            // Close the file
            if (H5Fclose(fileId) < 0)
                m_logger.warning() << "Error while closing table \"" << tbl << "\" file.";

            // Delete the file
            try {
                fs::remove(tblPath);
            } catch (const fs::filesystem_error & e) {
                m_logger.warning() << "Error while removing table \"" << tbl << "\" file: " << e.what();
            }
        }
    } BOOST_SCOPE_EXIT_END

    // Check the provided types
    std::vector<std::pair<std::string, size_type> > colIdxMap;
    std::map<SharemindTdbType *, size_t> typeMap;

    {
        std::vector<SharemindTdbType *>::const_iterator it;
        for (it = types.begin(); it != types.end(); ++it) {
            std::pair<std::map<SharemindTdbType *, size_t>::iterator, bool> rv = typeMap.insert(std::make_pair(*it, 1));
            if (!rv.second) {
                if (rv.first->first->size != (*it)->size) {
                    m_logger.error() << "Inconsistent type data given for type \"" << (*it)->domain << "::" << (*it)->name << "\".";
                    return false;
                }
                ++rv.first->second;
            }
        }
    }

    const size_t ntypes = typeMap.size();

    // TODO handle string type
    // TODO set type domain

    // Create the corresponding HDF5 types
    std::vector<hid_t> memTypes;
    memTypes.reserve(ntypes);

    std::vector<size_t> colSizes;
    colSizes.reserve(ntypes);

    BOOST_SCOPE_EXIT((&memTypes)) {
        std::vector<hid_t>::const_iterator it;
        for (it = memTypes.begin(); it != memTypes.end(); ++it)
            H5Tclose(*it); // TODO log warning on failure?
        memTypes.clear();
    } BOOST_SCOPE_EXIT_END

    {
        std::map<SharemindTdbType, size_t>::const_iterator it;
        for (it = typeMap.begin(); it != typeMap.end(); ++it) {
            hid_t tId = H5Tcreate(H5T_OPAQUE, it->first.size);
            if (tId < 0)
                return false;
            if (H5Tset_tag(tId, it->first.name) < 0) {
                if (H5Tclose(tId) < 0)
                    m_logger.warning() << ""; // TODO
                return false;
            }
            memTypes.push_back(tId);
            colSizes.push_back(it->second);
        }
    }

    // Create a dataset for each of the HDF5 types
    {
        // Set dataset creation properties
        hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);

        BOOST_SCOPE_EXIT((&plistId)) {
            H5Pclose(plistId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        assert(memTypes.size() == ntypes);
        assert(colSizes.size() == ntypes);

        std::vector<hid_t>::const_iterator typeIt = memTypes.begin();
        std::vector<size_t>::const_iterator sizeIt = colSizes.begin();

        for (size_t i = 0; i < ntypes; ++i, ++typeIt, ++sizeIt) {
            const hid_t & tId = *typeIt;

            // TODO we actually already have this information
            const char * name = H5Tget_tag(tId);
            const size_t size = H5Tget_size(tId);

            // Set chunk size
            // TODO take chunk size from configuration?
            const hsize_t chunkSize = CHUNK_SIZE / size;
            hsize_t dimsChunk[2];
            dimsChunk[0] = chunkSize; dimsChunk[1] = 1; // TODO are vertical chunks OK?
            if (H5Pset_chunk(plistId, 2, dimsChunk) < 0)
                return false;

            // TODO set compression?

            // Set fill data
            char * fillData = static_cast<char *>(calloc(1, size)); // is this ok?

            if (!fillData) {
                m_logger.error() << "Failed to allocate memory for table fill data for type \"" << name << "\".";
                free(fillData);
                return false;
            }
            if (H5Pset_fill_value(plistId, tId, fillData) < 0) {
                m_logger.error() << "Failed to set table fill data for type \"" << name << "\".";
                free(fillData);
                return false;
            }

            free(fillData);

            // Create a simple data space
            hsize_t dims[2];
            dims[0] = 0; dims[1] = *sizeIt;

            const hsize_t maxdims[2] = { H5S_UNLIMITED, H5S_UNLIMITED };

            hid_t sId = H5Screate_simple(2, dims, maxdims);
            if (sId < 0) {
                m_logger.error() << "Failed to create a data space for type \"" << name << "\".";
                return false;
            }

            // Create the dataset
            hid_t dId = H5Dcreate2(fileId, name, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
            if (dId < 0) {
                m_logger.error() << "Failed to create dataset for type \"" << name << "\"";
                if (H5Sclose(sId) < 0)
                    m_logger.warning() << ""; // TODO
                return false;
            }

            // Close the data space and the dataset
            if (H5Sclose(sId) < 0)
                m_logger.warning() << ""; // TODO

            if (H5Dclose(dId) < 0)
                m_logger.warning() << ""; // TODO
        }
    }

    // Create the column meta data
    {
        // Create the column index data type
        // struct ColumnIndex {
        //     char name[COL_ID_STR_SIZE];
        //     char dataset_name[TYPE_NAME_STR_SIZE];
        //     hsize_t dataset_column;
        // };

        assert(names.size() == types.size());
        const size_t size = names.size();

        hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(ColumnIndex));
        if (tId < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&tId)) {
            H5Tclose(tId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // char name[COL_ID_STR_SIZE];
        hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, COL_ID_STR_SIZE) < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.warning() << ""; // TODO
            return false;
        }

        BOOST_SCOPE_EXIT((&nameTId)) {
            H5Tclose(nameTId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "name", HOFFSET(ColumnIndex, name), nameTId) < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            return false;
        }

        // char dataset_name[TYPE_NAME_STR_SIZE];
        hid_t dsNameTId = H5Tcopy(H5T_C_S1);
        if (dsNameTId < 0 || H5Tset_size(dsNameTId, TYPE_NAME_STR_SIZE) < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            if (dsNameTId >= 0 && H5Tclose(dsNameTId) < 0)
                m_logger.warning() << ""; // TODO
            return false;
        }

        BOOST_SCOPE_EXIT((&dsNameTId)) {
            H5Tclose(dsNameTId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        if (H5Tinsert(tId, "dataset_name", HOFFSET(ColumnIndex, dataset_name), dsNameTId) < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            return false;
        }

        // size_t dataset_column;
        if (H5Tinsert(tId, "dataset_column", HOFFSET(ColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create the HDF5 data type for column meta info.";
            return false;
        }

        // Create the data space
        const hsize_t dims = size;
        const hsize_t maxdims = H5S_UNLIMITED;
        hid_t sId = H5Screate_simple(1, &dims, &maxdims);
        if (sId < 0) {
            m_logger.error() << "Failed to create data space creation property list for column meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&sId)) {
            H5Sclose(sId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Create the dataset creation property list
        hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);
        if (plistId < 0) {
            m_logger.error() << "Failed to create dataset creation property list for column meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&plistId)) {
            H5Pclose(plistId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        const hsize_t dimsChunk = CHUNK_SIZE / sizeof(ColumnIndex);
        if (H5Pset_chunk(plistId, 1, &dimsChunk) < 0) {
            m_logger.error() << "Failed to set dataset creation property list for column meta info.";
            return false;
        }

        if (H5Pset_create_intermediate_group(plistId, 1) < 0) {
            m_logger.error() << "Failed to set dataset creation property list for column meta info.";
            return false;
        }

        // Create the dataset
        hid_t dId = H5Dcreate2(fileId, COL_INDEX_DATASET, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to create dataset for column meta info.";
            return false;
        }

        // Write column index data
        if (size > 0) {
            // Serialize the column index data
            ColumnIndex colIdx[size];

            std::vector<SharemindTdbString *>::const_iterator nameIt = names.begin();
            std::vector<SharemindTdbType *>::const_iterator typeIt = types.begin();

            for (size_t i = 0; i < size; ++i, ++nameIt, ++typeIt) {
                const size_t nameSize = strlen(nameIt->str);
                const size_t tNameSize = strlen(typeIt->name);
                assert(nameSize < COL_ID_STR_SIZE);
                memcpy(colIdx[i].name, nameIt->str, nameSize + 1);
                memcpy(colIdx[i].dataset_name, typeIt->name, tNameSize + 1);
                colIdx[i].dataset_column = i;
            }

            // Write the column index data
            if (H5Dwrite(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, colIdx) < 0) {
                m_logger.error() << "Failed to write dataset for column meta info.";
                return false;
            }
        }

        if (H5Dclose(dId) < 0)
            m_logger.warning() << ""; // TODO
    }

    // Create the row meta data
    {
        // Create the row index data type
        // TODO Actually we would like a 128 bit unsigned integer here
        hid_t tId = H5Tcopy(H5T_C_S1);
        if (tId < 0 || H5Tset_size(tId, ROW_ID_STR_SIZE) < 0) { // TODO Should we use variable length strings instead?
            m_logger.error() << "Failed to create the HDF5 data type for row meta info.";
            if (tId >= 0 && H5Tclose(tId) < 0)
                m_logger.warning() << ""; // TODO
            return false;
        }

        BOOST_SCOPE_EXIT((&tId)) {
            H5Tclose(tId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Create the data space
        const hsize_t dims = 0;
        const hsize_t maxdims = H5S_UNLIMITED;
        hid_t sId = H5Screate_simple(1, &dims, &maxdims);
        if (sId < 0) {
            m_logger.error() << "Failed to create data space creation property list for row meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&sId)) {
            H5Sclose(sId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Create the dataset creation property list
        hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);
        if (plistId < 0) {
            m_logger.error() << "Failed to create dataset creation property list for row meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&plistId)) {
            H5Pclose(plistId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        const hsize_t dimsChunk = CHUNK_SIZE / ROW_ID_STR_SIZE;
        if (H5Pset_chunk(plistId, 1, &dimsChunk) < 0) {
            m_logger.error() << "Failed to set dataset creation property list for column meta info.";
            return false;
        }

        if (H5Pset_create_intermediate_group(plistId, 1) < 0) {
            m_logger.error() << "Failed to set dataset creation property list for row meta info.";
            return false;
        }

        // Create the dataset
        hid_t dId = H5Dcreate2(fileId, ROW_INDEX_DATASET, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to create dataset for row meta info.";
            return false;
        }

        if (H5Dclose(dId) < 0)
            m_logger.warning() << ""; // TODO
    }

    success = true;
    */

    return true;
}

bool TdbHdf5Connection::tblDelete(const std::string & tbl) {
    (void) tbl;
    return false;
}

bool TdbHdf5Connection::tblExists(const std::string & tbl, bool & status) {
    if (!validateTableName(tbl))
        return false;

    // Check if the file exists and has the right format
    const fs::path dbPath = nameToPath(tbl);
    htri_t isHdf5 = H5Fis_hdf5(dbPath.c_str());
    if (isHdf5 < 0) {
        m_logger.error() << "Error while checking if table \"" << tbl << "\" exists.";
        return false;
    }

    status = isHdf5 > 0;

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

    // Read the column index data
    {
        // Get dataset
        hid_t dId = H5Dopen2(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to open dataset for column meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&dId)) {
            H5Dclose(dId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Get data space
        hid_t sId = H5Dget_space(dId);
        if (sId < 0) {
            m_logger.error() << "Failed to open data space for column meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&sId)) {
            H5Sclose(sId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Get size of data space
        hsize_t dims;
        if (H5Sget_simple_extent_dims(sId, &dims, NULL) < 0) {
            m_logger.error() << "Failed to get column count from column meta info.";
            return false;
        }

        cols = dims;
    }

    // Read the row index data
    {
        // Get dataset
        hid_t dId = H5Dopen2(fileId, ROW_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to open dataset for row meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&dId)) {
            H5Dclose(dId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Get data space
        hid_t sId = H5Dget_space(dId);
        if (sId < 0) {
            m_logger.error() << "Failed to open data space for row meta info.";
            return false;
        }

        BOOST_SCOPE_EXIT((&sId)) {
            H5Sclose(sId); // TODO log warning on failure?
        } BOOST_SCOPE_EXIT_END

        // Get size of data space
        hsize_t dims;
        if (H5Sget_simple_extent_dims(sId, &dims, NULL) < 0) {
            m_logger.error() << "Failed to get row count from row meta info.";
            return false;
        }

        rows = dims;
    }

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

bool TdbHdf5Connection::insertRow(const std::string & tbl, const std::pair<uint64_t, uint64_t> & rowId, std::vector<SharemindTdbValue *> & vals) {
    (void) rowId; (void) vals;
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

    // Write row data

    return true;
}

bool TdbHdf5Connection::validateTableName(const std::string & tbl) const {
    if (tbl.empty()) {
        m_logger.error() << "Table name must be a non-empty string.";
        return false;
    }

    return true;
}

boost::filesystem::path TdbHdf5Connection::nameToPath(const std::string & tbl) const {
    assert(!tbl.empty());

    // TODO Should we try to eliminate the copying?
    fs::path p(m_path);
    p /= tbl;
    p.replace_extension(HDF5_EXT);

    return p;
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
