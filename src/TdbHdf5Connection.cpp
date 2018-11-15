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

#include "TdbHdf5Connection.h"

#include <algorithm>
#include <set>
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
#include <memory>
#include <sharemind/mod_tabledb/TdbTypesUtil.h>
#include <type_traits>


namespace fs = boost::filesystem;

#define COL_INDEX_DATASET      "/meta/column_index"
#define COL_INDEX_TYPE         "/meta/column_index_type"
#define COL_NAME_SIZE_MAX      (64u)
#define CHUNK_SIZE             (static_cast<size_t>(4096u))
#define DATASET_TYPE_ATTR      "type"
#define DATASET_TYPE_ATTR_TYPE "/meta/dataset_type"
#define ERR_MSG_SIZE_MAX       (64u)
#define FILE_EXT               ".h5"
#define META_GROUP             "/meta"
#define ROW_COUNT_ATTR         "row_count"
#define TBL_NAME_SIZE_MAX      (64u)

extern "C" {

static herr_t err_walk_cb(unsigned n, const H5E_error_t * err_desc, void * client_data) {
    assert(err_desc);
    assert(client_data);

    const LogHard::Logger & logger =
            *static_cast<const LogHard::Logger *>(client_data);

    char maj_msg[ERR_MSG_SIZE_MAX];
    if (H5Eget_msg(err_desc->maj_num, nullptr, maj_msg, ERR_MSG_SIZE_MAX) < 0)
        return -1;

    char min_msg[ERR_MSG_SIZE_MAX];
    if (H5Eget_msg(err_desc->min_num, nullptr, min_msg, ERR_MSG_SIZE_MAX) < 0)
        return -1;

    logger.fullDebug() << "HDF5 Error[" << n << "]:" << err_desc->func_name
                       << " - " << maj_msg << ": " << min_msg;

    return 0;
}

static herr_t err_handler(hid_t, void * client_data) {
    // Have to make a copy of the stack here. Otherwise HDF5 resets the stack at
    // some point.
    const hid_t estack = H5Eget_current_stack();
    if (estack < 0)
        return -1;
    const herr_t rv = H5Ewalk(estack, H5E_WALK_DOWNWARD, err_walk_cb, client_data);
    H5Eclose_stack(estack);
    return rv;
}

} /* extern "C" { */

namespace {

/*
 * http://en.wikipedia.org/wiki/In-place_matrix_transposition
 * http://stackoverflow.com/questions/9227747/in-place-transposition-of-a-matrix
 */
void transposeBlock(char * const first,
        char * const last,
        const size_t m,
        const size_t vsize)
{
    assert(m > 0u);
    assert(vsize > 0u);
    assert(last >= first);
    static auto const unsignedDistance =
            [](decltype(first) const f, decltype(last) const l) noexcept
                    -> std::make_unsigned<decltype(l - f)>::type
            {
                return static_cast<std::make_unsigned<decltype(l - f)>::type>(
                            l - f);
            };
    auto const size = unsignedDistance(first, last);
    assert(size % m == 0u);
    assert(size % vsize == 0u);

    const size_t mn1 = size / vsize - 1u;
    const size_t n = size / (m * vsize);
    assert(n > 0u);
    std::vector<bool> visited(n * m);
    char * cycle = first;
    while (cycle + vsize < last) {
        cycle += vsize;
        auto const visitedSize = unsignedDistance(first, cycle);
        if (visited[visitedSize / vsize])
            continue;
        size_t a = visitedSize / vsize;
        do {
            a = a == mn1 ? mn1 : (n * a) % mn1;
            std::swap_ranges(first + a * vsize, first + a * vsize + vsize, cycle);
            visited[a] = true;
        } while ((first + a * vsize) != cycle);
    }
}

struct SharemindTdbStringLess {
    bool operator() (SharemindTdbString const * const lhs,
                     SharemindTdbString const * const rhs) const
    { return strcmp(lhs->str, rhs->str) < 0; }
};

struct SharemindTdbTypeLess {
    bool operator() (SharemindTdbType const * const lhs,
                     SharemindTdbType const * const rhs) const
    {
        int cmp = 0;
        return !(cmp = strcmp(lhs->domain, rhs->domain))
                && !(cmp = strcmp(lhs->name, rhs->name))
                ? lhs->size < rhs->size
                : cmp < 0;
    }
};

inline bool isVariableLengthType(SharemindTdbType const * const type)
{ return !type->size; }

bool cleanupType(hid_t const aId, SharemindTdbType & type) {
    // Open the type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0)
        return false;

    BOOST_SCOPE_EXIT_ALL(aTId) {
        H5Tclose(aTId);
    };

    // Open the type attribute data space
    const hid_t aSId = H5Aget_space(aId);
    if (aSId < 0)
        return false;

    BOOST_SCOPE_EXIT_ALL(aSId) {
        H5Sclose(aSId);
    };

    // Release the memory allocated for the vlen types
    if (H5Dvlen_reclaim(aTId, aSId, H5P_DEFAULT, &type) < 0)
        return false;

    return true;
}

} /* namespace { */

namespace sharemind {

SHAREMIND_DEFINE_EXCEPTION_NOINLINE(sharemind::Exception,
                                    TdbHdf5Connection::,
                                    Exception);
SHAREMIND_DEFINE_EXCEPTION_NOINLINE(Exception,
                                    TdbHdf5Connection::,
                                    InitializationException);
SHAREMIND_DEFINE_EXCEPTION_CONST_MSG_NOINLINE(
        InitializationException,
        TdbHdf5Connection::,
        FailedToSetHdf5LoggingHandlerException,
        "Failed to set HDF5 logging handler.");

BOOST_STATIC_ASSERT(sizeof(TdbHdf5Connection::size_type) == sizeof(hsize_t));

TdbHdf5Connection::TdbHdf5Connection(const LogHard::Logger & logger,
                                     const fs::path & path)
    : m_logger(logger, "[TdbHdf5Connection]")
    , m_path(path)
{
    // TODO Needs some refactoring. It is getting unreadable.

    // TODO need two kinds of rollback:
    // 1) Per operation rollback stack - so that the operation leaves the
    // database in a consistent state.
    // 2) Rollback function for the last database operation. When we succeed,
    // but other miners fail.

    // TODO Check the given path.
    // TODO Some stuff probably should be allocated on the heap.
    // TODO locking?
    // TODO check for uniqueness of column names
    // TODO Need to refactor the huge functions into smaller ones.

    // Register a custom log handler
    if (H5Eset_auto(H5E_DEFAULT,
                    err_handler,
                    &const_cast<LogHard::Logger &>(m_logger)) < 0)
    {
        m_logger.error() << "Failed to set HDF5 logging handler.";
        throw FailedToSetHdf5LoggingHandlerException();
    }
}

TdbHdf5Connection::~TdbHdf5Connection() {
    for (auto & vp : m_tableFiles) {
        if (H5Fclose(vp.second) < 0)
            m_logger.warning() << "Error while closing handle to table file \""
                               << nameToPath(vp.first).string() << "\".";
    }

    m_tableFiles.clear();
}

SharemindTdbError TdbHdf5Connection::tblNames(std::vector<SharemindTdbString *> & names) {
    try {
        namespace fs = boost::filesystem;
        assert(names.empty());
        fs::directory_iterator it(m_path);
        while (it != fs::directory_iterator()) {
            fs::path filepath(it->path());
            if (filepath.extension().string().compare(FILE_EXT) == 0) {
                auto const stemString(filepath.stem().string());
                auto * const str =
                        SharemindTdbString_new2(stemString.c_str(),
                                                stemString.size());
                try {
                    names.emplace_back(str);
                } catch (...) {
                    SharemindTdbString_delete(str);
                    throw;
                }
            }
            ++it;
        }
    } catch (...) {
        for (auto * const name : names)
            SharemindTdbString_delete(name);
        throw;
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblCreate(const std::string & tbl,
        const std::vector<SharemindTdbString *> & names,
        const std::vector<SharemindTdbType *> & types)
{
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.fullDebug() << "Failed to create table \"" << tbl << "\".";
    };

    // Do some simple checks on the parameters
    if (names.empty()) {
        m_logger.error() << "No column names given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    if (types.empty()) {
        m_logger.error() << "No column types given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    if (names.size() != types.size()) {
        m_logger.error() << "Differing number of column names and column types.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check column names
    if (!validateColumnNames(names))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check for duplicate column names
    {
        std::set<SharemindTdbString const *, SharemindTdbStringLess> namesSet;
        for (SharemindTdbString const * const name : names) {
            if (!namesSet.emplace(name).second) {
                m_logger.error() << "Given column names must be unique.";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }
        }
    }

    fs::path tblPath = nameToPath(tbl);

    // Check if table file exists
    bool exists = false;
    if (!pathExists(tblPath, exists))
        return SHAREMIND_TDB_GENERAL_ERROR;

    if (exists) {
        m_logger.error() << "Table already exists.";
        return SHAREMIND_TDB_TABLE_ALREADY_EXISTS;
    }

    // Remove dangling file handler, if any (file was unlinked while the handler
    // was open).
    closeTableFile(tbl);

    // Create a new file handle
    // H5F_ACC_EXCL - Fail if file already exists.
    const hid_t fileId = H5Fcreate(tblPath.c_str(), H5F_ACC_EXCL, H5P_DEFAULT, H5P_DEFAULT);
    if (fileId < 0) {
        m_logger.error() << "Failed to create table file with path "
                         << tblPath.string() << '.';
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Set cleanup handler for the file
    BOOST_SCOPE_EXIT_ALL(&success, this, &tblPath, fileId) {
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
    };

    // Check the provided types
    typedef std::vector<std::pair<std::string, size_type> > ColInfoVector;
    ColInfoVector colInfoVector;
    typedef std::map<SharemindTdbType *, size_t, SharemindTdbTypeLess> TypeMap;
    TypeMap typeMap;

    for (SharemindTdbType * const type : types) {
        auto rv(typeMap.emplace(type, 1));
        if (!rv.second)
            ++rv.first->second;

        std::ostringstream oss; // TODO figure out something better than this
        oss << type->domain << "::" << type->name << "::" << type->size;
        colInfoVector.emplace_back(oss.str(), rv.first->second - 1);
    }

    const size_t ntypes = typeMap.size();

    // Create the corresponding HDF5 types
    std::vector<std::pair<SharemindTdbType *, hid_t> > memTypes;
    memTypes.reserve(ntypes);

    std::vector<size_t> colSizes;
    colSizes.reserve(ntypes);

    // Set cleanup handler for the opened HDF5 types
    BOOST_SCOPE_EXIT_ALL(this, &memTypes) {
        for (auto const & vp : memTypes)
            if (H5Tclose(vp.second) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type.";
        memTypes.clear();
    };

    {
        for (auto const & vp : typeMap) {
            SharemindTdbType * const type = vp.first;

            hid_t tId = H5I_INVALID_HID;

            if (isVariableLengthType(type)) {
                // Create a variable length type
                tId = H5Tvlen_create(H5T_NATIVE_CHAR);
                if (tId < 0) {
                    m_logger.error() << "Failed to create dataset type.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }
            } else {
                // Create a fixed length opaque type
                tId = H5Tcreate(H5T_OPAQUE, type->size);
                if (tId < 0) {
                    m_logger.error() << "Failed to create dataset type.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }

                // Set a type tag
                std::ostringstream oss; // TODO figure out something better than this
                oss << type->domain << "::" << type->name << "::" << type->size;
                const std::string tag(oss.str());

                if (H5Tset_tag(tId, tag.c_str()) < 0) {
                    m_logger.error() << "Failed to set dataset type tag.";

                    if (H5Tclose(tId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up dataset type.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }
            }

            assert(tId >= 0);
            memTypes.push_back(std::make_pair(type, tId));
            colSizes.push_back(vp.second);
        }
    }

    // Create some meta info objects
    {
        // Create a meta data group
        const hid_t gId = H5Gcreate(fileId, META_GROUP, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if (gId < 0) {
            m_logger.error() << "Failed to create meta info group.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, gId) {
            if (H5Gclose(gId) < 0)
                m_logger.fullDebug() << "Error while cleaning up meta info group.";
        };

        // Create a data space for the row count attribute
        const hsize_t aDims = 1;
        const hid_t aSId = H5Screate_simple(1, &aDims, nullptr);
        if (aSId < 0) {
            m_logger.error() << "Failed to create row count attribute data space.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Set cleanup handler for the row count attribute data space
        BOOST_SCOPE_EXIT_ALL(this, aSId) {
            if (H5Sclose(aSId) < 0)
                m_logger.fullDebug() << "Error while cleaning up row count attribute data space.";
        };

        // Add a type attribute to the dataset
        const hid_t aId = H5Acreate(gId, ROW_COUNT_ATTR, H5T_NATIVE_HSIZE, aSId, H5P_DEFAULT, H5P_DEFAULT);
        if (aId < 0) {
            m_logger.error() << "Failed to create row count attribute.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Set cleanup handler for the attribute
        BOOST_SCOPE_EXIT_ALL(this, aId) {
            if (H5Aclose(aId) < 0)
                m_logger.fullDebug() << "Error while cleaning up row count attribute.";
        };

        // Write the type attribute
        const hsize_t rowCount = 0;
        if (H5Awrite(aId, H5T_NATIVE_HSIZE, &rowCount) < 0) {
            m_logger.error() << "Failed to write row count attribute.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }
    }

    // Create a dataset for each unique column type
    {
        // Set dataset creation properties
        const hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);

        BOOST_SCOPE_EXIT_ALL(this, plistId) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset creation property list.";
        };

        // Create the type attribute data type
        const hid_t aTId = H5Tcreate(H5T_COMPOUND, sizeof(SharemindTdbType));
        if (aTId < 0) {
            m_logger.error() << "Failed to create dataset type attribute type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, aTId) {
            if (H5Tclose(aTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        };

        // const char * domain
        const hid_t domainTId = H5Tcopy(H5T_C_S1);
        if (domainTId < 0 || H5Tset_size(domainTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";

            if (domainTId >= 0 && H5Tclose(domainTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, domainTId) {
            if (H5Tclose(domainTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        };

        if (H5Tinsert(aTId, "domain", HOFFSET(SharemindTdbType, domain), domainTId) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // const char * name
        const hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";

            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, nameTId) {
            if (H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
        };

        if (H5Tinsert(aTId, "name", HOFFSET(SharemindTdbType, name), nameTId) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // hsize_t size
        if (H5Tinsert(aTId, "size", HOFFSET(SharemindTdbType, size), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create dataset type attribute data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Commit the dataset type attribute type
        if (H5Tcommit(fileId, DATASET_TYPE_ATTR_TYPE, aTId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            m_logger.error() << "Failed to commit dataset type attribute type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        assert(memTypes.size() == ntypes);
        assert(colSizes.size() == ntypes);

        auto typeIt(memTypes.cbegin());
        auto sizeIt(colSizes.cbegin());

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
            const hsize_t chunkSize = std::max(CHUNK_SIZE / size, static_cast<size_t>(1u));
            hsize_t dimsChunk[2];
            dimsChunk[0] = chunkSize; dimsChunk[1] = 1; // TODO are vertical chunks OK?
            if (H5Pset_chunk(plistId, 2, dimsChunk) < 0)
                return SHAREMIND_TDB_GENERAL_ERROR;

            // TODO set compression? Probably only useful for some public types
            // (variable length strings cannot be compressed as far as I know).

            // Create a simple two dimensional data space
            hsize_t dims[2];
            dims[0] = 0; dims[1] = *sizeIt;

            const hsize_t maxdims[2] = { H5S_UNLIMITED, H5S_UNLIMITED };

            const hid_t sId = H5Screate_simple(2, dims, maxdims);
            if (sId < 0) {
                m_logger.error() << "Failed to create a data space type \"" << tag << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Set cleanup handler for the data space
            BOOST_SCOPE_EXIT_ALL(this, sId) {
                if (H5Sclose(sId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up data space.";
            };

            // Create the dataset
            const hid_t dId = H5Dcreate(fileId, tag.c_str(), tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
            if (dId < 0) {
                m_logger.error() << "Failed to create dataset type \"" << tag << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Set cleanup handler for the dataset
            BOOST_SCOPE_EXIT_ALL(this, dId) {
                if (H5Dclose(dId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset.";
            };

            // Create a data space for the type attribute
            const hsize_t aDims = 1;
            const hid_t aSId = H5Screate_simple(1, &aDims, nullptr);
            if (aSId < 0) {
                m_logger.error() << "Failed to create dataset type attribute data space.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Set cleanup handler for the type attribute data space
            BOOST_SCOPE_EXIT_ALL(this, aSId) {
                if (H5Sclose(aSId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute data space.";
            };

            // Add a type attribute to the dataset
            const hid_t aId = H5Acreate(dId, DATASET_TYPE_ATTR, aTId, aSId, H5P_DEFAULT, H5P_DEFAULT);
            if (aId < 0) {
                m_logger.error() << "Failed to create dataset type attribute.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Set cleanup handler for the attribute
            BOOST_SCOPE_EXIT_ALL(this, aId) {
                if (H5Aclose(aId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
            };

            // Write the type attribute
            if (H5Awrite(aId, aTId, type) < 0) {
                m_logger.error() << "Failed to write dataset type attribute.";
                return SHAREMIND_TDB_IO_ERROR;
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
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, tId) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        };

        // const char * name
        const hid_t nameTId = H5Tcopy(H5T_C_S1);
        if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";

            if (nameTId >= 0 && H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, nameTId) {
            if (H5Tclose(nameTId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        };

        if (H5Tinsert(tId, "name", HOFFSET(ColumnIndex, name), nameTId) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // hobj_ref_t dataset_ref
        if (H5Tinsert(tId, "dataset_ref", HOFFSET(ColumnIndex, dataset_ref), H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // hsize_t dataset_column
        if (H5Tinsert(tId, "dataset_column", HOFFSET(ColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create column meta info data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Create the 1 dimensional data space
        const hsize_t dims = size;
        const hsize_t maxdims = H5S_UNLIMITED;
        const hid_t sId = H5Screate_simple(1, &dims, &maxdims);
        if (sId < 0) {
            m_logger.error() << "Failed to create column meta info data space creation property list.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, sId) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
        };

        // Commit the column meta info data type
        if (H5Tcommit(fileId, COL_INDEX_TYPE, tId, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) < 0) {
            m_logger.error() << "Failed to commit column meta info data type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Create the dataset creation property list
        const hid_t plistId = H5Pcreate(H5P_DATASET_CREATE);
        if (plistId < 0) {
            m_logger.error() << "Failed to create column meta info dataset creation property list.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, plistId) {
            if (H5Pclose(plistId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset creation property list.";
        };

        const hsize_t dimsChunk = CHUNK_SIZE / (sizeof(hobj_ref_t) + sizeof(hvl_t) + sizeof(size_type));
        if (H5Pset_chunk(plistId, 1, &dimsChunk) < 0) {
            m_logger.error() << "Failed to set column meta info dataset creation property list info.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Create the dataset
        const hid_t dId = H5Dcreate(fileId, COL_INDEX_DATASET, tId, sId, H5P_DEFAULT, plistId, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to create column meta info dataset.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, dId) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        };

        // Write column index data
        if (size > 0) {
            // Serialize the column index data
            auto const colIdx(std::make_unique<ColumnIndex[]>(size));

            auto nameIt(names.cbegin());
            auto mIt(colInfoVector.cbegin());

            for (size_t i = 0; i < size; ++i, ++nameIt, ++mIt) {
                colIdx[i].name = (*nameIt)->str;
                colIdx[i].dataset_column = mIt->second;

                if (H5Rcreate(&colIdx[i].dataset_ref, fileId, mIt->first.c_str(), H5R_OBJECT, -1) < 0) {
                    m_logger.error() << "Failed to create column meta info type reference.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }
            }

            // Write the column index data
            if (H5Dwrite(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, colIdx.get()) < 0) {
                m_logger.error() << "Failed to write column meta info dataset.";
                return SHAREMIND_TDB_IO_ERROR;
            }
        }
    }

    // Flush the buffers to reduce the chance of file corruption
    if (H5Fflush(fileId, H5F_SCOPE_LOCAL) < 0)
        m_logger.fullDebug() << "Error while flushing buffers.";

    // Add the file handler to the map
    #ifndef NDEBUG
    const bool r =
    #endif
            m_tableFiles.emplace(tbl, fileId)
            #ifndef NDEBUG
                .second
            #endif
            ;
    assert(r);

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblDelete(const std::string & tbl) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Get table path
    const fs::path tblPath = nameToPath(tbl);

    // Delete the table file
    try {
        if (!remove(tblPath)) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while deleting table \"" << tbl << "\" file "
                         << tblPath.string() << ": " << e.what() << ".";
        return SHAREMIND_TDB_IO_ERROR;
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblExists(const std::string & tbl, bool & status) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Get table path
    const fs::path tblPath = nameToPath(tbl);

    // Check if the file exists
    if (!pathExists(tblPath, status))
        return SHAREMIND_TDB_GENERAL_ERROR;

    // Check if the file has the right format
    if (status) {
        // Check if the file has the right format
        htri_t const isHdf5 = H5Fis_hdf5(tblPath.c_str());
        if (isHdf5 <= 0) {
            if (isHdf5 < 0) {
                m_logger.error() << "Error while checking file \""
                                 << tblPath.string() << "\" format.";
            }
            m_logger.error() << "Table \"" << tbl << "\" file \""
                             << tblPath.string()
                             << "\" is not a valid table file.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblColCount(const std::string & tbl, size_type & count) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to get column count for table \"" << tbl << "\".";
    };

    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Read column meta info
    hsize_t ncols = 0;
    {
        const SharemindTdbError ecode = getColumnCount(fileId, ncols);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    count = ncols;

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblColNames(const std::string & tbl, std::vector<SharemindTdbString *> & names) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to get column names for table \"" << tbl << "\".";
    };

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Get table column count
    hsize_t colCount = 0;
    {
        const SharemindTdbError ecode = getColumnCount(fileId, colCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Declare a partial column index type
    struct PartialColumnIndex {
        char * name;
    };

    // Create a type for reading the partial index
    const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(PartialColumnIndex));
    if (tId < 0) {
        m_logger.error() << "Failed to create column meta info type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, tId) {
        if (H5Tclose(tId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info type.";
    };

    // const char * name
    const hid_t nameTId = H5Tcopy(H5T_C_S1);
    if (nameTId < 0 || H5Tset_size(nameTId, H5T_VARIABLE) < 0) {
        m_logger.error() << "Failed to create column meta info data type.";

        if (nameTId >= 0 && H5Tclose(nameTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, nameTId) {
        if (H5Tclose(nameTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info type.";
    };

    if (H5Tinsert(tId, "name", HOFFSET(ColumnIndex, name), nameTId) < 0) {
        m_logger.error() << "Failed to create column meta info data type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Create a simple memory data space
    const hsize_t mDims = colCount;
    const hid_t mSId = H5Screate_simple(1, &mDims, nullptr);
    if (mSId < 0) {
        m_logger.error() << "Failed to create column meta info memory data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, mSId) {
        if (H5Sclose(mSId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info memory data space.";
    };

    // Open the column meta info dataset
    const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
    if (dId < 0) {
        m_logger.error() << "Failed to open column meta info dataset.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, dId) {
        if (H5Dclose(dId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
    };

    // Allocate a buffer for reading
    auto const buffer(std::make_unique<PartialColumnIndex[]>(colCount));

    // Read column meta info from the dataset
    if (H5Dread(dId, tId, mSId, H5S_ALL, H5P_DEFAULT, buffer.get()) < 0) {
        m_logger.error() << "Failed to read column meta info dataset.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, tId, mSId, &buffer) {
        // Release the memory allocated for the variable length types
        if (H5Dvlen_reclaim(tId, mSId, H5P_DEFAULT, buffer.get()) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta data.";
    };

    // Copy the strings to the return values
    assert(names.empty());
    names.reserve(colCount);

    BOOST_SCOPE_EXIT_ALL(&success, &names) {
        if (!success) {
            for (SharemindTdbString * const name : names)
                SharemindTdbString_delete(name);
            names.clear();
        }
    };

    for (hsize_t i = 0; i < colCount; ++i) {
        auto str(SharemindTdbString_new(buffer[i].name));
        try {
            names.emplace_back(str);
        } catch (...) {
            SharemindTdbString_delete(str);
            throw;
        }
    }

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblColTypes(const std::string & tbl, std::vector<SharemindTdbType *> & types) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to get column types for table \"" << tbl << "\".";
    };

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Get table column count
    hsize_t colCount = 0;
    {
        const SharemindTdbError ecode = getColumnCount(fileId, colCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Declare a partial column index type
    struct PartialColumnIndex {
        hobj_ref_t dataset_ref;
    };

    // Create a type for reading the partial index
    const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(PartialColumnIndex));
    if (tId < 0) {
        m_logger.error() << "Failed to create column meta info type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, tId) {
        if (H5Tclose(tId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info type.";
    };

    if (H5Tinsert(tId, "dataset_ref", HOFFSET(PartialColumnIndex, dataset_ref), H5T_STD_REF_OBJ) < 0) {
        m_logger.error() << "Failed to create column meta info type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Open the column meta info dataset
    const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
    if (dId < 0) {
        m_logger.error() << "Failed to open column meta info dataset.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, dId) {
        if (H5Dclose(dId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
    };

    // Allocate a buffer for reading
    auto const indices(std::make_unique<PartialColumnIndex[]>(colCount));

    // Read column meta info from the dataset
    if (H5Dread(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, indices.get()) < 0) {
        m_logger.error() << "Failed to read column meta info dataset.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Resolve the dataset references to dataset types
    types.clear();
    types.reserve(colCount);

    BOOST_SCOPE_EXIT_ALL(&success, &types) {
        if (!success) {
            for (SharemindTdbType * const type : types)
                SharemindTdbType_delete(type);
            types.clear();
        }
    };

    // Cache the types that have already been resolved
    typedef std::map<hobj_ref_t, SharemindTdbType *> TypesMap;
    TypesMap typesMap;

    for (hsize_t i = 0; i < colCount; ++i) {
        auto const it(const_cast<TypesMap const &>(typesMap).find(
                          indices[i].dataset_ref));
        if (it == typesMap.end()) {
            // Get dataset from reference
            const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &indices[i].dataset_ref);
            if (oId < 0) {
                m_logger.error() << "Failed to dereference object.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, oId) {
                if (H5Oclose(oId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset.";
            };

            // Open the type attribute
            const hid_t aId = H5Aopen(oId, DATASET_TYPE_ATTR, H5P_DEFAULT);
            if (aId < 0) {
                m_logger.error() << "Failed to open dataset type attribute.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, aId) {
                if (H5Aclose(aId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
            };

            // Open type attribute type
            const hid_t aTId = H5Aget_type(aId);
            if (aTId < 0) {
                m_logger.error() << "Failed to get dataset type attribute type.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, aTId) {
                if (H5Tclose(aTId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
            };

            const hid_t aSId = H5Aget_space(aId);
            if (aSId < 0) {
                m_logger.error() << "Failed to get dataset type attribute data space.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, aSId) {
                if (H5Sclose(aSId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute data space.";
            };

            // Read the type attribute
            auto const type(std::make_unique<SharemindTdbType>());
            if (H5Aread(aId, aTId, type.get()) < 0) {
                m_logger.error() << "Failed to read dataset type attribute.";
                return SHAREMIND_TDB_IO_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, aTId, aSId, &type) {
                if (H5Dvlen_reclaim(aTId, aSId, H5P_DEFAULT, type.get()) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type attribute object.";
            };

            {
                auto tdbType(SharemindTdbType_new(type->domain,
                                                  type->name,
                                                  type->size));
                try {
                    types.emplace_back(tdbType);
                } catch (...) {
                    SharemindTdbType_delete(tdbType);
                    throw;
                }
            }

            #ifndef NDEBUG
            auto const rv =
            #endif
                    typesMap.emplace(indices[i].dataset_ref, types.back());
            assert(rv.second);
        } else {
            auto tdbType(SharemindTdbType_new(it->second->domain,
                                              it->second->name,
                                              it->second->size));
            try {
                types.push_back(tdbType);
            } catch (...) {
                SharemindTdbType_delete(tdbType);
                throw;
            }
        }
    }

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::tblRowCount(const std::string & tbl, size_type & count) {
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to get row count for table \"" << tbl << "\".";
    };

    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Read row meta info
    hsize_t nrows = 0;
    {
        const SharemindTdbError ecode = getRowCount(fileId, nrows);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    count = nrows;

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::insertRow(const std::string & tbl,
        const std::vector<std::vector<SharemindTdbValue *> > & valuesBatch,
        const std::vector<bool> & valueAsColumnBatch)
{
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to insert row(s) into table \"" << tbl << "\".";
    };

    if (valuesBatch.empty()) {
        m_logger.error() << "No values given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    if (valuesBatch.size() != valueAsColumnBatch.size()) {
        m_logger.error() << "Incomplete arguments given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    {
        for (auto const & values : valuesBatch) {
            if (values.empty()) {
                m_logger.error() << "Empty batch of values given.";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }

            if (!validateValues(values))
                return SHAREMIND_TDB_INVALID_ARGUMENT;
        }
    }

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Get table row count
    hsize_t rowCount = 0u;
    {
        const SharemindTdbError ecode = getRowCount(fileId, rowCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Get table column count
    hsize_t colCount = 0u;
    {
        const SharemindTdbError ecode = getColumnCount(fileId, colCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Get column types
    typedef std::map<hobj_ref_t, std::pair<SharemindTdbType *, hid_t> > RefTypeMap;
    RefTypeMap refTypes;

    typedef std::map<SharemindTdbType *, size_type, SharemindTdbTypeLess> TypeCountMap;
    TypeCountMap typeCounts;

    BOOST_SCOPE_EXIT_ALL(this, &refTypes) {
        for (auto & pair : refTypes) {
            SharemindTdbType * const type = pair.second.first;
            const hid_t aId = pair.second.second;

            if (!cleanupType(aId, *type))
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute object.";

            if (H5Aclose(aId) < 0)
                m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";

            delete type;
        }
        refTypes.clear();
    };

    {
        // Create a type for reading the dataset references
        const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(hobj_ref_t));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, tId) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        };

        if (H5Tinsert(tId, "dataset_ref", 0u, H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Open the column meta info dataset
        const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.fullDebug() << "Failed to open column meta info dataset.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, dId) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        };

        // Read dataset references from the column meta info dataset
        auto const dsetRefs(std::make_unique<hobj_ref_t[]>(colCount));
        if (H5Dread(dId, tId, H5S_ALL, H5S_ALL, H5P_DEFAULT, dsetRefs.get()) < 0) {
            m_logger.fullDebug() << "Failed to read column meta info dataset.";
            return SHAREMIND_TDB_IO_ERROR;
        }

        // Resolve references to types
        for (size_type i = 0u; i < colCount; ++i) {
            auto const it(const_cast<RefTypeMap const &>(refTypes).find(
                              dsetRefs[i]));
            if (it == refTypes.end()) {
                hid_t aId = H5I_INVALID_HID;

                // Read the type attribute
                auto type(std::make_unique<SharemindTdbType>());
                const SharemindTdbError ecode = objRefToType(fileId, dsetRefs[i], aId, *type);
                if (ecode != SHAREMIND_TDB_OK) {
                    m_logger.error() << "Failed to get type info from dataset reference.";
                    return ecode;
                }

                #ifndef NDEBUG
                const bool r =
                #endif
                        typeCounts.emplace(type.get(), 1)
                        #ifndef NDEBUG
                            .second
                        #endif
                        ;
                assert(r);
                #ifndef NDEBUG
                const bool r2 =
                #endif
                        refTypes.emplace(dsetRefs[i], RefTypeMap::mapped_type(type.release(), aId))
                        #ifndef NDEBUG
                            .second
                        #endif
                        ;
                assert(r2);
            } else {
                SharemindTdbType * const type = it->second.first;
                ++typeCounts[type];
            }
        }
    }

    // Aggregate the values by the value types
    struct ValuesInfo {
        std::vector<SharemindTdbValue *> values;
        std::vector<bool> valueAsColumn;
    };

    typedef std::vector<SharemindTdbValue *> ValuesVector;
    typedef std::map<SharemindTdbType *, ValuesInfo, SharemindTdbTypeLess> TypeValueMap;
    TypeValueMap typeValues;

    size_type insertedRowCount = 0u;

    auto vacIt(valueAsColumnBatch.cbegin());
    for (const ValuesVector & values : valuesBatch) {
        size_type batchColCount = 0u;

        // Get the row count for this batch
        const size_type batchRowCount =
            !*vacIt || isVariableLengthType(values.front()->type) ?
            1u : values.front()->size / values.front()->type->size;

        typedef std::map<SharemindTdbType *, size_t, SharemindTdbTypeLess> BatchTypeCountMap;
        BatchTypeCountMap batchTypeCount;

        for (SharemindTdbValue * const val : values) {
            SharemindTdbType * const type = val->type;

            if (isVariableLengthType(type)) {
                if (*vacIt && batchRowCount != 1u) {
                    m_logger.error() << "Inconsistent row count for a value batch.";
                    return SHAREMIND_TDB_INVALID_ARGUMENT;
                }

                // For variable length types we do not support arrays
                batchColCount += 1u;
                batchTypeCount[type] += 1u;
            } else {
                assert(val->size);

                auto const tIt(
                            const_cast<TypeCountMap const &>(typeCounts).find(
                                type));
                if (tIt == typeCounts.end()) {
                    m_logger.error() << "Given values do not match the table schema.";
                    return SHAREMIND_TDB_INVALID_ARGUMENT;
                }

                if (type->size != tIt->first->size) {
                    m_logger.error() << "Given values do not match the table schema.";
                    return SHAREMIND_TDB_INVALID_ARGUMENT;
                }

                assert(val->size % type->size == 0u);

                if (*vacIt) {
                    if (val->size / type->size != batchRowCount) {
                        m_logger.error() << "Inconsistent row count for a value batch.";
                        return SHAREMIND_TDB_INVALID_ARGUMENT;
                    }

                    batchColCount += 1u;
                    batchTypeCount[type] += 1u;
                } else {
                    batchColCount += val->size / type->size;
                    batchTypeCount[type] += val->size / type->size;
                }
            }

            ValuesInfo & valInfo = typeValues[type];
            valInfo.values.push_back(val);
            valInfo.valueAsColumn.push_back(*vacIt);
        }

        // Check if we have values for all the columns
        if (batchColCount != colCount) {
            m_logger.error() << "Given number of values differs from the number of columns.";
            return SHAREMIND_TDB_INVALID_ARGUMENT;
        }

        // Check the if we have the correct number of values for each type
        for (auto & pair : batchTypeCount) {
            SharemindTdbType * const type = pair.first;
            const size_t count = pair.second;

            auto const tIt(const_cast<TypeCountMap const &>(typeCounts).find(
                               type));
            assert(tIt != typeCounts.end());

            if (count != tIt->second) {
                m_logger.error() << "Invalid number of values for type \""
                    << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }
        }

        insertedRowCount += batchRowCount;
        ++vacIt;
    }

    // Set cleanup handler to restore the initial state if something goes wrong
    typedef std::map<hobj_ref_t, std::pair<hsize_t, hsize_t> > CleanupMap;
    CleanupMap cleanup;

    BOOST_SCOPE_EXIT_ALL(&success, this, &cleanup, fileId) {
        if (!success) {
            for (auto const & vp : cleanup) {
                const hobj_ref_t dsetRef = vp.first;

                // Get dataset from reference
                const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &dsetRef);
                if (oId < 0) {
                    m_logger.error() << "Error while restoring initial state: Failed to open dataset reference.";
                    break;
                }

                // Set the size of the dataset back to the original
                const hsize_t dims[] = { vp.second.first, vp.second.second };
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
    };

    // For each dataset, write the data
    // TODO move to a separate function
    {
        for (auto const & pair : refTypes) {
            const hobj_ref_t dsetRef = pair.first;
            SharemindTdbType * const type = pair.second.first;

            // Get the number of columns for this type
            auto const tIt(const_cast<TypeCountMap const &>(typeCounts).find(
                               type));
            assert(tIt != typeCounts.end());

            const size_type dsetCols = tIt->second;
            // TODO sanity checks for row and column counts

            // Get dataset from reference. We already checked earlier if this is
            // a valid dataset reference.
            const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &dsetRef);
            if (oId < 0) {
                m_logger.error() << "Failed to get dataset from dataset reference.";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, oId) {
                if (H5Oclose(oId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset.";
            };

            // Get dataset type
            const hid_t tId = H5Dget_type(oId);
            if (tId < 0) {
                m_logger.error() << "Failed to get dataset type for type \"" << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, tId) {
                if (H5Tclose(tId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset type.";
            };

            // Create a simple memory data space
            const hsize_t mDims[] = { insertedRowCount, dsetCols };
            const hid_t mSId = H5Screate_simple(2, mDims, nullptr);
            if (mSId < 0) {
                m_logger.error() << "Failed to create memory data space for type \"" << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, mSId) {
                if (H5Sclose(mSId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up memory data space.";
            };

            // Extend the dataset
            const hsize_t dims[] = { rowCount + insertedRowCount, dsetCols };
            if (H5Dset_extent(oId, dims) < 0) {
                m_logger.error() << "Failed to extend dataset for type \"" << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Register this dataset for cleanup
            cleanup.emplace(dsetRef, std::pair<hsize_t, hsize_t>(rowCount, dsetCols));

            // Get dataset data space
            const hid_t sId = H5Dget_space(oId);
            if (tId < 0) {
                m_logger.error() << "Failed to get dataset data space for type \"" << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            BOOST_SCOPE_EXIT_ALL(this, sId) {
                if (H5Sclose(sId) < 0)
                    m_logger.fullDebug() << "Error while cleaning up dataset data space.";
            };

            // Select a hyperslab in the data space to write to
            const hsize_t start[] = { rowCount, 0 };
            const hsize_t count[] = { insertedRowCount, dsetCols };
            if (H5Sselect_hyperslab(sId, H5S_SELECT_SET, start, nullptr, count, nullptr) < 0) {
                m_logger.error() << "Failed to do selection in data space for type \"" << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_GENERAL_ERROR;
            }

            // Serialize the values
            auto const tvIt(const_cast<TypeValueMap const &>(typeValues).find(
                                type));
            assert(tvIt != typeValues.end());
            const std::vector<SharemindTdbValue *> & values = tvIt->second.values;
            const std::vector<bool> & vac = tvIt->second.valueAsColumn;

            // Aggregate the values into a single buffer
            void * buffer = nullptr;
            bool delBuffer = false;

            if (isVariableLengthType(type)) {
                assert(insertedRowCount * dsetCols == values.size());

                buffer = ::operator new(insertedRowCount * dsetCols * sizeof(hvl_t));
                delBuffer = true;

                hvl_t * cursor = static_cast<hvl_t *>(buffer);
                for (SharemindTdbValue const * const val : values) {
                    cursor->len = val->size;
                    cursor->p = val->buffer;
                    ++cursor;
                }
            } else {
                if (values.size() == 1u) {
                    // Since we don't have to aggregate anything, we can use the
                    // existing buffer.
                    buffer = values.back()->buffer;
                } else {
                    // Copy the values into a continuous buffer
                    buffer = ::operator new(insertedRowCount * dsetCols * type->size);
                    delBuffer = true;

                    if (dsetCols > 1u) {
                        size_t offset = 0u;
                        size_t transposeOffset = 0u;

                        bool lastAsColumn = false;
                        auto vacIt(vac.cbegin());

                        for (SharemindTdbValue const * const val : values) {
                            const bool asColumn = *vacIt++;
                            memcpy(static_cast<char *>(buffer) + offset, val->buffer, val->size);

                            // Check if we are at the beginning of a transposed
                            // block
                            if (!lastAsColumn && asColumn)
                                transposeOffset = offset;

                            // Check if we are at the end of a transposed block
                            if (lastAsColumn && !asColumn)
                                transposeBlock(static_cast<char *>(buffer) + transposeOffset,
                                        static_cast<char *>(buffer) + offset,
                                        (offset - transposeOffset) / (type->size * dsetCols),
                                        type->size);

                            offset += val->size;
                            lastAsColumn = asColumn;
                        }

                        // Check if we still need to transpose the last block
                        if (lastAsColumn)
                            transposeBlock(static_cast<char *>(buffer) + transposeOffset,
                                    static_cast<char *>(buffer) + offset,
                                    (offset - transposeOffset) / (type->size * dsetCols),
                                    type->size);
                    } else {
                        size_t offset = 0u;

                        for (SharemindTdbValue const * const val : values) {
                            memcpy(static_cast<char *>(buffer) + offset, val->buffer, val->size);
                            offset += val->size;
                        }
                    }
                }
            }

            assert(buffer);

            BOOST_SCOPE_EXIT_ALL(&buffer, &delBuffer) {
                if (delBuffer)
                    ::operator delete(buffer);
            };

            // Write the values
            if (H5Dwrite(oId, tId, mSId, sId, H5P_DEFAULT, buffer) < 0) {
                m_logger.error() << "Failed to write values for type \""
                    << type->domain << "::" << type->name << "\".";
                return SHAREMIND_TDB_IO_ERROR;
            }
        }
    }

    // Update row count
    {
        const SharemindTdbError ecode = setRowCount(fileId, rowCount + insertedRowCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Flush the buffers to reduce the chance of file corruption
    if (H5Fflush(fileId, H5F_SCOPE_LOCAL) < 0)
        m_logger.fullDebug() << "Error while flushing buffers.";

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::readColumn(const std::string & tbl,
        const std::vector<SharemindTdbString *> & colIdBatch,
        std::vector<std::vector<SharemindTdbValue *> > & valuesBatch)
{
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to read column(s) in table \"" << tbl << "\".";
    };

    if (colIdBatch.empty()) {
        m_logger.error() << "Empty batch of parameters given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Check the column names
    if (!validateColumnNames(colIdBatch))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check for duplicates
    {
        std::set<SharemindTdbString const *, SharemindTdbStringLess> colIdSet;
        for (auto const * const v : colIdBatch) {
            if (!colIdSet.emplace(v).second) {
                m_logger.error() << "Duplicate column names given.";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }
        }
    }

    // Get the table column names
    std::vector<SharemindTdbString *> colNames;
    {
        const SharemindTdbError ecode = tblColNames(tbl, colNames);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    BOOST_SCOPE_EXIT_ALL(&colNames) {
        for (auto * const colName : colNames)
            SharemindTdbString_delete(colName);
        colNames.clear();
    };

    // Sort the table column names
    typedef std::map<SharemindTdbString *, size_t, SharemindTdbStringLess> ColNamesMap;
    ColNamesMap colNamesMap;
    for (size_t i = 0, end = colNames.size(); i < end; ++i) {
        const bool rv = colNamesMap.emplace(colNames[i], i).second;
        (void) rv; assert(rv);
    }

    // Get the column numbers for the names
    std::vector<SharemindTdbIndex *> colNrBatch;
    colNrBatch.reserve(colIdBatch.size());

    BOOST_SCOPE_EXIT_ALL(&colNrBatch) {
        for (auto * const colNr : colNrBatch)
            SharemindTdbIndex_delete(colNr);
        colNrBatch.clear();
    };

    {
        for (auto * const colId : colIdBatch) {
            auto const nIt(const_cast<ColNamesMap const &>(colNamesMap).find(
                               colId));
            if (nIt == colNamesMap.end()) {
                m_logger.error() << "Table \"" << tbl << "\" does not contain column \"" << colId->str << "\".";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }

            auto tdbIndex(SharemindTdbIndex_new(nIt->second));
            try {
                colNrBatch.push_back(tdbIndex);
            } catch (...) {
                SharemindTdbIndex_delete(tdbIndex);
                throw;
            }
        }
    }

    {
        const SharemindTdbError ecode = readColumn(fileId, colNrBatch, valuesBatch);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::readColumn(const std::string & tbl,
        const std::vector<SharemindTdbIndex *> & colIdBatch,
        std::vector<std::vector<SharemindTdbValue *> > & valuesBatch)
{
    H5Eset_auto(H5E_DEFAULT, err_handler, &const_cast<LogHard::Logger &>(m_logger));

    // Set the cleanup flag
    bool success = false;

    BOOST_SCOPE_EXIT_ALL(&success, this, &tbl) {
        if (!success)
            m_logger.error() << "Failed to read column(s) in table \"" << tbl << "\".";
    };

    if (colIdBatch.empty()) {
        m_logger.error() << "Empty batch of parameters given.";
        return SHAREMIND_TDB_INVALID_ARGUMENT;
    }

    // Do some simple checks on the parameters
    if (!validateTableName(tbl))
        return SHAREMIND_TDB_INVALID_ARGUMENT;

    // Check if table exists
    {
        bool exists = false;
        const SharemindTdbError ecode = tblExists(tbl, exists);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;

        if (!exists) {
            m_logger.error() << "Table \"" << tbl << "\" does not exist.";
            return SHAREMIND_TDB_TABLE_NOT_FOUND;
        }
    }

    // Open the table file
    const hid_t fileId = openTableFile(tbl);
    if (fileId < 0) {
        m_logger.error() << "Failed to open table file.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    // Get table column count
    hsize_t colCount = 0;
    {
        const SharemindTdbError ecode = getColumnCount(fileId, colCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Check if column numbers are valid
    {
        std::set<uint64_t> uniqueColumns;
        for (SharemindTdbIndex const * const colId : colIdBatch) {
            assert(colId);

            if (colId->idx >= colCount) {
                m_logger.error() << "Column number out of range.";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }
            if (!uniqueColumns.emplace(colId->idx).second) {
                m_logger.error() << "Duplicate column numbers given.";
                return SHAREMIND_TDB_INVALID_ARGUMENT;
            }
        }
    }

    {
        const SharemindTdbError ecode = readColumn(fileId, colIdBatch, valuesBatch);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    success = true;

    return SHAREMIND_TDB_OK;
}

bool TdbHdf5Connection::pathExists(const fs::path & path, bool & status) {
    try {
        status = exists(path);
    } catch (const fs::filesystem_error & e) {
        m_logger.error() << "Error while checking if file " << path.string()
                         << " exists: " << e.what();
        return false;
    }

    return true;
}

bool TdbHdf5Connection::validateColumnNames(const std::vector<SharemindTdbString *> & names) const {
    for (auto const * const str : names) {
        assert(str);

        const size_t size = strlen(str->str);
        if (size == 0) {
            m_logger.error() << "Column name must be a non-empty string.";
            return false;
        }
        if (size > COL_NAME_SIZE_MAX) {
            m_logger.error() << "Column name too long. Maximum length is " << COL_NAME_SIZE_MAX << ".";
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
    for (auto const * const val : values) {
        assert(val);

        SharemindTdbType * const type = val->type;
        assert(type);

        // Variable length types are handled differently
        if (isVariableLengthType(type))
            continue;

        // Check if the value is non-empty
        if (val->size == 0) {
            m_logger.error() << "Invalid value of type \"" << type->domain
                << "::" << type->name << "\": Value size must be greater than zero.";
            return false;
        }

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
    p /= tbl + FILE_EXT;

    return p;
}

SharemindTdbError TdbHdf5Connection::readColumn(const hid_t fileId,
                                   const std::vector<SharemindTdbIndex *> & colNrBatch,
                                   std::vector<std::vector<SharemindTdbValue *> > & valuesBatch)
{
    // Set the cleanup flag
    bool success = false;

    // Get table row count
    hsize_t rowCount = 0;
    {
        const SharemindTdbError ecode = getRowCount(fileId, rowCount);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    // Declare a partial column index type
    struct PartialColumnIndex {
        hobj_ref_t dataset_ref;
        hsize_t dataset_column;
    };

    // Get the column meta info
    // TODO this needs to go into a separate function
    std::vector<PartialColumnIndex> indices;
    {
        // Create a type for reading the partial index
        const hid_t tId = H5Tcreate(H5T_COMPOUND, sizeof(PartialColumnIndex));
        if (tId < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, tId) {
            if (H5Tclose(tId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info type.";
        };

        if (H5Tinsert(tId, "dataset_ref", HOFFSET(PartialColumnIndex, dataset_ref), H5T_STD_REF_OBJ) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        if (H5Tinsert(tId, "dataset_column", HOFFSET(PartialColumnIndex, dataset_column), H5T_NATIVE_HSIZE) < 0) {
            m_logger.error() << "Failed to create column meta info type.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        // Create a simple memory data space
        const hsize_t mDims = colNrBatch.size();
        const hid_t mSId = H5Screate_simple(1, &mDims, nullptr);
        if (mSId < 0) {
            m_logger.error() << "Failed to create column meta info memory data space.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, mSId) {
            if (H5Sclose(mSId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info memory data space.";
        };

        // Open the column meta info dataset
        const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
        if (dId < 0) {
            m_logger.error() << "Failed to open column meta info dataset.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, dId) {
            if (H5Dclose(dId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
        };

        // Open the column meta info data space
        const hid_t sId = H5Dget_space(dId);
        if (sId < 0) {
            m_logger.error() << "Failed to get column meta info data space.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        BOOST_SCOPE_EXIT_ALL(this, sId) {
            if (H5Sclose(sId) < 0)
                m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
        };

        // Select points in the data space for reading
        // NOTE: points are read in the order of point selection
        std::vector<hsize_t> coords;
        coords.reserve(colNrBatch.size());

        for (auto const * const colNr : colNrBatch)
            coords.push_back(colNr->idx);

        if (H5Sselect_elements(sId, H5S_SELECT_SET, coords.size(), &coords.front()) < 0) {
            m_logger.error() << "Failed to do selection in column meta info data space.";
            return SHAREMIND_TDB_GENERAL_ERROR;
        }

        indices.resize(colNrBatch.size());

        // Read column meta info from the dataset
        if (H5Dread(dId, tId, mSId, sId, H5P_DEFAULT, &indices.front()) < 0) {
            m_logger.error() << "Failed to read column meta info dataset.";
            return SHAREMIND_TDB_IO_ERROR;
        }
    }

    // Aggregate the column numbers and results for each dataset
    typedef std::vector<SharemindTdbValue *> ValuesVector;
    typedef std::map<hobj_ref_t, std::vector<std::pair<hsize_t, ValuesVector *> > > DatasetBatchMap;

    DatasetBatchMap dsetBatch;
    {
        valuesBatch.resize(colNrBatch.size());

        size_t count = 0;
        for (auto const & index : indices)
            dsetBatch[index.dataset_ref].push_back(
                        std::make_pair(index.dataset_column,
                                       &valuesBatch[count++]));
    }

    // Register cleanup for valuesBatch, if something goes wrong
    BOOST_SCOPE_EXIT_ALL(&success, &valuesBatch) {
        if (!success) {
            for (auto const & values : valuesBatch)
                for (auto * const value : values)
                    SharemindTdbValue_delete(value);
            valuesBatch.clear();
        }
    };

    // Read the columns
    for (auto const & vp : dsetBatch) {
        SharemindTdbError const ecode =
                readDatasetColumn(fileId, vp.first, vp.second);
        if (ecode != SHAREMIND_TDB_OK)
            return ecode;
    }

    success = true;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::readDatasetColumn(const hid_t fileId, const hobj_ref_t ref,
        const std::vector<std::pair<hsize_t, std::vector<SharemindTdbValue *> *> > & paramBatch) {
    assert(paramBatch.size());

    // Get dataset from reference
    const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &ref);
    if (oId < 0) {
        m_logger.error() << "Failed to dereference object.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, oId) {
        if (H5Oclose(oId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset.";
    };

    // Check reference object type
    H5O_type_t rType = H5O_TYPE_UNKNOWN;
    if (H5Rget_obj_type(oId, H5R_OBJECT, &ref, &rType) < 0) {
        m_logger.error() << "Failed to get reference object type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    if (rType != H5O_TYPE_DATASET) {
        m_logger.error() << "Invalid dataset reference object.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Get data space
    const hid_t sId = H5Dget_space(oId);
    if (sId < 0) {
        m_logger.error() << "Failed to get dataset data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, sId) {
        if (H5Sclose(sId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset data space.";
    };

    // Get data space rank
    const int rank = H5Sget_simple_extent_ndims(sId);
    if (rank < 0) {
        m_logger.error() << "Failed to get dataset data space rank.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Check data space rank
    if (rank != 2) {
        m_logger.error() << "Invalid rank for dataset data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Get size of data space
    hsize_t dims[2];
    if (H5Sget_simple_extent_dims(sId, dims, nullptr) < 0) {
        m_logger.error() << "Failed to get dataset data space size.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // TODO check dims[0] against row count attribute?

    // Check if column offset is in range
    for (auto const & param : paramBatch) {
        if (param.first >= dims[1]) {
            m_logger.error() << "Invalid dataset column number: out of range.";
            return SHAREMIND_TDB_INVALID_ARGUMENT;
        }
    }

    // Open the type attribute
    const hid_t aId = H5Aopen(oId, DATASET_TYPE_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to open dataset type attribute.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aId) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
    };

    // Open type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0) {
        m_logger.error() << "Failed to get dataset type attribute type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aTId) {
        if (H5Tclose(aTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
    };

    const hid_t aSId = H5Aget_space(aId);
    if (aSId < 0) {
        m_logger.error() << "Failed to get dataset type attribute data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aSId) {
        if (H5Sclose(aSId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute data space.";
    };

    // Read the type attribute
    auto const type(std::make_unique<SharemindTdbType>());
    if (H5Aread(aId, aTId, type.get()) < 0) {
        m_logger.error() << "Failed to read dataset type attribute.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aTId, aSId, &type) {
        if (H5Dvlen_reclaim(aTId, aSId, H5P_DEFAULT, type.get()) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute object.";
    };

    {
        for (auto const & param : paramBatch) {
            // Check if we have anything to read
            if (dims[0] == 0) {
                // TODO check if this is handled correctly
                auto val(SharemindTdbValue_new(type->domain,
                                               type->name,
                                               type->size,
                                               nullptr,
                                               0));
                try {
                    param.second->push_back(val);
                } catch (...) {
                    SharemindTdbValue_delete(val);
                    throw;
                }
            } else {
                void * buffer = nullptr;
                size_type bufferSize = 0;

                // Read the column data
                if (isVariableLengthType(type.get())) {
                    buffer = ::operator new(dims[0] * sizeof(hvl_t));
                } else {
                    bufferSize = dims[0] * type->size;
                    buffer = ::operator new(bufferSize);
                }

                assert(buffer);

                // Select a hyperslab in the data space to read from
                const hsize_t start[] = { 0, param.first };
                const hsize_t count[] = { dims[0], 1 };
                if (H5Sselect_hyperslab(sId, H5S_SELECT_SET, start, nullptr, count, nullptr) < 0) {
                    m_logger.error() << "Failed to do selection in dataset data space.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }

                // Get dataset type
                const hid_t tId = H5Dget_type(oId);
                if (tId < 0) {
                    m_logger.error() << "Failed to get dataset type.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }

                BOOST_SCOPE_EXIT_ALL(this, tId) {
                    if (H5Tclose(tId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up type for column data";
                };

                // Create a simple memory data space
                const hsize_t mDims[] = { dims[0], 1 };
                const hid_t mSId = H5Screate_simple(2, mDims, nullptr);
                if (mSId < 0) {
                    m_logger.error() << "Failed to create memory data space for column data.";
                    return SHAREMIND_TDB_GENERAL_ERROR;
                }

                BOOST_SCOPE_EXIT_ALL(this, mSId) {
                    if (H5Sclose(mSId) < 0)
                        m_logger.fullDebug() << "Error while cleaning up memory data space for column data.";
                };

                // Read the dataset data
                if (H5Dread(oId, tId, mSId, sId, H5P_DEFAULT, buffer) < 0) {
                    m_logger.error() << "Failed to read the dataset.";
                    return SHAREMIND_TDB_IO_ERROR;
                }

                if (isVariableLengthType(type.get())) {
                    hvl_t * const hvlBuffer = static_cast<hvl_t *>(buffer);

                    for (hsize_t i = 0; i < dims[0]; ++i) {
                        auto val(std::make_unique<SharemindTdbValue>());
                        val->type = SharemindTdbType_new(type->domain,
                                                         type->name,
                                                         type->size);
                        try {
                            bufferSize = hvlBuffer[i].len;
                            val->buffer = ::operator new(bufferSize);
                            try {
                                memcpy(val->buffer, hvlBuffer[i].p, bufferSize);
                                val->size = bufferSize;

                                param.second->push_back(val.get());
                                val.release();
                            } catch (...) {
                                ::operator delete(val->buffer);
                                throw;
                            }
                        } catch (...) {
                            SharemindTdbType_delete(val->type);
                            throw;
                        }
                    }

                    // Release the memory allocated for the variable length types
                    if (H5Dvlen_reclaim(tId, mSId, H5P_DEFAULT, buffer) < 0)
                        m_logger.fullDebug() << "Error while cleaning up column data.";

                    // Free the variable length type array
                    ::operator delete(buffer);
                } else {
                    auto val(std::make_unique<SharemindTdbValue>());
                    val->type = SharemindTdbType_new(type->domain,
                                                     type->name,
                                                     type->size);
                    try {
                        val->buffer = buffer;
                        val->size = bufferSize;

                        param.second->push_back(val.get());
                        val.release();
                    } catch (...) {
                        SharemindTdbType_delete(val->type);
                        throw;
                    }
                }
            }
        }
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::objRefToType(const hid_t fileId, const hobj_ref_t ref, hid_t & aId, SharemindTdbType & type) {
    // Get the dataset from the reference
    const hid_t oId = H5Rdereference(fileId, H5R_OBJECT, &ref);
    if (oId < 0) {
        m_logger.error() << "Failed to dereference object.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, oId) {
        if (H5Oclose(oId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset.";
    };

    // Check the reference object type
    H5O_type_t rType = H5O_TYPE_UNKNOWN;
    if (H5Rget_obj_type(oId, H5R_OBJECT, &ref, &rType) < 0) {
        m_logger.error() << "Failed to get reference object type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    if (rType != H5O_TYPE_DATASET) {
        m_logger.error() << "Invalid dataset reference object.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Open the type attribute
    aId = H5Aopen(oId, DATASET_TYPE_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to open dataset type attribute.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    bool closeAttr = true;

    BOOST_SCOPE_EXIT_ALL(&closeAttr, this, aId) {
        if (closeAttr && H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute.";
    };

    // Open the type attribute type
    const hid_t aTId = H5Aget_type(aId);
    if (aTId < 0) {
        m_logger.error() << "Failed to get dataset type attribute type.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aTId) {
        if (H5Tclose(aTId) < 0)
            m_logger.fullDebug() << "Error while cleaning up dataset type attribute type.";
    };

    // Read the type attribute
    if (H5Aread(aId, aTId, &type) < 0) {
        m_logger.error() << "Failed to read dataset type attribute type.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    closeAttr = false;

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::getColumnCount(const hid_t fileId, hsize_t & ncols) {
    // Get dataset
    const hid_t dId = H5Dopen(fileId, COL_INDEX_DATASET, H5P_DEFAULT);
    if (dId < 0) {
        m_logger.error() << "Failed to open column meta info dataset.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, dId) {
        if (H5Dclose(dId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info dataset.";
    };

    // Get data space
    const hid_t sId = H5Dget_space(dId);
    if (sId < 0) {
        m_logger.error() << "Failed to open column meta info data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, sId) {
        if (H5Sclose(sId) < 0)
            m_logger.fullDebug() << "Error while cleaning up column meta info data space.";
    };

    // Get data space rank
    const int rank = H5Sget_simple_extent_ndims(sId);
    if (rank < 0) {
        m_logger.error() << "Failed to get column meta info data space rank.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Check data space rank
    if (rank != 1) {
        m_logger.error() << "Invalid rank for column meta info data space.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    // Get size of data space
    if (H5Sget_simple_extent_dims(sId, &ncols, nullptr) < 0) {
        m_logger.error() << "Failed to get column count from column meta info.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::getRowCount(const hid_t fileId, hsize_t & nrows) {
    // Open meta info group
    const hid_t gId = H5Gopen(fileId, META_GROUP, H5P_DEFAULT);
    if (gId < 0) {
        m_logger.error() << "Failed to get row count: Failed to open meta info group.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, gId) {
        if (H5Gclose(gId) < 0)
            m_logger.fullDebug() << "Error while cleaning up meta info group.";
    };

    // Open the row count attribute
    const hid_t aId = H5Aopen(gId, ROW_COUNT_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to get row count: Failed to open row meta info attribute.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aId) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up row meta info.";
    };

    if (H5Aread(aId, H5T_NATIVE_HSIZE, &nrows) < 0) {
        m_logger.error() << "Failed to get row count: Failed to read row meta info attribute.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    return SHAREMIND_TDB_OK;
}

SharemindTdbError TdbHdf5Connection::setRowCount(const hid_t fileId, const hsize_t nrows) {
    // Open meta info group
    const hid_t gId = H5Gopen(fileId, META_GROUP, H5P_DEFAULT);
    if (gId < 0) {
        m_logger.error() << "Failed to set row count: Failed to open meta info group.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, gId) {
        if (H5Gclose(gId) < 0)
            m_logger.fullDebug() << "Error while cleaning up meta info group.";
    };

    // Open the row count attribute
    const hid_t aId = H5Aopen(gId, ROW_COUNT_ATTR, H5P_DEFAULT);
    if (aId < 0) {
        m_logger.error() << "Failed to set row count: Failed to open row meta info attribute.";
        return SHAREMIND_TDB_GENERAL_ERROR;
    }

    BOOST_SCOPE_EXIT_ALL(this, aId) {
        if (H5Aclose(aId) < 0)
            m_logger.fullDebug() << "Error while cleaning up row meta info.";
    };

    // Write the new row count
    if (H5Awrite(aId, H5T_NATIVE_HSIZE, &nrows) < 0) {
        m_logger.error() << "Failed to set row count: Failed to write row count attribute.";
        return SHAREMIND_TDB_IO_ERROR;
    }

    return SHAREMIND_TDB_OK;
}

bool TdbHdf5Connection::closeTableFile(const std::string & tbl) {
    assert(!tbl.empty());

    auto it(m_tableFiles.find(tbl));
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
    auto const it(const_cast<TableFileMap const &>(m_tableFiles).find(tbl));
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

    m_tableFiles.emplace(tbl, id);

    return id;
}

} /* namespace sharemind { */
