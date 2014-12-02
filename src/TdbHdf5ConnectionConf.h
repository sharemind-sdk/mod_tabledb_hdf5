 /*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#ifndef SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTIONCONF_H
#define SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTIONCONF_H

#include <string>


namespace sharemind {

class __attribute__ ((visibility("internal"))) TdbHdf5ConnectionConf {
public: /* Methods: */

    bool load(const std::string & filename);

    inline std::string & getPath() { return m_path; }
    inline const std::string & getPath() const { return m_path; }

    inline std::string & getLastErrorMessage() { return m_lastErrorMessage; }
    inline const std::string & getLastErrorMessage() const { return m_lastErrorMessage; }

private: /* Fields: */

    std::string m_lastErrorMessage;

    std::string m_path;

}; /* class TdbHdf5ConnectionConf { */

} /* namespace sharemind */

#endif /* SHAREMIND_MOD_TABLEDB_HDF5_TDBHDF5CONNECTIONCONF_H */
