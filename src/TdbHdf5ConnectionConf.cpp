 /*
 * This file is a part of the Sharemind framework.
 * Copyright (C) Cybernetica AS
 *
 * All rights are reserved. Reproduction in whole or part is prohibited
 * without the written consent of the copyright owner. The usage of this
 * code is subject to the appropriate license agreement.
 */

#include "TdbHdf5ConnectionConf.h"

#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>


namespace pt = boost::property_tree;

namespace sharemind {

bool TdbHdf5ConnectionConf::load(const std::string & filename) {
    // TODO this probably needs some refactoring... maybe move to constructor

    // Define the configuration property tree:
    pt::ptree config;

    // Parse the configuration file into the property tree:
    try {

        pt::read_ini(filename, config);
        m_path = config.get<std::string>("DatabasePath");

    } catch (const pt::ini_parser_error & error) {
#if BOOST_VERSION <= 104200
        m_lastErrorMessage = error.what();
#else
        std::ostringstream o;
        o << error.message() << " [" << error.filename() << ":" << error.line() << "].";
        m_lastErrorMessage = o.str();
#endif
        return false;
    } catch (const pt::ptree_bad_data & error) {
        std::ostringstream o;
        o << "Bad data: " << error.what();
        m_lastErrorMessage = o.str();
        return false;
    } catch (const pt::ptree_bad_path & error) {
        std::ostringstream o;
        o << "Bad path: " << error.what();
        m_lastErrorMessage = o.str();
        return false;
    }

    return true;
}

} /* namespace sharemind { */
