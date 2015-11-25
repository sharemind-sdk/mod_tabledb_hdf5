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
