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

#ifndef SHAREMINDCOMMON_KEYVALUECACHE_H
#define SHAREMINDCOMMON_KEYVALUECACHE_H

#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <sharemind/DebugOnly.h>
#include <utility>


namespace sharemind {

template<typename K, typename V>
class KeyValueCache {

private: /* Types: */

    class Inner {

    private: /* Types: */

        class DeleteActor {

        public: /* Methods: */

            DeleteActor(DeleteActor &&) noexcept = default;
            DeleteActor(DeleteActor const &) noexcept = default;

            DeleteActor(std::weak_ptr<Inner> cache) noexcept
                : m_cache(std::move(cache))
            {}

            DeleteActor & operator=(DeleteActor &&) noexcept = default;
            DeleteActor & operator=(DeleteActor const &) noexcept = default;

            void operator()(V * const ptr) noexcept {
                if (std::shared_ptr<Inner> cache = m_cache.lock())
                    cache->removePtr(ptr);
                delete ptr;
            }

        private: /* Fields: */

            std::weak_ptr<Inner> m_cache;

        };

        using ValuePtr = std::pair<V *, std::weak_ptr<V> >;
        using ValueMap = std::map<K, ValuePtr>;
        using DeleterMap = std::map<V *, typename ValueMap::iterator>;

    public: /* Methods: */

        template <typename ValueFactory>
        std::shared_ptr<V> get(K const & key,
                               std::shared_ptr<Inner> const & self,
                               ValueFactory valueFactory)
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            // Check if we already have the value in cache
            {
                auto const it(m_valueMap.find(key));
                if (it != m_valueMap.end()) {
                    // Check if the value is valid
                    ValuePtr & valPtr = it->second;
                    if (auto ptr = valPtr.second.lock())
                        return ptr;

                    SHAREMIND_DEBUG_ONLY(auto const c =)
                            m_deleterMap.erase(valPtr.first);
                    assert(c > 0u);
                    m_valueMap.erase(it);
                }
            }

            // Create a new value
            std::shared_ptr<V> ptr(valueFactory(key), DeleteActor(self));
            auto const it(
                        m_valueMap.insert(
                            typename ValueMap::value_type(
                                key,
                                ValuePtr(ptr.get(), ptr))).first);
            SHAREMIND_DEBUG_ONLY(auto const r =)
                    m_deleterMap.insert(
                        typename DeleterMap::value_type(ptr.get(), it))
                    SHAREMIND_DEBUG_ONLY(.second);
            assert(r);
            return ptr;
        }

    private: /* Methods: */

        void removePtr(V * const ptr) {
            std::lock_guard<std::mutex> lock(m_mutex);

            auto const it(m_deleterMap.find(ptr));
            if (it == m_deleterMap.end())
                return;

            m_valueMap.erase(it->second);
            m_deleterMap.erase(it);
        }

    private: /* Fields: */

        std::mutex m_mutex;

        ValueMap m_valueMap;
        DeleterMap m_deleterMap;

    };

public: /* Methods: */

    template <typename ValueFactory>
    std::shared_ptr<V> get(K const & key, ValueFactory && valueFactory) {
        assert(m_inner);
        return m_inner->get(key,
                            m_inner,
                            std::forward<ValueFactory>(valueFactory));
    }

private: /* Fields: */

    std::shared_ptr<Inner> m_inner{std::make_shared<Inner>()};

};

} /* namespace sharemind { */

#endif // SHAREMINDCOMMON_KEYVALUECACHE_H
