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

#include <map>
#include <memory>
#include <mutex>


namespace sharemind {

template<typename K, typename V>
class KeyValueCache : public std::enable_shared_from_this<KeyValueCache<K, V> > {
private: /* Types: */

    struct DeleteActor {
        DeleteActor(std::weak_ptr<KeyValueCache<K, V> > cache)
            : m_cache(cache)
        { }

        void operator() (V * const ptr) {
            std::shared_ptr<KeyValueCache<K, V> > cache = m_cache.lock();
            if (cache.get())
                cache->removePtr(ptr);

            delete ptr;
        }

        std::weak_ptr<KeyValueCache<K, V> > m_cache;
    };

    typedef std::pair<V *, std::weak_ptr<V> > ValuePtr;
    typedef std::map<K, ValuePtr> ValueMap;
    typedef std::map<V *, typename ValueMap::iterator> DeleterMap;

protected: /* Methods: */

    virtual ~KeyValueCache() noexcept {}

    std::shared_ptr<V> get(const K & key) {
        std::lock_guard<std::mutex> lock(m_mutex);

        // Check if we already have the value in cache
        typename ValueMap::iterator it = m_valueMap.find(key);
        if (it != m_valueMap.end()) {
            // Check if the value is valid
            ValuePtr & valPtr = it->second;
            std::shared_ptr<V> ptr = valPtr.second.lock();
            if (ptr.get())
                return ptr;

            #ifndef NDEBUG
            const size_t c =
            #endif
                    m_deleterMap.erase(valPtr.first);
            assert(c > 0u);
            m_valueMap.erase(it);
        }

        // Create a new value
        V * const val = alloc(key);
        if (!val)
            return std::shared_ptr<V>();

        std::shared_ptr<V> ptr(val, DeleteActor(this->shared_from_this()));
        it = m_valueMap.insert(typename ValueMap::value_type(key, ValuePtr(ptr.get(), ptr))).first;
        #ifndef NDEBUG
        const bool r =
        #endif
                m_deleterMap.insert(typename DeleterMap::value_type(ptr.get(), it))
                #ifndef NDEBUG
                    .second
                #endif
                ;
        assert(r);

        return ptr;
    }

    virtual V * alloc(const K & key) const = 0;

private: /* Methods: */

    void removePtr(V * const ptr) {
        std::lock_guard<std::mutex> lock(m_mutex);

        typename DeleterMap::iterator it = m_deleterMap.find(ptr);
        if (it == m_deleterMap.end())
            return;

        m_valueMap.erase(it->second);
        m_deleterMap.erase(it);
    }

private: /* Fields: */

    mutable std::mutex m_mutex;

    ValueMap m_valueMap;
    DeleterMap m_deleterMap;

};

} /* namespace sharemind { */

#endif // SHAREMINDCOMMON_KEYVALUECACHE_H
