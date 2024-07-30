#ifndef __MVBTREE_WRAPPER_HPP__
#define __MVBTREE_WRAPPER_HPP__

#include "tree_api.hpp"
#include "mvbtree_wrapper.h"
#include <cstdint>
#include <iostream>
#include <type_traits>
#include <map>
#include <cstring>
#include <array>
#include <mutex>
#include <shared_mutex>

template<typename Key, typename T>
class mvbtree_wrapper : public tree_api
{
public:
    mvbtree_wrapper();
    virtual ~mvbtree_wrapper();
    
    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;

private:
    void* mvbtree;
};

template<typename Key, typename T>
mvbtree_wrapper<Key,T>::mvbtree_wrapper()
{
    mvbtree = init_tree();
}

template<typename Key, typename T>
mvbtree_wrapper<Key,T>::~mvbtree_wrapper()
{
    destroy_tree_api(mvbtree);
}

template<typename Key, typename T>
bool mvbtree_wrapper<Key,T>::find(const char* key, size_t key_sz, char* value_out)
{
    return tree_api_find(mvbtree, key, key_sz, value_out);
}


template<typename Key, typename T>
bool mvbtree_wrapper<Key, T>::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    return tree_api_insert(mvbtree, key, key_sz, value, value_sz);
}

template<typename Key, typename T>
bool mvbtree_wrapper<Key, T>::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    return tree_api_update(mvbtree, key, key_sz, value, value_sz);
}

template<typename Key, typename T>
bool mvbtree_wrapper<Key,T>::remove(const char* key, size_t key_sz)
{
    return tree_api_remove(mvbtree, key, key_sz);
}

template<typename Key, typename T>
int mvbtree_wrapper<Key,T>::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    return tree_api_scan(mvbtree, key, key_sz, scan_sz, values_out);
}

#endif
