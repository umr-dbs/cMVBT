#ifndef __DEXABTREE_WRAPPER_HPP__
#define __DEXABTREE_WRAPPER_HPP__

#include "tree_api.hpp"
#include "dexabtree_wrapper.h"
#include <cstdint>
#include <iostream>
#include <type_traits>
#include <map>
#include <cstring>
#include <array>
#include <mutex>
#include <shared_mutex>

template<typename Key, typename T>
class dexabtree_wrapper : public tree_api
{
public:
    dexabtree_wrapper();
    virtual ~dexabtree_wrapper();
    
    virtual bool find(const char* key, size_t key_sz, char* value_out) override;
    virtual bool insert(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool update(const char* key, size_t key_sz, const char* value, size_t value_sz) override;
    virtual bool remove(const char* key, size_t key_sz) override;
    virtual int scan(const char* key, size_t key_sz, int scan_sz, char*& values_out) override;

private:
    void* dexabtree;
};

template<typename Key, typename T>
dexabtree_wrapper<Key,T>::dexabtree_wrapper()
{
    /*
        pub const ORWC: c_int   = 0;
        pub const OLC: c_int    = 1;
        pub const LHL: c_int    = 2;
        pub const MONO: c_int   = 3;
        pub const HL: c_int     = 4;
        pub const LC: c_int     = 5;
     */
    dexabtree = init_tree(0, 4, 0);
}

template<typename Key, typename T>
dexabtree_wrapper<Key,T>::~dexabtree_wrapper()
{
    destroy_tree_api(dexabtree);
}

template<typename Key, typename T>
bool dexabtree_wrapper<Key,T>::find(const char* key, size_t key_sz, char* value_out)
{
    return tree_api_find(dexabtree, key, key_sz, value_out);
}


template<typename Key, typename T>
bool dexabtree_wrapper<Key, T>::insert(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    return tree_api_insert(dexabtree, key, key_sz, value, value_sz);
}

template<typename Key, typename T>
bool dexabtree_wrapper<Key, T>::update(const char* key, size_t key_sz, const char* value, size_t value_sz)
{
    return tree_api_update(dexabtree, key, key_sz, value, value_sz);
}

template<typename Key, typename T>
bool dexabtree_wrapper<Key,T>::remove(const char* key, size_t key_sz)
{
    return tree_api_remove(dexabtree, key, key_sz);
}

template<typename Key, typename T>
int dexabtree_wrapper<Key,T>::scan(const char* key, size_t key_sz, int scan_sz, char*& values_out)
{
    return tree_api_scan(dexabtree, key, key_sz, scan_sz, values_out);
}

#endif
