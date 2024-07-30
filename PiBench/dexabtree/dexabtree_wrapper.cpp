#include "dexabtree_wrapper.hpp"

extern "C" tree_api* create_tree(const tree_options_t& opt)
{
    if (opt.key_size == 4)
    {
        if (opt.value_size == 4)
            return new dexabtree_wrapper<uint32_t, uint32_t>();
        else if (opt.value_size == 8)
            return new dexabtree_wrapper<uint32_t, uint64_t>();
        else if (opt.value_size > 8)
            return new dexabtree_wrapper<uint32_t, std::string>();
        else
            return nullptr;// ERROR
    }
    else if (opt.key_size == 8)
    {
        if (opt.value_size == 4)
            return new dexabtree_wrapper<uint64_t, uint32_t>();
        else if (opt.value_size == 8)
            return new dexabtree_wrapper<uint64_t, uint64_t>();
        else if (opt.value_size > 8)
            return new dexabtree_wrapper<uint64_t, std::string>();
        else
            return nullptr;// ERROR

    }
    else if (opt.key_size > 8)
    {
        if (opt.value_size == 4)
            return new dexabtree_wrapper<std::string, uint32_t>();
        else if (opt.value_size == 8)
            return new dexabtree_wrapper<std::string, uint64_t>();
        else if (opt.value_size > 8)
            return new dexabtree_wrapper<std::string, std::string>();
        else
            return nullptr ;// ERROR

    }
    else
        return nullptr; // ERROR!
}
