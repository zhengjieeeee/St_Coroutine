#pragma once

#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <vector>

namespace St_Coroutine 
{
inline uint64_t now_ms() 
{
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

class free_list
{
public:
    struct alignas(16) chunk
    {
        chunk* next;
    };

    explicit free_list(size_t count = 16, size_t chunk_size = 64 * 1024)
        : _chunk_size(chunk_size)
    {
        grow(count);
    }

    ~free_list()
    {
        for (auto* slab : _slabs) free(slab);
    }

    size_t chunk_size() const { return _chunk_size; }

    char* alloc()
    {
        if (!_head) grow(16);
        chunk* c = _head;
        _head = c->next;
        return (char*)(c + 1);
    }

    void dealloc(char* data)
    {
        chunk* c = ((chunk*)data) - 1;
        c->next = _head;
        _head = c;
    }

private:
    void grow(size_t count)
    {
        size_t total = count * (sizeof(chunk) + _chunk_size);
        char* buf = (char*)malloc(total);
        _slabs.push_back(buf);

        for (size_t i = 0; i < count; ++i)
        {
            chunk* c = (chunk*)(buf + i * (sizeof(chunk) + _chunk_size));
            c->next = _head;
            _head = c;
        }
    }

private:
    chunk* _head = nullptr;
    size_t _chunk_size;
    std::vector<char*> _slabs;  
};
} // namespace St_Coroutine
