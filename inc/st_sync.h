#pragma once

#include <stdint.h>
#include <set>
#include <deque>

namespace St_Coroutine 
{
class Coroutine;

class Cond 
{
public:
    void wait();
    bool timedwait(uint64_t ms);
    void signal();
    void broadcast();

private:
    std::set<Coroutine*> _waiters;
};

class Mutex 
{
public:
    void lock();
    bool trylock();
    void unlock();

private:
    Coroutine* _owner = nullptr;
    std::deque<Coroutine*> _waiters;
};
} // namespace St_Coroutine
