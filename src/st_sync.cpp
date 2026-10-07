#include <errno.h>

#include "st_sync.h"
#include "st_coroutine.h"
#include "st_scheduler.h"
#include "st_utils.h"

namespace St_Coroutine 
{
void Cond::wait()
{
    Scheduler* sche = Scheduler::get();
    Coroutine* self = sche->get_current_coro();
    if (!self) return;

    _waiters.insert(self);
    self->set_state(CoroutineState::COND_WAIT);
    swapcontext(&self->get_context(), &sche->get_main_context());

    if (self->is_interrupted())
    {
        self->set_interrupted(false);
        _waiters.erase(self);
    }
}

bool Cond::timedwait(uint64_t ms)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* self = sche->get_current_coro();
    if (!self) return false;

    _waiters.insert(self);
    sche->add_timer(self, now_ms() + ms);
    self->set_state(CoroutineState::COND_WAIT);
    swapcontext(&self->get_context(), &sche->get_main_context());

    if (self->is_interrupted())
    {
        self->set_interrupted(false);
        _waiters.erase(self);
        errno = EINTR;
        return false;
    }

    if (_waiters.erase(self) != 0)
    {
        errno = ETIMEDOUT;
        return false;
    }
    return true;
}

void Cond::signal()
{
    Scheduler* sche = Scheduler::get();
    for (auto it = _waiters.begin(); it != _waiters.end(); ++it)
    {
        Coroutine* coro = *it;
        if (coro->get_state() != CoroutineState::COND_WAIT)
            continue;
        _waiters.erase(it);
        sche->remove_timer(coro);
        sche->wakeup(coro);
        return;
    }
}

void Cond::broadcast()
{
    Scheduler* sche = Scheduler::get();
    for (auto it = _waiters.begin(); it != _waiters.end(); )
    {
        Coroutine* coro = *it;
        if (coro->get_state() != CoroutineState::COND_WAIT)
        {
            ++it;
            continue;
        }
        it = _waiters.erase(it);
        sche->remove_timer(coro);
        sche->wakeup(coro);
    }
}

void Mutex::lock()
{
    Scheduler* sche = Scheduler::get();
    Coroutine* self = sche->get_current_coro();
    if (!self) return;

    if (_owner == nullptr)
    {
        _owner = self;
        return;
    }
    if (_owner == self) return;

    _waiters.push_back(self);
    self->set_state(CoroutineState::LOCK_WAIT);
    swapcontext(&self->get_context(), &sche->get_main_context());
}

bool Mutex::trylock()
{
    Coroutine* self = Scheduler::get()->get_current_coro();
    if (!self || _owner != nullptr) return false;
    _owner = self;
    return true;
}

void Mutex::unlock()
{
    Scheduler* sche = Scheduler::get();
    Coroutine* self = sche->get_current_coro();
    if (_owner != self) return;

    _owner = nullptr;
    if (!_waiters.empty())
    {
        Coroutine* coro = _waiters.front();
        _waiters.pop_front();
        _owner = coro;
        sche->wakeup(coro);
    }
}
} // namespace St_Coroutine
