#include <sys/mman.h>

#include "st_coroutine.h"

namespace St_Coroutine 
{
void Coroutine::trampoline()
{
    Coroutine* coro = Scheduler::get()->get_current_coro();
    if (coro)
    {
        if (coro->get_state() == CoroutineState::RUNABLE)
        {
            coro->set_state(CoroutineState::RUNNING);
            coro->run();
            coro->set_state(CoroutineState::ZOMBIE);
            for (auto* joiner : coro->_joiners)
            {
                Scheduler::get()->wakeup(joiner);
            }
            swapcontext(&coro->get_context(), &Scheduler::get()->get_main_context());
        }
    }
}

Coroutine::Coroutine(std::function<void()> func, bool joinable, size_t stack_size)
    : _func(std::move(func))
    , _joinable(joinable)
    , _stack_size(stack_size)
    , _stack(nullptr)
    , _ctx()
    , _state(CoroutineState::RUNABLE)
{
    free_list& pool = Scheduler::get()->get_stack_pool();
    if (_stack_size == pool.chunk_size())
    {
        _stack = pool.alloc();
        _pool = &pool;
    }
    else
    {
        _stack = (char*)mmap(nullptr, _stack_size, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    }

    getcontext(&_ctx);

    _ctx.uc_stack.ss_sp = _stack;
    _ctx.uc_stack.ss_size = _stack_size;
    _ctx.uc_link = nullptr;

    makecontext(&_ctx, trampoline, 0);
}

Coroutine::~Coroutine()
{
    if (_pool)
        _pool->dealloc(_stack);
    else
        munmap(_stack, _stack_size);
}

void Coroutine::run()
{
    if (_func) _func();
}

bool Coroutine::join()
{
    Scheduler* sche = Scheduler::get();
    Coroutine* self = sche->get_current_coro();
    if (self == nullptr || self == this) return false;

    if (_state != CoroutineState::ZOMBIE)
    {
        _joiners.insert(self);
        self->set_state(CoroutineState::COND_WAIT);
        swapcontext(&self->get_context(), &sche->get_main_context());

        _joiners.erase(self);
        if (self->is_interrupted())
        {
            self->set_interrupted(false);
            return false;
        }
    }

    if (_joiners.empty()) delete this;

    return true;
}

void Coroutine::interrupt()
{
    _interrupted = true;

    Scheduler* sche = Scheduler::get();
    sche->remove_timer(this);
    sche->remove_io(this);

    if (_state == CoroutineState::IO_WAIT ||
        _state == CoroutineState::SLEEPING ||
        _state == CoroutineState::COND_WAIT ||
        _state == CoroutineState::SUSPENDED)
    {
        sche->wakeup(this);
    }
}
} // namespace St_Coroutine
