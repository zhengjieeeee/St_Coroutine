#include <unistd.h>
#include <errno.h>
#include <set>

#include "st_scheduler.h"
#include "st_coroutine.h"
#include "st_utils.h"

namespace St_Coroutine 
{
Scheduler::Scheduler()
    : _shutdown(false)
    , _main_ctx()
    , _epoll_fd(-1)
    , _current_coro(nullptr)
{
}

Scheduler::~Scheduler()
{
    std::set<Coroutine*> all;
    for (auto* coro : _ready_coroutines)
    {
        all.insert(coro);
    }
    for (auto& pair : _io_wait_map)
    {
        all.insert(pair.second);
    }
    for (auto& entry : _timers)
    {
        all.insert(entry.second);
    }
    if (_current_coro)
    {
        all.insert(_current_coro);
    }
    for (auto* coro : all)
    {
        delete coro;
    }
    _current_coro = nullptr;

    if (_epoll_fd != -1)
    {
        ::close(_epoll_fd);
        _epoll_fd = -1;
    }
}

bool Scheduler::init()
{
    _tls_scheduler = this;

    _epoll_fd = epoll_create1(0);
    if (_epoll_fd == -1) return false;

    return true;
}

Coroutine* Scheduler::spawn(std::function<void()> func, bool joinable, size_t stack_size)
{
    Coroutine* coro = new Coroutine(func, joinable, stack_size);
    _ready_coroutines.push_back(coro);
    return coro;
}

void Scheduler::run()
{
    while (!_shutdown)
    {
        if (_ready_coroutines.empty())
        {
            if (_timers.empty() && _io_wait_map.empty())
            {
                return;
            }

            int timeout = -1;
            if (!_timers.empty())
            {
                uint64_t deadline = _timers.begin()->first;
                uint64_t now = now_ms();
                timeout = deadline > now ? int(deadline - now) : 0;
            }
            epoll_wait(timeout);
            continue;
        }

        _current_coro = _ready_coroutines.front();
        _ready_coroutines.pop_front();

        if (_current_coro == nullptr) continue;

        swapcontext(&_main_ctx, &_current_coro->get_context());

        if (_current_coro->get_state() == CoroutineState::ZOMBIE)
        {
            if (!_current_coro->is_joinable())
            {
                delete _current_coro;
            }
            _current_coro = nullptr;
        }
    }
}

void Scheduler::epoll_wait(int timeout)
{
    epoll_event events[128];
    int ret = ::epoll_wait(_epoll_fd, events, 128, timeout);
    if (ret > 0)
    {
        process_io_events(events, ret);
    }
    process_timers();
}

void Scheduler::process_timers()
{
    uint64_t now = now_ms();

    while (!_timers.empty() && _timers.begin()->first <= now)
    {
        auto it = _timers.begin();
        Coroutine* coro = it->second;
        _timers.erase(it);
        coro->set_timer_deadline(0);

        if (coro->get_state() != CoroutineState::IO_WAIT && 
            coro->get_state() != CoroutineState::SLEEPING &&
            coro->get_state() != CoroutineState::COND_WAIT)
        {
            continue;
        }

        if (coro->get_state() == CoroutineState::IO_WAIT)
        {
            remove_io(coro);
            coro->set_timed_out(true);
        }

        coro->set_state(CoroutineState::RUNABLE);
        _ready_coroutines.push_back(coro);
    }
}

void Scheduler::process_io_events(epoll_event* events, int num_events)
{
    for (int i = 0; i < num_events; ++i)
    {
        int fd = events[i].data.fd;
        auto it = _io_wait_map.find(fd);
        if (it == _io_wait_map.end())
        {
            continue;
        }

        Coroutine* coro = it->second;
        _io_wait_map.erase(it);
        ::epoll_ctl(_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        coro->set_wait_fd(-1);

        remove_timer(coro);

        coro->set_state(CoroutineState::RUNABLE);
        _ready_coroutines.push_back(coro);
    }
}

bool Scheduler::sleep(uint64_t ms)
{
    if (!_current_coro) return false;

    add_timer(_current_coro, now_ms() + ms);

    _current_coro->set_state(CoroutineState::SLEEPING);

    swapcontext(&_current_coro->get_context(), &_main_ctx);

    if (_current_coro->is_interrupted())
    {
        _current_coro->set_interrupted(false);
        return false;
    }
    return true;
}

void Scheduler::yield()
{
    if (!_current_coro) return;

    _current_coro->set_state(CoroutineState::RUNABLE);
    _ready_coroutines.push_back(_current_coro);

    swapcontext(&_current_coro->get_context(), &_main_ctx);
}

void Scheduler::exit_current()
{
    if (!_current_coro) return;

    _current_coro->set_state(CoroutineState::ZOMBIE);

    swapcontext(&_current_coro->get_context(), &_main_ctx);
}

bool Scheduler::register_io(int fd, int op, uint64_t timeout_ms, Coroutine* coro)
{
    if (!coro) return false;
    
    epoll_event ev{};
    ev.events = op;
    ev.data.fd = fd;

    if (::epoll_ctl(_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1)
    {
        if (errno != EEXIST ||
            ::epoll_ctl(_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1)
        {
            return false;
        }
    }

    _io_wait_map[fd] = coro;
    coro->set_wait_fd(fd);

    if (timeout_ms > 0)
    {
        add_timer(coro, now_ms() + timeout_ms);
    }

    coro->set_state(CoroutineState::IO_WAIT);

    swapcontext(&coro->get_context(), &_main_ctx);

    if (coro->is_interrupted())
    {
        coro->set_interrupted(false);
        coro->set_timed_out(false);
        errno = EINTR;
        return false;
    }
    if (coro->is_timed_out())
    {
        coro->set_interrupted(false);
        coro->set_timed_out(false);
        errno = ETIMEDOUT;
        return false;
    }
    return true;
}

void Scheduler::wakeup(Coroutine* coro)
{
    if (!coro) return;
    coro->set_state(CoroutineState::RUNABLE);
    _ready_coroutines.push_back(coro);
}

void Scheduler::add_timer(Coroutine* coro, uint64_t deadline)
{
    if (!coro) return;
    coro->set_timer_deadline(deadline);
    _timers.insert({deadline, coro});
}

void Scheduler::remove_timer(Coroutine* coro)
{
    if (!coro || coro->get_timer_deadline() == 0) return;
    _timers.erase({coro->get_timer_deadline(), coro});
    coro->set_timer_deadline(0);
}

void Scheduler::remove_io(Coroutine* coro)
{
    if (!coro || coro->get_wait_fd() == -1) return;
    int fd = coro->get_wait_fd();
    ::epoll_ctl(_epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    _io_wait_map.erase(fd);
    coro->set_wait_fd(-1);
}

void Scheduler::shut_down()
{
    _shutdown = true;
}
} // namespace St_Coroutine
