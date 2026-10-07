#include "st_io.h"

#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>

#include "st_coroutine.h"
#include "st_scheduler.h"

namespace St_Coroutine 
{
ssize_t st_io_recv(int fd, void* buf, size_t len, uint64_t timeout_ms)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* coro = sche->get_current_coro();
    if (coro == nullptr) return -1;

    for (;;)
    {
        ssize_t ret = ::recv(fd, buf, len, 0);
        if (ret >= 0) return ret;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) return -1;

        if (!sche->register_io(fd, EPOLLIN, timeout_ms, coro))
        {
            return -1;
        }
    }
}

ssize_t st_io_send(int fd, const void* buf, size_t len, uint64_t timeout_ms)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* coro = sche->get_current_coro();
    if (coro == nullptr) return -1;

    for (;;)
    {
        ssize_t ret = ::send(fd, buf, len, 0);
        if (ret >= 0) return ret;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) return -1;

        if (!sche->register_io(fd, EPOLLOUT, timeout_ms, coro))
        {
            return -1;
        }
    }
}

int st_io_accept(int fd, uint64_t timeout_ms)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* coro = sche->get_current_coro();
    if (coro == nullptr) return -1;

    for (;;)
    {
        int ret = ::accept(fd, nullptr, nullptr);
        if (ret >= 0) return ret;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) return -1;

        if (!sche->register_io(fd, EPOLLIN, timeout_ms, coro))
        {
            return -1;
        }
    }
}
} // namespace St_Coroutine
