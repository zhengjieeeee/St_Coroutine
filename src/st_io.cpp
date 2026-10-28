#include "st_io.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
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

ssize_t st_io_recvfrom(int fd, void* buf, size_t len, sockaddr* from, socklen_t* fromlen)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* coro = sche->get_current_coro();
    if (coro == nullptr) return -1;

    for (;;) 
    {
        ssize_t ret = ::recvfrom(fd, buf, len, 0, from, fromlen);
        if (ret >= 0) return ret;
        if (errno == EINTR) continue;
        if (errno != EAGAIN) return -1;

        if (!sche->register_io(fd, EPOLLIN, 0, coro))
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

int st_io_connect(int fd, const struct ::sockaddr* addr, socklen_t addrlen, uint64_t timeout_ms)
{
    Scheduler* sche = Scheduler::get();
    Coroutine* coro = sche->get_current_coro();
    if (coro == nullptr) return -1;

    for (;;)
    {
        int ret = ::connect(fd, addr, addrlen);
        if (ret == 0) return 0;
        if (errno == EINTR) continue;
        if (errno == EISCONN) return 0;
        if (errno == EINPROGRESS || errno == EALREADY || errno == EAGAIN)
        {
            if (!sche->register_io(fd, EPOLLOUT, timeout_ms, coro))
            {
                return -1;
            }
            continue;
        }
        return -1;
    }
}
} // namespace St_Coroutine
