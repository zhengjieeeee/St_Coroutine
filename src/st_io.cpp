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

int st_io_connect(int fd, const struct sockaddr* addr, socklen_t addrlen, uint64_t timeout_ms)
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

int st_io_listen(const struct sockaddr* addr, socklen_t addrlen, int backlog)
{
    int fd = ::socket(addr->sa_family, SOCK_STREAM, 0);
    if (fd == -1) return -1;

    int reuse = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1)
    {
        ::close(fd);
        return -1;
    }

    if (::bind(fd, addr, addrlen) == -1)
    {
        ::close(fd);
        return -1;
    }

    if (::listen(fd, backlog) == -1)
    {
        ::close(fd);
        return -1;
    }

    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags == -1 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        ::close(fd);
        return -1;
    }

    return fd;
}
} // namespace St_Coroutine
