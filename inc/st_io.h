#pragma once

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/socket.h>

namespace St_Coroutine 
{
ssize_t st_io_recv(int fd, void* buf, size_t len, uint64_t timeout_ms = 0);

ssize_t st_io_recvfrom(int fd, void* buf, size_t len, sockaddr* from, socklen_t* fromlen);

ssize_t st_io_send(int fd, const void* buf, size_t len, uint64_t timeout_ms = 0);

int st_io_accept(int fd, uint64_t timeout_ms = 0);

int st_io_connect(int fd, const struct ::sockaddr* addr, socklen_t addrlen, uint64_t timeout_ms = 0);
} // namespace St_Coroutine
