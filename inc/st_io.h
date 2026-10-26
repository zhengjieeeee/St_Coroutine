#pragma once

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

namespace St_Coroutine 
{
ssize_t st_io_recv(int fd, void* buf, size_t len, uint64_t timeout_ms = 0);

ssize_t st_io_send(int fd, const void* buf, size_t len, uint64_t timeout_ms = 0);

int st_io_accept(int fd, uint64_t timeout_ms = 0);

int st_io_connect(int fd, const struct sockaddr* addr, socklen_t addrlen, uint64_t timeout_ms = 0);

int st_io_listen(const struct sockaddr* addr, socklen_t addrlen, int backlog = 128);
} // namespace St_Coroutine
