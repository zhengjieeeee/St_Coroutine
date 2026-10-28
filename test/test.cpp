#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "st_scheduler.h"
#include "st_coroutine.h"
#include "st_sync.h"
#include "st_io.h"

using namespace St_Coroutine;

static void test_yield()
{
    printf("==== test_yield ====\n");
    Scheduler sched;
    sched.init();

    for (int i = 0; i < 3; ++i)
    {
        sched.spawn([&sched, i] {
            for (int j = 0; j < 2; ++j)
            {
                printf("coro %d, round %d\n", i, j);
                sched.yield();
            }
        });
    }
    sched.run();
}

static void test_sleep()
{
    printf("==== test_sleep ====\n");
    Scheduler sched;
    sched.init();

    sched.spawn([&sched] { sched.sleep(300); printf("wake after 300ms\n"); });
    sched.spawn([&sched] { sched.sleep(100); printf("wake after 100ms\n"); });
    sched.spawn([&sched] { sched.sleep(200); printf("wake after 200ms\n"); });

    sched.run();
}

static void test_join()
{
    printf("==== test_join ====\n");
    Scheduler sched;
    sched.init();

    int result = 0;
    Coroutine* worker = sched.spawn([&sched, &result] {
        sched.sleep(200);
        result = 42;
    }, true);
    sched.spawn([worker, &result] {
        worker->join();
        printf("join got result=%d (expect 42)\n", result);
    });

    Coroutine* quick = sched.spawn([] {}, true);
    sched.spawn([&sched, quick] {
        sched.sleep(300);
        quick->join();
        printf("join already-dead coroutine ok\n");
    });

    Coroutine* shared = sched.spawn([&sched] { sched.sleep(400); }, true);
    for (int i = 0; i < 3; ++i)
    {
        sched.spawn([&sched, shared, i] {
            sched.sleep(i * 100);
            shared->join();
            printf("multi-joiner %d joined\n", i);
        });
    }

    sched.run();
}

static void test_interrupt()
{
    printf("==== test_interrupt ====\n");
    Scheduler sched;
    sched.init();

    Coroutine* sleeper = sched.spawn([&sched] {
        bool full = sched.sleep(10000);
        printf("sleep: %s (expect interrupted)\n", full ? "full" : "interrupted");
    });
    sched.spawn([&sched, sleeper] {
        sched.sleep(200);
        sleeper->interrupt();
    });

    sched.run();
}

static void test_cond()
{
    printf("==== test_cond ====\n");
    Scheduler sched;
    sched.init();

    Cond cond;
    sched.spawn([&cond] {
        cond.wait();
        printf("wait got signal\n");
    });
    sched.spawn([&sched, &cond] {
        sched.sleep(200);
        cond.signal();
    });

    Cond cond2;
    sched.spawn([&cond2] {
        bool ok = cond2.timedwait(100);
        printf("timedwait no signal: %s (expect timeout)\n", ok ? "signaled" : "timeout");
    });

    Cond cond3;
    sched.spawn([&sched, &cond3] { sched.sleep(100); cond3.signal(); });
    sched.spawn([&cond3] {
        bool ok = cond3.timedwait(2000);
        printf("timedwait with signal: %s (expect signaled)\n", ok ? "signaled" : "timeout");
    });

    sched.run();
}

static void test_mutex()
{
    printf("==== test_mutex ====\n");
    Scheduler sched;
    sched.init();

    Mutex mtx;
    int counter = 0;
    for (int i = 0; i < 3; ++i)
    {
        sched.spawn([&sched, &mtx, &counter] {
            for (int j = 0; j < 2; ++j)
            {
                mtx.lock();
                int tmp = counter;
                sched.yield();
                counter = tmp + 1;
                mtx.unlock();
            }
        });
    }

    sched.run();
    printf("counter=%d (expect 6)\n", counter);
}

static void test_io()
{
    printf("==== test_io ====\n");
    Scheduler sched;
    sched.init();

    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == -1)
    {
        printf("socketpair failed\n");
        return;
    }
    fcntl(fds[0], F_SETFL, O_NONBLOCK);
    fcntl(fds[1], F_SETFL, O_NONBLOCK);

    sched.spawn([fds] {
        char buf[64];
        ssize_t n = st_io_recv(fds[1], buf, sizeof(buf));
        printf("recv %zd bytes: %.*s\n", n, (int)n, buf);
    });
    sched.spawn([&sched, fds] {
        sched.sleep(200);
        const char* msg = "hello coroutine io";
        ssize_t n = st_io_send(fds[0], msg, strlen(msg));
        printf("sent %zd bytes\n", n);
    });

    sched.run();
    close(fds[0]);
    close(fds[1]);
}

static void test_io_timeout()
{
    printf("==== test_io_timeout ====\n");
    Scheduler sched;
    sched.init();

    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == -1)
    {
        printf("socketpair failed\n");
        return;
    }
    fcntl(fds[0], F_SETFL, O_NONBLOCK);
    fcntl(fds[1], F_SETFL, O_NONBLOCK);

    sched.spawn([fds] {
        char buf[64];
        ssize_t n = st_io_recv(fds[1], buf, sizeof(buf), 200);
        printf("recv timeout: ret=%zd errno=%d %s (expect -1 ETIMEDOUT)\n",
               n, errno, strerror(errno));
    });

    sched.run();
    close(fds[0]);
    close(fds[1]);
}

int main()
{
    test_yield();
    test_sleep();
    test_join();
    test_interrupt();
    test_cond();
    test_mutex();
    test_io();
    test_io_timeout();

    printf("all tests done\n");
    return 0;
}
