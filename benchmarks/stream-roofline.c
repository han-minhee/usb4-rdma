/* SPDX-License-Identifier: MIT
 * See NOTICE for source attribution.
 * Measure a Linux USB4STREAM character device with generated host data.
 * The coordinator creates GO on both hosts after both READY files appear.
 */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MIB UINT64_C(1048576)
#define TIMEOUT_SECONDS 20.0
#define PING_WARMUP 40
#define CHECK(expression)                                                      \
    do {                                                                       \
        if (!(expression)) {                                                   \
            fprintf(stderr, "line %d: %s (errno=%d)\n", __LINE__, #expression, \
                    errno);                                                    \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    } while (0)

static double monotonic_seconds(void)
{
    struct timespec time;
    CHECK(clock_gettime(CLOCK_MONOTONIC, &time) == 0);
    return time.tv_sec + time.tv_nsec * 1e-9;
}

static double cpu_seconds(void)
{
    struct rusage usage;
    CHECK(getrusage(RUSAGE_SELF, &usage) == 0);
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec * 1e-6 +
           usage.ru_stime.tv_sec + usage.ru_stime.tv_usec * 1e-6;
}

static void transfer(int fd, unsigned char *buffer, size_t remaining,
                     bool sending, bool busy_poll)
{
    const double deadline = monotonic_seconds() + TIMEOUT_SECONDS;
    while (remaining > 0) {
        ssize_t count = sending ? write(fd, buffer, remaining)
                                : read(fd, buffer, remaining);
        if (count > 0) {
            buffer += count;
            remaining -= (size_t)count;
            continue;
        }
        CHECK(count < 0 && (errno == EAGAIN || errno == EINTR));
        CHECK(monotonic_seconds() < deadline);
        if (busy_poll)
            continue;
        struct pollfd event = {.fd = fd, .events = sending ? POLLOUT : POLLIN};
        int status = poll(&event, 1, 10);
        CHECK(status >= 0 || errno == EINTR);
        CHECK(!(event.revents & (POLLERR | POLLNVAL)));
    }
}

static uint64_t number(const char *text, uint64_t min, uint64_t max)
{
    char *end;
    errno = 0;
    unsigned long long value = strtoull(text, &end, 10);
    CHECK(errno == 0 && text[0] >= '0' && text[0] <= '9' && *end == '\0');
    CHECK(value >= min && value <= max);
    return value;
}

static int compare_latency(const void *a, const void *b)
{
    const double left = *(const double *)a;
    const double right = *(const double *)b;
    return (left > right) - (left < right);
}

int main(int argc, char **argv)
{
    const bool busy_poll = argc == 8 && strcmp(argv[7], "--busy-poll") == 0;
    if (argc != 7 && !busy_poll) {
        fprintf(stderr, "usage: %s DEVICE send|recv READY GO MiB BLOCK_BYTES [--busy-poll]\n"
                        "       %s DEVICE ping|pong READY GO ITERS BLOCK_BYTES [--busy-poll]\n",
                argv[0], argv[0]);
        return EXIT_FAILURE;
    }
    const bool ping = strcmp(argv[2], "ping") == 0;
    const bool pong = strcmp(argv[2], "pong") == 0;
    const bool round_trip = ping || pong;
    const bool sending = ping || strcmp(argv[2], "send") == 0;
    CHECK(sending || pong || strcmp(argv[2], "recv") == 0);
    const size_t block = (size_t)number(argv[6], round_trip ? 8 : 4096, 4 * MIB);
    const uint64_t count = number(argv[5], 1, round_trip ? 65536 : 8192);
    CHECK(round_trip || (count * MIB) % block == 0);
    const uint64_t rounds = round_trip ? count + PING_WARMUP : count * MIB / block;
    double *latencies = ping ? malloc(count * sizeof(*latencies)) : NULL;
    CHECK(!ping || latencies);

    unsigned char *data = malloc(block);
    unsigned char *expected = malloc(block);
    CHECK(data && expected);
    for (size_t i = 0; i < block; ++i)
        expected[i] = (unsigned char)((i * 13 + (i >> 8) * 7) ^ 0x5a);
    memcpy(data, expected, block);

    int fd = open(argv[1], O_RDWR | O_NONBLOCK | O_CLOEXEC);
    struct stat device;
    CHECK(fd >= 0 && fstat(fd, &device) == 0 && S_ISCHR(device.st_mode));
    int ready = open(argv[3], O_WRONLY | O_CREAT | O_EXCL, 0600);
    CHECK(ready >= 0 && close(ready) == 0);
    const double deadline = monotonic_seconds() + TIMEOUT_SECONDS;
    while (access(argv[4], F_OK) != 0) {
        CHECK(errno == ENOENT && monotonic_seconds() < deadline);
        const struct timespec delay = {.tv_nsec = 1000000};
        nanosleep(&delay, NULL);
    }

    unsigned char ack[8] = "U4RDOK!";
    const double start = monotonic_seconds();
    const double cpu_start = cpu_seconds();
    for (uint64_t sequence = 0; sequence < rounds; ++sequence) {
        for (size_t i = 0; i < sizeof(sequence); ++i)
            expected[i] = (unsigned char)(sequence >> (i * 8));
        if (sending)
            memcpy(data, expected, sizeof(sequence));
        const double sent_at = ping ? monotonic_seconds() : 0.0;
        transfer(fd, data, block, sending, busy_poll);
        if (ping) {
            transfer(fd, data, block, false, busy_poll);
            const double half_rtt_us = (monotonic_seconds() - sent_at) * 5e5;
            if (sequence >= PING_WARMUP)
                latencies[sequence - PING_WARMUP] = half_rtt_us;
        }
        if (!sending || ping)
            CHECK(memcmp(data, expected, block) == 0);
        if (pong)
            transfer(fd, data, block, true, busy_poll);
    }
    if (sending) {
        unsigned char received[sizeof(ack)];
        transfer(fd, received, sizeof(received), false, busy_poll);
        CHECK(memcmp(received, ack, sizeof(ack)) == 0);
    } else {
        transfer(fd, ack, sizeof(ack), true, busy_poll);
    }
    const double seconds = monotonic_seconds() - start;
    const double cpu = cpu_seconds() - cpu_start;
    const uint64_t total = rounds * block;
    printf("{\"mode\":\"%s\",\"bytes\":%" PRIu64 ",\"block\":%zu,"
           "\"seconds\":%.9f,\"GBps\":%.6f,\"cpu_seconds\":%.6f,\"verified\":true",
           argv[2], total, block, seconds, total / seconds / 1e9, cpu);
    if (ping) {
        qsort(latencies, count, sizeof(*latencies), compare_latency);
        printf(",\"p50_half_rtt_us\":%.3f,\"p99_half_rtt_us\":%.3f",
               latencies[(count * 50 + 99) / 100 - 1],
               latencies[(count * 99 + 99) / 100 - 1]);
    }
    puts("}");
    fflush(stdout);

    /* Keep the receiver open while the sender consumes the acknowledgment. */
    if (!sending) {
        const struct timespec delay = {.tv_nsec = 200000000};
        nanosleep(&delay, NULL);
    }
    CHECK(close(fd) == 0);
    free(data);
    free(expected);
    free(latencies);
    return EXIT_SUCCESS;
}
