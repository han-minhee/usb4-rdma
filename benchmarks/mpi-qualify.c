/* SPDX-License-Identifier: MIT
 * Compile with an ordinary mpicc. No transport-specific APIs are used.
 */
#define _POSIX_C_SOURCE 200809L
#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <unistd.h>

static int rank, ranks;
static const size_t sizes[] = {8, 4096, 8192, 16384, 65536, 1048576, 4194304,
                               16777216, 67108864};

static void fail(const char *what, int line)
{
    fprintf(stderr, "rank %d: %s at line %d\n", rank, what, line);
    MPI_Abort(MPI_COMM_WORLD, 1);
    exit(EXIT_FAILURE);
}

#define CHECK(expression) do { if (!(expression)) fail(#expression, __LINE__); } while (0)
#define MPI_OK(expression) CHECK((expression) == MPI_SUCCESS)

static unsigned char value(int source, unsigned int sequence, size_t offset)
{
    return (unsigned char)((source + 3u) * 97u ^ sequence * 17u ^ offset ^ (offset >> 12));
}

static void fill(unsigned char *buffer, size_t count, unsigned int sequence)
{
    for (size_t i = 0; i < count; ++i)
        buffer[i] = value(rank, sequence, i);
}

static void verify(const unsigned char *buffer, size_t count, int source, unsigned int sequence)
{
    for (size_t i = 0; i < count; ++i)
        CHECK(buffer[i] == value(source, sequence, i));
}

static void ring(size_t bytes, unsigned int sequence, unsigned char *send, unsigned char *recv)
{
    int next = (rank + 1) % ranks, prev = (rank + ranks - 1) % ranks;
    fill(send, bytes, sequence);
    MPI_Request ops[2];
    MPI_Status status[2];
    MPI_OK(MPI_Irecv(recv, (int)bytes, MPI_BYTE, MPI_ANY_SOURCE, 7, MPI_COMM_WORLD, &ops[0]));
    MPI_OK(MPI_Isend(send, (int)bytes, MPI_BYTE, next, 7, MPI_COMM_WORLD, &ops[1]));
    MPI_OK(MPI_Waitall(2, ops, status));
    int count;
    MPI_OK(MPI_Get_count(&status[0], MPI_BYTE, &count));
    CHECK(status[0].MPI_SOURCE == prev && count == (int)bytes);
    verify(recv, bytes, prev, sequence);
}

static void correctness(unsigned char *send, unsigned char *recv)
{
    int value = rank + 7, received = -1;
    MPI_OK(MPI_Sendrecv(&value, 1, MPI_INT, rank, 8, &received, 1, MPI_INT, rank, 8,
                        MPI_COMM_WORLD, MPI_STATUS_IGNORE));
    CHECK(received == value);

    const int next = (rank + 1) % ranks, prev = (rank + ranks - 1) % ranks;
    MPI_Request send_request;
    MPI_Status status;
    MPI_Message message;
    MPI_OK(MPI_Isend(&value, 1, MPI_INT, next, 9, MPI_COMM_WORLD, &send_request));
    MPI_OK(MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status));
    CHECK(status.MPI_TAG == 9 && status.MPI_SOURCE == prev);
    MPI_OK(MPI_Mprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message, &status));
    CHECK(status.MPI_TAG == 9 && status.MPI_SOURCE == prev);
    MPI_OK(MPI_Mrecv(&received, 1, MPI_INT, &message, &status));
    MPI_OK(MPI_Wait(&send_request, MPI_STATUS_IGNORE));
    CHECK(received == prev + 7);

    MPI_Request cancel;
    MPI_OK(MPI_Irecv(&received, 1, MPI_INT, prev, 44, MPI_COMM_WORLD, &cancel));
    MPI_OK(MPI_Cancel(&cancel));
    MPI_OK(MPI_Wait(&cancel, &status));
    int canceled = 0;
    MPI_OK(MPI_Test_cancelled(&status, &canceled));
    CHECK(canceled);

    int pair[2] = {rank, rank + 1};
    MPI_OK(MPI_Isend(pair, 2, MPI_INT, next, 10, MPI_COMM_WORLD, &send_request));
    int rc = MPI_Recv(&received, 1, MPI_INT, prev, 10, MPI_COMM_WORLD, &status);
    int error_class = 0;
    MPI_OK(MPI_Error_class(rc, &error_class));
    CHECK(error_class == MPI_ERR_TRUNCATE);
    MPI_OK(MPI_Wait(&send_request, MPI_STATUS_IGNORE));

    /* Test FIFO matching when an MPI application reuses its tag. */
    MPI_Request transmit[8], receive[8];
    int ordered[8];
    for (int i = 0; i < 8; ++i) {
        pair[0] = 0;
        ordered[i] = -1;
        MPI_OK(MPI_Irecv(&ordered[i], 1, MPI_INT, prev, 11, MPI_COMM_WORLD, &receive[i]));
    }
    int values[8];
    for (int i = 0; i < 8; ++i) {
        values[i] = rank * 100 + i;
        MPI_OK(MPI_Isend(&values[i], 1, MPI_INT, next, 11, MPI_COMM_WORLD, &transmit[i]));
    }
    MPI_OK(MPI_Waitall(8, receive, MPI_STATUSES_IGNORE));
    MPI_OK(MPI_Waitall(8, transmit, MPI_STATUSES_IGNORE));
    for (int i = 0; i < 8; ++i)
        CHECK(ordered[i] == prev * 100 + i);

    /* Cross the OFI credit limit with eager, empty and rendezvous messages. */
    enum { BURST = 40, STRIDE = 16385 };
    MPI_Request burst[BURST];
    for (int i = 0; i < BURST; ++i) {
        int bytes = i % 3 == 0 ? 0 : STRIDE - (i % 3 == 1);
        unsigned char *message_start = send + (size_t)i * STRIDE;
        fill(message_start, (size_t)bytes, (unsigned int)i);
        MPI_OK(MPI_Isend(message_start, bytes, MPI_BYTE, next, 12, MPI_COMM_WORLD, &burst[i]));
    }
    for (int i = 0; i < BURST; ++i) {
        int count, bytes = i % 3 == 0 ? 0 : STRIDE - (i % 3 == 1);
        MPI_OK(MPI_Probe(prev, MPI_ANY_TAG, MPI_COMM_WORLD, &status));
        MPI_OK(MPI_Get_count(&status, MPI_BYTE, &count));
        CHECK(status.MPI_TAG == 12 && count == bytes);
        MPI_OK(MPI_Recv(recv, bytes, MPI_BYTE, prev, 12, MPI_COMM_WORLD, MPI_STATUS_IGNORE));
        verify(recv, (size_t)bytes, prev, (unsigned int)i);
    }
    MPI_OK(MPI_Waitall(BURST, burst, MPI_STATUSES_IGNORE));

    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i)
        ring(sizes[i], (unsigned int)i, send, recv);
    int sum = 0;
    MPI_OK(MPI_Allreduce(&rank, &sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD));
    CHECK(sum == ranks * (ranks - 1) / 2);
    int gathered[8] = {0};
    MPI_OK(MPI_Allgather(&rank, 1, MPI_INT, gathered, 1, MPI_INT, MPI_COMM_WORLD));
    for (int i = 0; i < ranks; ++i)
        CHECK(gathered[i] == i);
    received = rank == ranks - 1 ? 173 : 0;
    MPI_OK(MPI_Bcast(&received, 1, MPI_INT, ranks - 1, MPI_COMM_WORLD));
    CHECK(received == 173);
    MPI_OK(MPI_Barrier(MPI_COMM_WORLD));
    if (rank == 0) {
        printf("MPI qualification passed, ranks=%d\n", ranks);
        fflush(stdout);
    }
}

static void soak(double seconds, unsigned char *send, unsigned char *recv)
{
    const double start = MPI_Wtime();
    if (rank == 0) {
        puts("MPI soak started");
        fflush(stdout);
    }
    unsigned int rounds = 0;
    int continue_run;
    do {
        for (int i = 0; i < 8; ++i) {
            ring(sizes[(rounds / 2) % 6], rounds, send, recv);
            ++rounds;
        }
        continue_run = rank == 0 && MPI_Wtime() - start < seconds;
        MPI_OK(MPI_Bcast(&continue_run, 1, MPI_INT, 0, MPI_COMM_WORLD));
    } while (continue_run);
    if (rank == 0)
        printf("MPI soak passed, rounds=%u seconds=%.2f\n", rounds, MPI_Wtime() - start);
}

static int compare_samples(const void *left, const void *right)
{
    const double a = *(const double *)left, b = *(const double *)right;
    return (a > b) - (a < b);
}

static double cpu_seconds(void)
{
    struct rusage usage;
    CHECK(getrusage(RUSAGE_SELF, &usage) == 0);
    return (double)usage.ru_utime.tv_sec + (double)usage.ru_stime.tv_sec +
           ((double)usage.ru_utime.tv_usec + (double)usage.ru_stime.tv_usec) / 1000000.0;
}

static void performance(unsigned char *send, unsigned char *recv)
{
    CHECK(ranks == 2);
    if (rank == 0)
        puts("metric,bytes,iterations,p50_us,p95_us,p99_us,gbits_s");
    for (size_t s = 0; s < sizeof(sizes) / sizeof(sizes[0]); ++s) {
        int bytes = (int)sizes[s], iterations = bytes <= 65536 ? 200 : bytes < 16777216 ? 64 : 16;
        double samples[200];
        fill(send, (size_t)bytes, 0);
        MPI_OK(MPI_Barrier(MPI_COMM_WORLD));
        for (int i = -4; i < iterations; ++i) {
            double start = MPI_Wtime();
            if (rank == 0) {
                MPI_OK(MPI_Send(send, bytes, MPI_BYTE, 1, 12, MPI_COMM_WORLD));
                MPI_OK(MPI_Recv(recv, bytes, MPI_BYTE, 1, 12, MPI_COMM_WORLD, MPI_STATUS_IGNORE));
            } else {
                MPI_OK(MPI_Recv(recv, bytes, MPI_BYTE, 0, 12, MPI_COMM_WORLD, MPI_STATUS_IGNORE));
                MPI_OK(MPI_Send(send, bytes, MPI_BYTE, 0, 12, MPI_COMM_WORLD));
            }
            if (i >= 0)
                samples[i] = (MPI_Wtime() - start) * 500000.0;
        }
        verify(recv, (size_t)bytes, 1 - rank, 0);
        if (rank == 0) {
            qsort(samples, (size_t)iterations, sizeof(double), compare_samples);
            printf("half-rtt,%d,%d,%.3f,%.3f,%.3f,\n", bytes, iterations,
                   samples[(iterations + 1) / 2 - 1], samples[(95 * iterations + 99) / 100 - 1],
                   samples[(99 * iterations + 99) / 100 - 1]);
        }
        if (s < 4)
            continue;
        MPI_OK(MPI_Barrier(MPI_COMM_WORLD));
        double start = MPI_Wtime(), elapsed = 0;
        for (int i = 0; i < iterations; ++i) {
            if (rank == 0)
                MPI_OK(MPI_Send(send, bytes, MPI_BYTE, 1, 13, MPI_COMM_WORLD));
            else
                MPI_OK(MPI_Recv(recv, bytes, MPI_BYTE, 0, 13, MPI_COMM_WORLD, MPI_STATUS_IGNORE));
        }
        double duration = MPI_Wtime() - start;
        MPI_OK(MPI_Reduce(&duration, &elapsed, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD));
        if (rank == 0)
            printf("one-way,%d,%d,,,,%.3f\n", bytes, iterations, bytes * (double)iterations * 8e-9 / elapsed);
        else
            verify(recv, (size_t)bytes, 0, 0);
    }
}

int main(int argc, char **argv)
{
    int support = 0;
    if (MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &support) != MPI_SUCCESS)
        return EXIT_FAILURE;
    MPI_OK(MPI_Comm_rank(MPI_COMM_WORLD, &rank));
    MPI_OK(MPI_Comm_size(MPI_COMM_WORLD, &ranks));
    CHECK(support >= MPI_THREAD_FUNNELED && ranks >= 2 && ranks <= 8);
    MPI_OK(MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN));
    const double cpu_start = cpu_seconds();
    const size_t max_size = sizes[sizeof(sizes) / sizeof(sizes[0]) - 1];
    unsigned char *send = malloc(max_size), *recv = malloc(max_size);
    CHECK(send && recv);
    if (argc == 1 || (argc == 2 && strcmp(argv[1], "--correctness") == 0))
        correctness(send, recv);
    else if (argc == 2 && strcmp(argv[1], "--idle-check") == 0) {
        MPI_OK(MPI_Barrier(MPI_COMM_WORLD));
        sleep(12);
        correctness(send, recv);
    } else if (argc == 2 && strcmp(argv[1], "--bench") == 0)
        performance(send, recv);
    else if (argc == 3 && strcmp(argv[1], "--soak") == 0) {
        char *end;
        double seconds = strtod(argv[2], &end);
        CHECK(*end == '\0' && seconds > 0 && seconds <= 7200);
        soak(seconds, send, recv);
    } else
        fail("expected --correctness, --idle-check, --bench, or --soak SECONDS", __LINE__);
    fprintf(stderr, "cpu_seconds,%d,%.6f\n", rank, cpu_seconds() - cpu_start);
    free(recv);
    free(send);
    MPI_OK(MPI_Finalize());
    return EXIT_SUCCESS;
}
