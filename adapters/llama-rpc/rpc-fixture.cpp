#include "ggml.h"
#include "ggml-alloc.h"
#include "ggml-backend.h"
#include "ggml-cpu.h"
#include "ggml-rpc.h"

#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

int main(int argc, char **argv) {
    if (argc == 3 && std::strcmp(argv[1], "--server") == 0) {
        ggml_backend_dev_t cpu = ggml_backend_reg_dev_get(ggml_backend_cpu_reg(), 0);
        assert(cpu);
        ggml_backend_rpc_start_server(argv[2], nullptr, 1, 1, &cpu);
        return 1;
    }
    assert(argc == 2);
    ggml_backend_t backend = ggml_backend_rpc_init(argv[1], 0);
    assert(backend);
    ggml_init_params params{1024 * 1024, nullptr, true};
    ggml_context *ctx = ggml_init(params);
    assert(ctx);
    constexpr size_t count = 512 * 1024;
    auto *left = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, count);
    auto *right = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, count);
    auto *a = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 32);
    auto *b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 32);
    auto *sum = ggml_add(ctx, a, b);
    auto *graph = ggml_new_graph(ctx);
    ggml_build_forward_expand(graph, sum);
    auto buffer = ggml_backend_alloc_ctx_tensors(ctx, backend);
    assert(buffer);

    std::vector<float> x(count, 3.5f), y(count, -8.0f), out(count);
    std::thread send_left([&] { ggml_backend_tensor_set(left, x.data(), 0, x.size() * sizeof(float)); });
    std::thread send_right([&] { ggml_backend_tensor_set(right, y.data(), 0, y.size() * sizeof(float)); });
    send_left.join();
    send_right.join();
    ggml_backend_tensor_get(left, out.data(), 0, out.size() * sizeof(float));
    assert(out == x);
    ggml_backend_tensor_get(right, out.data(), 0, out.size() * sizeof(float));
    assert(out == y);

    std::this_thread::sleep_for(std::chrono::seconds(35));
    std::array<float, 32> small_x, small_y, small_result;
    small_x.fill(11.0f);
    small_y.fill(42.0f);
    ggml_backend_tensor_set(a, small_x.data(), 0, sizeof(small_x));
    ggml_backend_tensor_set(b, small_y.data(), 0, sizeof(small_y));
    assert(ggml_backend_graph_compute(backend, graph) == GGML_STATUS_SUCCESS);
    ggml_backend_tensor_get(sum, small_result.data(), 0, sizeof(small_result));
    for (float value : small_result) {
        assert(value == 53.0f);
    }
    ggml_backend_buffer_free(buffer);
    ggml_free(ctx);
    ggml_backend_free(backend);
    std::puts("ggml-rpc concurrent tensor load and graph after 35 seconds idle passed");
}
