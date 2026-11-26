"""Benchmark fixed token streams on an OpenAI-compatible dummy-weight server."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time

from inference import generate


def request(args, size, trial, client, output_tokens):
    prompt = [(101 + offset * 37 + trial * 19 + client * 13) % 20_000 + 300
              for offset in range(size)]
    result = generate(args, prompt, output_tokens, ignore_eos=True,
                      skip_special_tokens=False, seed=args.seed)
    if (result["input_tokens"], result["output_tokens"]) != (size, output_tokens):
        raise ValueError("server returned unexpected input or output token counts")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--transport", required=True, choices=["Socket", "Usb4Rdma"])
    parser.add_argument("--fixture", required=True, help="Describe dummy-weight dtype and geometry")
    parser.add_argument("--seed", default=777, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    request(args, 32, 777, 0, 8)
    reports = []
    for size in (32, 1024):
        for concurrency in (1, 4):
            trials = []
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                list(pool.map(lambda client: request(args, size, 777, client, 64),
                              range(concurrency)))
                for trial in range(3):
                    start = time.perf_counter()
                    outputs = list(pool.map(
                        lambda client: request(args, size, trial, client, 64), range(concurrency)
                    ))
                    trials.append({
                        "requests": outputs,
                        "throughput_tokens_s": concurrency * 64 / (time.perf_counter() - start),
                    })
            reports.append({"input_tokens": size, "concurrency": concurrency, "trials": trials})
            args.output.write_text(json.dumps({
                "model": args.model, "revision": args.revision, "transport": args.transport,
                "fixture": args.fixture, "weights": "dummy", "seed": args.seed,
                "output_tokens": 64, "reports": reports,
            }, indent=2) + "\n")


if __name__ == "__main__":
    main()
