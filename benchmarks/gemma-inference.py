"""Measure the Gemma 4 31B text-serving benchmark over an OpenAI-compatible API."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from statistics import median
import time

from inference import generate


SHORT = "Explain how a checksum helps detect corruption during a file transfer. Give a concrete example."
SECTIONS = (
    "A distributed archive stores a manifest of each object and its checksum. The sender divides files into chunks.",
    "The receiver verifies a chunk before committing it to disk. A failed verification triggers a retry and a record in the audit log.",
    "Two independent hosts may observe different errors when a link is intermittent. The transfer report separates corruption, loss, and duplication.",
    "For large objects the verifying process can stream input, keep a fixed-size buffer, and update the digest without loading the whole file.",
)
LONG = "\n".join(f"Operational note {i + 1}. {SECTIONS[i % len(SECTIONS)]}" for i in range(35))
CASES = (("short-c1", SHORT, 1),
         ("long-c1", LONG + "\nSummarize the procedure in ordered steps.", 1),
         ("short-c4", SHORT, 4))
OUTPUT_TOKENS = 64


def request(args, prompt):
    row = generate(args, prompt, OUTPUT_TOKENS)
    if row["output_tokens"] != OUTPUT_TOKENS:
        raise ValueError("server returned an unexpected output token count")
    row["ttft_s"] = row.pop("ttft_ms") / 1000
    row["total_s"] = row.pop("total_ms") / 1000
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--transport", required=True, choices=["Socket", "Usb4Rdma"])
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.min_tokens = OUTPUT_TOKENS

    request(args, "Summarize the steps involved in hashing a file.")
    reports = []
    for case, prompt, concurrency in CASES:
        prompts = [f"{prompt}\nUse example number {index + 1}." for index in range(concurrency)]
        rounds = []
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for _ in range(3):
                start = time.perf_counter()
                requests = list(pool.map(lambda value: request(args, value), prompts))
                rounds.append({"requests": requests,
                               "batch_tokens_s": concurrency * OUTPUT_TOKENS / (time.perf_counter() - start)})
        flat = [row for batch in rounds for row in batch["requests"]]
        summary = {field: median(row[field] for row in flat)
                   for field in ("ttft_s", "total_s", "decode_tokens_s")}
        summary["batch_tokens_s"] = median(batch["batch_tokens_s"] for batch in rounds)
        reports.append({"case": case, "concurrency": concurrency, "summary": summary, "rounds": rounds})
        args.output.write_text(json.dumps({
            "model": args.model, "revision": args.revision, "transport": args.transport,
            "weights": "bf16", "output_tokens": OUTPUT_TOKENS, "reports": reports,
        }, indent=2) + "\n")
        print(case, summary, flush=True)


if __name__ == "__main__":
    main()
