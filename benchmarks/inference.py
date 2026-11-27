"""Measure one streamed request at a time against an OpenAI-compatible TP server."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import statistics
import subprocess
import time
from urllib.request import Request, urlopen

PROMPT = "Explain how a checksum helps detect corruption during a file transfer."


def counters(host, binary, socket):
    command = shlex.join([binary, "monitor", "--socket", socket, "--links"])
    raw = subprocess.check_output(["ssh", "-o", "BatchMode=yes", host, command], timeout=15)
    report = json.loads(raw)
    if not report["fabric_ready"]:
        raise RuntimeError("fabric is unavailable")
    return report


def generate(args, prompt=PROMPT, max_tokens=64, **sampling):
    payload = dict(model=args.model, prompt=prompt, max_tokens=max_tokens, temperature=0,
                   stream=True, stream_options={"include_usage": True})
    payload.update(sampling)
    min_tokens = getattr(args, "min_tokens", 0)
    if min_tokens:
        payload["min_tokens"] = min_tokens
    request = Request(args.url.rstrip("/") + "/v1/completions", json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    pieces, usage, first, last, done = [], None, None, None, False
    start = time.perf_counter()
    with urlopen(request, timeout=120) as response:
        for line in response:
            if not line.startswith(b"data:"):
                continue
            event = line[5:].strip()
            if event == b"[DONE]":
                done = True
                break
            data = json.loads(event)
            if data.get("usage"):
                usage = data["usage"]
            for choice in data.get("choices", []):
                if choice.get("text"):
                    last = time.perf_counter()
                    if first is None:
                        first = last
                    pieces.append(choice["text"])
    end = time.perf_counter()
    if not done or not usage or usage["completion_tokens"] < 2 or first is None or last <= first:
        raise ValueError("incomplete stream or insufficient individually streamed output")
    return dict(input_tokens=usage["prompt_tokens"], output_tokens=usage["completion_tokens"],
                text_sha256=hashlib.sha256("".join(pieces).encode()).hexdigest(),
                ttft_ms=(first-start)*1000, total_ms=(end-start)*1000,
                decode_tokens_s=(usage["completion_tokens"]-1)/(last-first))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--transport", required=True, choices=["Socket", "Usb4Rdma"])
    parser.add_argument("--node", action="append", default=[], metavar="SSH_ALIAS")
    parser.add_argument("--daemon", default="usb4-rdma-ctl")
    parser.add_argument("--socket", default="/run/usb4-rdma/node.sock")
    parser.add_argument("--min-tokens", type=int, default=0,
                        help="SGLang minimum generated tokens, up to 64")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not 0 <= args.min_tokens <= 64:
        parser.error("--min-tokens must be between 0 and 64")
    if args.transport == "Usb4Rdma" and len(set(args.node)) != 2:
        parser.error("USB4 requires two distinct --node aliases with matching daemon paths")
    generate(args)
    rows = []
    for _ in range(5):
        before = [counters(h, args.daemon, args.socket) for h in args.node]
        row = generate(args)
        deltas = []
        for host, old in zip(args.node, before):
            new = counters(host, args.daemon, args.socket)
            if new["epoch"] != old["epoch"] or new["config_digest"] != old["config_digest"]:
                raise RuntimeError("fabric changed during inference")
            if len(new["links"]) != len(old["links"]):
                raise RuntimeError("link count changed")
            links = [{k: end[k]-beg[k] for k in ("submitted_payload_bytes", "received_payload_bytes")}
                     for beg,end in zip(old["links"],new["links"])]
            if any(v < 0 for link in links for v in link.values()) or not links:
                raise RuntimeError("invalid link counters")
            if any(sum(link[k] for link in links) <= 0 for k in links[0]):
                raise RuntimeError("missing bidirectional USB4 payload")
            deltas.append(links)
        row["payload_delta_by_node_and_link"] = deltas
        rows.append(row)
    metrics = {}
    for key in ("ttft_ms", "decode_tokens_s", "total_ms"):
        values = [r[key] for r in rows]
        metrics[key] = dict(median=statistics.median(values), min=min(values), max=max(values))
    result = dict(model=args.model, revision=args.revision, transport=args.transport,
                  prompt=PROMPT, max_tokens=64, min_tokens=args.min_tokens, temperature=0, measured_runs=rows, summary=metrics)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
