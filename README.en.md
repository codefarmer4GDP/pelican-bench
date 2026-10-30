# pelican-bench

A neutral, open-source quality benchmarking tool for **any OpenAI-compatible API endpoint** (official or relayed). It measures streaming TTFB, non-streaming response completeness, prompt-cache hit signals, and the inventory of usage (billing) fields.

- Zero third-party dependencies: runs on the Python 3.9+ standard library (`urllib` / `json` / `time` / `statistics`)
- Endpoint-neutral: no built-in vendor endpoints, keys, or model names — everything is supplied via arguments
- Observability only: no speculation, no ratings, no certification of any service

## Quick Start

```bash
python3 pelican-bench.py \
    --base-url https://api.example.com/v1 \
    --api-key YOUR_API_KEY \
    --model your-model-name \
    --rounds 5 \
    --json report.json

# Or via the thin wrapper (checks for python3 first)
./pelican-bench.sh --base-url https://api.example.com/v1 \
    --api-key YOUR_API_KEY --model your-model-name

# The API key can also come from an environment variable
export PELICAN_BENCH_API_KEY="YOUR_API_KEY"
```

`api.example.com` above is a placeholder — replace it with whatever endpoint you want to test. The tool appends `/chat/completions` to `--base-url`.

## Tests & How to Read Them

1. **Streaming TTFB distribution** — `rounds` streaming requests; TTFB is measured from request start to the first SSE chunk carrying a non-empty `content` delta; reports p50/p95/min/max/mean. It reflects how long users wait for the first token. It does not measure throughput or mid-stream stability.
2. **Non-streaming completeness** — per-round checks: HTTP 2xx, parseable JSON, non-empty `choices[0].message.content`, `finish_reason` present, `usage` present; reports a pass rate. Structural checks only, not answer quality.
3. **Cache A/B probe** — sends the same ~2000-token fixed filler prefix twice and compares `usage`. Verdicts are exactly three: *hit signal detected* / *no hit observed* / *fields missing, undeterminable*. It is a signal, not proof — server-side cache policy may vary by prefix, account, or time window.
4. **Usage field inventory** — lists the usage keys actually present in responses (cache-related ones flagged), to help you verify billing semantics. Absent keys do not mean unsupported.

## Exit Codes

| Code | Meaning |
| --- | --- |
| 0 | all tests passed |
| 1 | partial failures |
| 2 | argument or connection error |

## Relationship to test-pelican.sh

pelican-bench's measurement approach originated from the public script [test-pelican.sh](https://github.com/codefarmer4GDP/awesome-claude-code-relay). The original script targets a single service's "pelican test" scenario: one-shot verification of a relay's Claude model output and a single first-token latency measurement. pelican-bench generalizes that idea into a standalone benchmark for **any OpenAI-compatible endpoint**, adding:

- multi-round TTFB distribution statistics (p50 / p95 / mean, not single-shot);
- a cache-hit A/B probe and a usage-field inventory;
- machine-readable JSON reports and CI-friendly exit codes.

Beyond the shared measurement lineage, the two projects share no code; pelican-bench embeds no vendor endpoints, keys, or model names.

## FAQ

- **Does it cost money?** Yes. Every request is a real billed request — about `rounds × 2 + 2` in total, including one ~2000-token cache probe pair. Use your own key.
- **Is any data uploaded anywhere?** No. No telemetry, no third-party libraries; requests go only to the `--base-url` you provide. The cache probe uses locally generated repetitive filler text.

## Disclaimer

> Results are affected by your local network, the time of day, and the target service's routing. This tool only measures observable behavior and does not constitute a quality certification of any service.

Provided as-is under the MIT License (see [LICENSE](LICENSE)), without warranty of any kind, express or implied. Users bear all costs and consequences arising from running this tool.

---

## Related Tools & Selection Guide

- [AI API Selection Guide & Cost Calculator](https://github.com/codefarmer4GDP/awesome-ai-api-cn): Curated directory of aggregators, relays, member-session sharing, and official APIs with pricing comparisons, constraints, and evaluation templates.
- [Online Two-Plan Cost Calculator](https://codefarmer4gdp.github.io/awesome-ai-api-cn/index.en.html#calculator): Compare estimated cash costs across two quotes for the same workload, factoring in prompt cache hit rates and top-up fees.

---

Maintained by [TokenDos](https://tokendos.com). This tool is not bound to TokenDos and can benchmark any OpenAI-compatible endpoint.
