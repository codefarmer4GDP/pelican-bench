#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pelican-bench — 中立的开源质量基准测试工具（任意 OpenAI 兼容 API 端点）。

设计原则：
  * 中立：不内置任何厂商端点、密钥或模型名，一切由参数提供。
  * 零依赖：仅用 Python 3.9+ 标准库（urllib/json/time/statistics）。
  * 只测可观测行为，不臆测：字段缺失时明确输出「无法判定」。

测试项：
  1. 流式 TTFB 分布 —— stream=true，测首个携带非空 content 增量的 SSE 块到达时间；
  2. 非流式完整性 —— HTTP 2xx / JSON 可解析 / content 非空 / finish_reason / usage；
  3. 缓存 A/B 探测 —— 相同约 2000 token 长前缀连发两次，比较 usage 缓存相关字段；
  4. usage 字段清单 —— 列出响应中实际出现的 usage 键（含缓存相关）。

注意：所有请求均为真实计费请求（rounds N 总计约 2N+2 次）。
免责声明见文末 DISCLAIMER 与 README。
"""

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

TOOL_NAME = "pelican-bench"
TOOL_VERSION = "0.1.0"
DISCLAIMER = ("结果受本机网络环境、时段与目标服务路由影响；"
              "本工具只测量可观测行为，不构成任何服务的质量认证。")
EXIT_OK, EXIT_PARTIAL, EXIT_ERROR = 0, 1, 2

# 常见缓存读命中学段名（不同兼容端点风格不同），仅记录实际出现的
CACHE_FIELD_PRIORITY = ("prompt_tokens_details.cached_tokens", "cached_tokens",
                        "cache_read_input_tokens", "prompt_cache_hit_tokens")


class BenchConnectionError(Exception):
    """连接层错误（DNS 失败、拒绝连接、超时等），退出码 2。"""


# ---------------------------- HTTP 封装（urllib） ----------------------------

def build_headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": "Bearer " + api_key, "Content-Type": "application/json",
            "User-Agent": TOOL_NAME + "/" + TOOL_VERSION, "Accept": "application/json"}


def http_post_json(base_url: str, api_key: str, payload: Dict[str, Any],
                   timeout: float) -> Tuple[int, bytes]:
    """非流式 POST，返回 (status, body)。连接层错误抛 BenchConnectionError。"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(base_url.rstrip("/") + "/chat/completions", data=data,
                                 headers=build_headers(api_key), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:  # HTTP 层错误读出 body 供诊断
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, body
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise BenchConnectionError("连接错误: %r" % (e,)) from e


def http_post_stream(base_url: str, api_key: str, payload: Dict[str, Any], timeout: float):
    """流式 POST，返回已建立未读取的响应对象（调用方逐块读取）。"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = build_headers(api_key)
    headers["Accept"] = "text/event-stream"
    req = urllib.request.Request(base_url.rstrip("/") + "/chat/completions", data=data,
                                 headers=headers, method="POST")
    try:
        return urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise BenchConnectionError("流式请求 HTTP 错误: %r" % (e,)) from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise BenchConnectionError("连接错误: %r" % (e,)) from e


# ------------------------------- 通用工具 -------------------------------

def dist_stats(values: List[float]) -> Dict[str, Any]:
    """对一组秒值计算 count/min/max/mean/p50/p95（p95 用 statistics.quantiles）。"""
    if not values:
        return {}
    vs = sorted(values)
    st: Dict[str, Any] = {"count": len(vs), "min_s": vs[0], "max_s": vs[-1],
                          "mean_s": statistics.fmean(vs), "p50_s": statistics.median(vs)}
    # n=20 → 切点为 5%,...,95%；第 19 个切点即 95 分位（样本数不足时退化为单值）
    st["p95_s"] = statistics.quantiles(vs, n=20, method="inclusive")[18] if len(vs) >= 2 else vs[0]
    return st


def fmt_ms(seconds: Optional[float]) -> str:
    return "-" if seconds is None else "%.1f ms" % (seconds * 1000.0)


def json_safe(obj: Any) -> Any:
    """转为可 JSON 序列化结构（错误对象转字符串）。"""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    return repr(obj)


def extract_error_snippet(body: bytes, limit: int = 300) -> str:
    text = " ".join(body.decode("utf-8", errors="replace").strip().split())
    return text[:limit] + "..." if len(text) > limit else text


# --------------------------- 测试 1：流式 TTFB ---------------------------

def run_stream_round(base_url: str, api_key: str, model: str, timeout: float) -> Dict[str, Any]:
    """单轮流式测试：记录首字节、首 content 增量块、总时长。"""
    payload = {"model": model, "stream": True, "max_tokens": 128,
               "messages": [{"role": "user", "content": "用一句话解释什么是首字延迟（TTFB）。"}]}
    result: Dict[str, Any] = {"ok": False, "error": None, "ttfb_first_byte_s": None,
                              "ttfb_first_content_s": None, "total_s": None, "http_status": None}
    start = time.perf_counter()
    try:
        resp = http_post_stream(base_url, api_key, payload, timeout)
    except BenchConnectionError as e:
        result["error"] = str(e)
        result["total_s"] = time.perf_counter() - start
        return result
    result["http_status"] = getattr(resp, "status", None)
    first_byte: Optional[float] = None
    first_content: Optional[float] = None
    buf = b""
    try:
        while True:
            chunk = resp.read(1024)
            if not chunk:
                break
            if first_byte is None:
                first_byte = time.perf_counter() - start
            buf += chunk
            # 逐行扫 SSE：只消费不解析完整；命中非空 content 增量即记 TTFB
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                data_str = line.decode("utf-8", errors="replace").strip()
                if not data_str.startswith("data:"):
                    continue
                data_str = data_str[len("data:"):].strip()
                if data_str == "[DONE]" or first_content is not None:
                    continue  # [DONE] 只识别；首块已命中则其余行只消费
                try:
                    event = json.loads(data_str)
                except ValueError:
                    continue
                choices = event.get("choices") or []
                if choices and isinstance(choices[0], dict):
                    delta = choices[0].get("delta") or {}
                    if isinstance(delta.get("content"), str) and delta["content"] != "":
                        first_content = time.perf_counter() - start
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        result["error"] = "流式读取中断: %r" % (e,)
    finally:
        try:
            resp.close()
        except Exception:
            pass
    result["total_s"] = time.perf_counter() - start
    result["ttfb_first_byte_s"] = first_byte
    result["ttfb_first_content_s"] = first_content
    result["ok"] = first_content is not None  # 见到首内容增量块即算本轮成功
    if not result["ok"] and result["error"] is None:
        result["error"] = "未收到任何 content 增量块"
    return result


def test_stream_ttfb(base_url: str, api_key: str, model: str,
                     rounds: int, timeout: float) -> Dict[str, Any]:
    rounds_out = []
    for i in range(rounds):
        r = run_stream_round(base_url, api_key, model, timeout)
        r["round"] = i + 1
        rounds_out.append(r)
    success = [r for r in rounds_out if r["ok"]]
    return {
        "name": "stream_ttfb", "rounds": rounds_out,
        "success_count": len(success), "failure_count": rounds - len(success),
        "ttfb_stats": dist_stats([r["ttfb_first_content_s"] for r in success
                                  if r["ttfb_first_content_s"] is not None]),
        "total_stats": dist_stats([r["total_s"] for r in success
                                   if r["total_s"] is not None]),
        "pass": rounds > 0 and len(success) == rounds,
        "partial": 0 < len(success) < rounds,
    }


# ------------------------- 测试 2：非流式完整性 -------------------------

def run_plain_round(base_url: str, api_key: str, model: str, timeout: float) -> Dict[str, Any]:
    payload = {"model": model, "max_tokens": 32,
               "messages": [{"role": "user", "content": "回答一个词即可：1+1=?"}]}
    result: Dict[str, Any] = {"ok": False, "error": None, "http_status": None,
                              "checks": {}, "usage_keys": [], "total_s": None}
    start = time.perf_counter()
    try:
        status, body = http_post_json(base_url, api_key, payload, timeout)
    except BenchConnectionError as e:
        result["error"] = str(e)
        result["total_s"] = time.perf_counter() - start
        return result
    result["http_status"] = status
    result["total_s"] = time.perf_counter() - start
    checks: Dict[str, Optional[bool]] = {
        "http_2xx": 200 <= status < 300, "json_parseable": False,
        "content_nonempty": False, "finish_reason_present": False, "usage_present": False}
    try:
        parsed = json.loads(body.decode("utf-8"))
        checks["json_parseable"] = True
    except (ValueError, UnicodeDecodeError):
        parsed = None
    if isinstance(parsed, dict):
        choices = parsed.get("choices") or []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message") or {}
            content = msg.get("content")
            checks["content_nonempty"] = isinstance(content, str) and content.strip() != ""
            checks["finish_reason_present"] = bool(choices[0].get("finish_reason"))
        usage = parsed.get("usage")
        if isinstance(usage, dict) and usage:
            checks["usage_present"] = True
            result["usage_keys"] = sorted(str(k) for k in usage)
    result["checks"] = checks
    result["ok"] = all(bool(v) for v in checks.values())
    if not result["ok"]:
        if not checks["http_2xx"]:
            result["error"] = "HTTP %s: %s" % (status, extract_error_snippet(body))
        else:
            result["error"] = "未通过: " + ", ".join(k for k, v in checks.items() if not v)
    return result


def test_plain_completeness(base_url: str, api_key: str, model: str,
                            rounds: int, timeout: float) -> Dict[str, Any]:
    rounds_out = []
    for i in range(rounds):
        r = run_plain_round(base_url, api_key, model, timeout)
        r["round"] = i + 1
        rounds_out.append(r)
    n_ok = sum(1 for r in rounds_out if r["ok"])
    return {"name": "plain_completeness", "rounds": rounds_out,
            "success_count": n_ok, "failure_count": rounds - n_ok,
            "pass_rate": (n_ok / rounds) if rounds else 0.0,
            "pass": rounds > 0 and n_ok == rounds,
            "partial": 0 < n_ok < rounds}


# ------------------------- 测试 3：缓存 A/B 探测 -------------------------

def build_cache_probe_content() -> str:
    """约 2000 token 的固定长前缀（重复中性填充文本，不含敏感内容）。"""
    unit = ("基准测试填充文本。 pelican-bench neutral filler text for cache probing. "
            "本段落用于构造稳定的长前缀，不包含任何真实或敏感内容。 "
            "Repeated neutral sentences are used to reach a fixed token count. ")
    return unit * 48  # 中英混排约 40~50 token/次，共约 2000 token


def extract_cache_tokens(usage: Any) -> Dict[str, Any]:
    """从 usage 提取缓存相关字段（若存在），不臆测、不补默认值。"""
    out: Dict[str, Any] = {"found": False, "field": None, "value": None, "candidates": {}}
    if not isinstance(usage, dict):
        return out
    ptd = usage.get("prompt_tokens_details")
    if isinstance(ptd, dict) and "cached_tokens" in ptd:
        candidates = {"prompt_tokens_details.cached_tokens": ptd.get("cached_tokens")}
    else:
        candidates = {}
    for key in ("cached_tokens", "cache_read_input_tokens",
                "prompt_cache_hit_tokens", "cache_creation_input_tokens"):
        if key in usage:
            candidates[key] = usage.get(key)
    out["candidates"] = candidates
    if candidates:
        out["found"] = True
        for prefer in CACHE_FIELD_PRIORITY:
            if prefer in candidates:
                out["field"], out["value"] = prefer, candidates[prefer]
                break
    return out


def run_cache_probe(base_url: str, api_key: str, model: str, timeout: float) -> Dict[str, Any]:
    """连发两次完全相同的长前缀请求，比较 usage，输出三态结论（不臆测）。"""
    payload = {"model": model, "max_tokens": 8,
               "messages": [{"role": "user",
                             "content": build_cache_probe_content() + "\n\n请只回复：OK"}]}
    usages: List[Dict[str, Any]] = []
    errors: List[str] = []
    for idx in (1, 2):
        try:
            status, body = http_post_json(base_url, api_key, payload, timeout)
            if 200 <= status < 300:
                parsed = json.loads(body.decode("utf-8"))
                usage = parsed.get("usage") if isinstance(parsed, dict) else None
                usages.append(usage if isinstance(usage, dict) else {})
                continue
            errors.append("第 %d 次请求 HTTP %s: %s" % (idx, status, extract_error_snippet(body)))
        except BenchConnectionError as e:
            errors.append("第 %d 次请求连接错误: %s" % (idx, e))
        except (ValueError, UnicodeDecodeError):
            errors.append("第 %d 次响应不是合法 JSON" % idx)
        usages.append({})
    a, b = usages[0], usages[1]
    b_cache = extract_cache_tokens(b)
    result: Dict[str, Any] = {
        "name": "cache_probe", "errors": errors,
        "attempts": [{"attempt": i + 1, "prompt_tokens": u.get("prompt_tokens"),
                      "completion_tokens": u.get("completion_tokens"),
                      "usage_keys": sorted(str(k) for k in u),
                      "cache_fields": extract_cache_tokens(u)["candidates"]}
                     for i, u in enumerate(usages)],
        "second_cache_field": b_cache}
    if errors:
        result.update(verdict="error", verdict_text="请求失败，无法判定（" + "; ".join(errors) + "）",
                      pass_=False, partial=True)
    elif not extract_cache_tokens(a)["found"] and not b_cache["found"]:
        result.update(verdict="no_field",
                      verdict_text="两次响应的 usage 中均未出现缓存相关字段（如 "
                                   "prompt_tokens_details.cached_tokens / "
                                   "cache_read_input_tokens），字段缺失无法判定。",
                      pass_=True, partial=False)
    else:
        value = b_cache["value"]
        if isinstance(value, (int, float)) and value > 0:
            result.update(verdict="hit",
                          verdict_text="检测到缓存命中迹象：第二次请求 %s = %s（> 0）。"
                                       % (b_cache["field"], value))
        else:
            result.update(verdict="no_hit",
                          verdict_text="未检测到缓存命中：第二次请求 %s = %s（不大于 0）。"
                                       % (b_cache["field"], value if value is not None else 0))
        result.update(pass_=True, partial=False)  # 命中/未见命中都是有效观测，不算失败
    # pass_ 是合法 Python 标识符，但输出键名统一为 pass
    result["pass"] = result.pop("pass_")
    return result


# ------------------ 测试 4：usage 字段清单（复用前两项响应） ------------------

def test_usage_inventory(plain_result: Dict[str, Any],
                         cache_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    key_lists = [r["usage_keys"] for r in plain_result.get("rounds", []) if r.get("usage_keys")]
    if cache_result:
        key_lists += [u["usage_keys"] for u in cache_result.get("attempts", []) if u.get("usage_keys")]
    all_keys: List[str] = []
    for ks in key_lists:
        all_keys += [k for k in ks if k not in all_keys]
    return {"name": "usage_inventory",
            "keys": [{"key": k, "cache_related": "cache" in k.lower()} for k in all_keys],
            "found": bool(all_keys), "pass": True}


# ---------------------------- 人读 markdown 报告 ----------------------------

def verdict_of(t: Dict[str, Any]) -> str:
    return "PASS" if t.get("pass") else ("PARTIAL" if t.get("partial") else "FAIL")


def md_stream_section(t: Dict[str, Any]) -> List[str]:
    lines = ["## 1. 流式 TTFB 分布", ""]
    s = t["ttfb_stats"]
    if s:
        lines += ["| 指标 | 数值 |", "| --- | --- |",
                  "| 成功轮数 | %d/%d |" % (s["count"], len(t["rounds"])),
                  "| TTFB p50 | %s |" % fmt_ms(s["p50_s"]),
                  "| TTFB p95 | %s |" % fmt_ms(s["p95_s"]),
                  "| TTFB 最小 | %s |" % fmt_ms(s["min_s"]),
                  "| TTFB 最大 | %s |" % fmt_ms(s["max_s"]),
                  "| TTFB 均值 | %s |" % fmt_ms(s["mean_s"]),
                  "", "（TTFB 口径：自请求发出至第一个携带非空 content 增量的 SSE 块到达。）", ""]
    else:
        lines += ["> 本项未获得有效数据（%d/%d 轮失败）。" % (t["failure_count"], len(t["rounds"])), ""]
    lines += ["| 轮次 | 结果 | 首内容块 | 总时长 | 备注 |", "| --- | --- | --- | --- | --- |"]
    for r in t["rounds"]:
        lines.append("| %d | %s | %s | %s | %s |" % (
            r["round"], "PASS" if r["ok"] else "FAIL",
            fmt_ms(r["ttfb_first_content_s"]), fmt_ms(r["total_s"]),
            "正常" if r["ok"] else (r.get("error") or "失败")))
    return lines + [""]


def md_plain_section(t: Dict[str, Any]) -> List[str]:
    lines = ["## 2. 非流式完整性", "",
             "通过率: **%.0f%%**（%d/%d）" % (t["pass_rate"] * 100, t["success_count"], len(t["rounds"])),
             "", "| 轮次 | 结果 | HTTP | JSON | content | finish_reason | usage |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    for r in t["rounds"]:
        c = r.get("checks", {})
        mark = lambda k: "-" if c.get(k) is None else ("Y" if c[k] else "N")  # noqa: E731
        status = r.get("http_status")
        lines.append("| %d | %s | %s | %s | %s | %s | %s |" % (
            r["round"], "PASS" if r["ok"] else "FAIL", status if status is not None else "-",
            mark("json_parseable"), mark("content_nonempty"),
            mark("finish_reason_present"), mark("usage_present")))
    lines.append("")
    if t["failure_count"]:
        first_err = next((r.get("error") for r in t["rounds"] if r.get("error")), "")
        lines += ["> 失败示例: %s" % first_err, ""]
    return lines


def md_cache_section(t: Dict[str, Any]) -> List[str]:
    lines = ["## 3. 缓存 A/B 探测", "",
             "方法：同一约 2000 token 固定长前缀连发两次相同请求，比较 usage。", "",
             "| 次数 | prompt_tokens | completion_tokens | 缓存相关字段 |",
             "| --- | --- | --- | --- |"]
    for u in t.get("attempts", []):
        cf = u.get("cache_fields") or {}
        cf_text = ", ".join("%s=%s" % kv for kv in sorted(cf.items())) if cf else "（无）"
        lines.append("| 第 %d 次 | %s | %s | %s |" % (
            u["attempt"], u.get("prompt_tokens") if u.get("prompt_tokens") is not None else "-",
            u.get("completion_tokens") if u.get("completion_tokens") is not None else "-", cf_text))
    icon = {"hit": "[迹象]", "no_hit": "[未见]", "no_field": "[无法判定]", "error": "[错误]"}.get(t["verdict"], "")
    return lines + ["", "**结论: %s %s**" % (icon, t["verdict_text"]), ""]


def md_usage_section(t: Dict[str, Any]) -> List[str]:
    if not t["found"]:
        return ["## 4. usage 字段清单", "", "> 未在响应中观察到 usage 对象。", ""]
    lines = ["## 4. usage 字段清单", "", "| 字段 | 缓存相关 |", "| --- | --- |"]
    lines += ["| `%s` | %s |" % (k["key"], "是" if k["cache_related"] else "-") for k in t["keys"]]
    return lines + ["", "> 该清单仅为本次响应中实际出现的键，供核对计费口径参考；",
                    "> 未出现的键不代表端点不支持，可能与模型、网关或计费策略配置有关。", ""]


def build_markdown_report(meta, stream, plain, cache, usage, overall) -> str:
    lines = ["# pelican-bench 质量基准报告", "",
             "- 目标端点: `%s`" % meta["base_url"],
             "- 模型: `%s`" % meta["model"],
             "- 轮次: %d    超时: %ds    流式测试: %s    缓存探测: %s" % (
                 meta["rounds"], meta["timeout"],
                 "关" if meta["no_stream"] else "开", "关" if not meta["cache_test"] else "开"),
             "- 工具版本: %s    时间: %s" % (TOOL_VERSION, meta["timestamp"]),
             "", "## 总览", "", "| 测试项 | 结果 |", "| --- | --- |"]
    if stream is not None:
        lines.append("| 流式 TTFB | %s（%d/%d 轮） |" % (verdict_of(stream), stream["success_count"], len(stream["rounds"])))
    lines.append("| 非流式完整性 | %s（%d/%d 轮） |" % (verdict_of(plain), plain["success_count"], len(plain["rounds"])))
    if cache is not None:
        lines.append("| 缓存 A/B 探测 | %s（%s） |" % (verdict_of(cache), cache["verdict"]))
    lines.append("| usage 字段清单 | PASS（%s） |" % ("%d 个键" % len(usage["keys"]) if usage["found"] else "未观察到 usage"))
    lines += ["", "**总体结论: %s**（%s）" % (overall["verdict"], overall["text"]), "", "---", ""]
    if stream is not None:
        lines += md_stream_section(stream)
    lines += md_plain_section(plain)
    if cache is not None:
        lines += md_cache_section(cache)
    lines += md_usage_section(usage)
    return "\n".join(lines + ["---", "", "> " + DISCLAIMER, ""])


# ------------------------------- 主流程 -------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog=TOOL_NAME,
        description="中立的开源质量基准测试工具：对任意 OpenAI 兼容 API 端点测量流式 TTFB、"
                    "非流式完整性、缓存命中迹象与 usage 字段清单。所有请求均为真实计费请求，会产生费用。",
        epilog="示例: pelican-bench.py --base-url https://api.example.com/v1 "
               "--api-key $KEY --model your-model --rounds 5 --json report.json")
    p.add_argument("--base-url", required=True,
                   help="OpenAI 兼容端点基础 URL，如 https://api.example.com/v1（必填）")
    p.add_argument("--api-key", default=os.environ.get("PELICAN_BENCH_API_KEY"),
                   help="API 密钥（必填，或设环境变量 PELICAN_BENCH_API_KEY）")
    p.add_argument("--model", required=True, help="被测模型名（必填）")
    p.add_argument("--rounds", type=int, default=5,
                   help="逐次请求次数（默认 5；总请求约 2N+2 次，均为真实计费请求）")
    p.add_argument("--timeout", type=float, default=30.0, help="单请求超时秒数（默认 30）")
    p.add_argument("--cache-test", dest="cache_test", action="store_true", default=True,
                   help="做 A/B 缓存探测（默认开）")
    p.add_argument("--no-cache-test", dest="cache_test", action="store_false", help="禁用缓存探测")
    p.add_argument("--json", metavar="FILE", default=None, help="同时输出机器可读 JSON 报告到文件")
    p.add_argument("--no-stream", action="store_true", help="禁用流式测试（只跑非流式与缓存探测）")
    p.add_argument("--version", action="version", version="%s %s" % (TOOL_NAME, TOOL_VERSION))
    args = p.parse_args(argv)
    if not args.api_key:
        p.error("--api-key 必填（或设置环境变量 PELICAN_BENCH_API_KEY）")
    if args.rounds < 1:
        p.error("--rounds 必须 >= 1")
    if args.timeout <= 0:
        p.error("--timeout 必须 > 0")
    if not args.base_url.startswith(("http://", "https://")):
        p.error("--base-url 必须以 http:// 或 https:// 开头")
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    meta = {"base_url": args.base_url, "model": args.model, "rounds": args.rounds,
            "timeout": int(args.timeout), "no_stream": args.no_stream,
            "cache_test": args.cache_test,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %z").strip()}

    def warn(msg: str) -> None:
        print("[pelican-bench] " + msg, file=sys.stderr)

    # 连通性预检：一次最小请求，快速失败（连接/HTTP 错误 → 退出码 2）
    try:
        status, body = http_post_json(args.base_url, args.api_key,
                                      {"model": args.model, "max_tokens": 4,
                                       "messages": [{"role": "user", "content": "ping"}]},
                                      args.timeout)
    except BenchConnectionError as e:
        warn("预检失败（连接错误）: %s" % e)
        return EXIT_ERROR
    if not (200 <= status < 300):
        warn("预检失败（HTTP %s）: %s" % (status, extract_error_snippet(body)))
        return EXIT_ERROR

    # 测试 1：流式 TTFB
    stream_result: Optional[Dict[str, Any]] = None
    if not args.no_stream:
        warn("开始流式 TTFB 测试（%d 轮，真实计费请求）..." % args.rounds)
        try:
            stream_result = test_stream_ttfb(args.base_url, args.api_key, args.model,
                                             args.rounds, args.timeout)
        except BenchConnectionError as e:
            warn("流式测试连接错误: %s" % e)

    # 测试 2：非流式完整性
    warn("开始非流式完整性测试（%d 轮，真实计费请求）..." % args.rounds)
    try:
        plain_result = test_plain_completeness(args.base_url, args.api_key,
                                               args.model, args.rounds, args.timeout)
    except BenchConnectionError as e:
        warn("非流式测试连接错误: %s" % e)
        plain_result = {"name": "plain_completeness", "rounds": [], "success_count": 0,
                        "failure_count": args.rounds, "pass_rate": 0.0,
                        "pass": False, "partial": True}

    # 测试 3：缓存 A/B 探测
    cache_result: Optional[Dict[str, Any]] = None
    if args.cache_test:
        warn("开始缓存 A/B 探测（2 次相同请求，真实计费请求）...")
        try:
            cache_result = run_cache_probe(args.base_url, args.api_key, args.model, args.timeout)
        except BenchConnectionError as e:
            warn("缓存探测连接错误: %s" % e)

    # 测试 4：usage 字段清单
    usage_result = test_usage_inventory(plain_result, cache_result)

    # 汇总与退出码
    sections = [r for r in (stream_result, plain_result, cache_result, usage_result) if r is not None]
    n_pass = sum(1 for r in sections if r.get("pass"))
    has_fail = any(not r.get("pass") for r in sections)
    has_partial = any(r.get("partial") for r in sections)
    if has_fail or has_partial:
        overall = {"verdict": "PARTIAL",
                   "text": "部分测试项未全部通过（%d/%d 项通过），详见各项明细" % (n_pass, len(sections))}
        exit_code = EXIT_PARTIAL
    else:
        overall = {"verdict": "PASS", "text": "全部测试项通过"}
        exit_code = EXIT_OK

    print(build_markdown_report(meta, stream_result, plain_result,
                                cache_result, usage_result, overall))

    if args.json:
        machine = {"tool": TOOL_NAME, "version": TOOL_VERSION, "meta": meta,
                   "overall": dict(overall, exit_code=exit_code),
                   "tests": {"stream_ttfb": stream_result, "plain_completeness": plain_result,
                             "cache_probe": cache_result, "usage_inventory": usage_result},
                   "disclaimer": DISCLAIMER}
        try:
            with open(args.json, "w", encoding="utf-8") as f:
                json.dump(json_safe(machine), f, ensure_ascii=False, indent=2)
                f.write("\n")
            warn("JSON 报告已写入: %s" % args.json)
        except OSError as e:
            warn("写 JSON 报告失败: %r" % (e,))
            exit_code = EXIT_PARTIAL
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
