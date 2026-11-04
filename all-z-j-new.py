#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多模式 IPTV 频道批量探测 / 测速 / 列表生成（优化版）。

三种源模式与原版一致：
    --jsmpeg   jsmpeg-streamer（CSV 含 host 列）
    --txiptv   txiptv（CSV 含 link 列）
    --zhgxtv   zhgxtv（CSV 含 host 列）

输出：<prefix>.txt、<prefix>.m3u、speed.txt
频道归一化规则、分组规则、排序规则、每频道条数上限、进度打印文案均与原版一致。
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import csv
import re
import threading
import time
from collections import defaultdict
from urllib.parse import urlsplit

import aiohttp
import requests
from requests.adapters import HTTPAdapter

# --------------------------------------------------------------------------
# 可调参数（默认值与原始实现保持一致）
# --------------------------------------------------------------------------
PROBE_TIMEOUT = 1.0          # 探测列表/接口超时（秒）
STREAM_TIMEOUT = 5.0         # 拉取 TS 分片超时（秒）
SYNC_CONCURRENCY = 100       # 同步探测线程数
ASYNC_CONCURRENCY = 500      # 异步探测并发数
SPEED_THREADS = 50           # 测速线程数
MAX_PROBE_RETRIES = 3        # 探测重试次数（总尝试次数）
MAX_SOURCES_PER_CHANNEL = 8  # 每个频道在每个分组中最多保留的源数
MAX_SOURCES_PER_CHANNEL_SORTED = 15  # 按速度排序后每频道最多参与输出的源数

TXIPTV_SUFFIX = "/iptv/live/1000.json?key=txiptv"
ZHGXTV_SUFFIX = "/zhgxtv/Public/json/live_interface.txt"

GENRES = ("央视频道", "卫视频道", "其他频道")

# --------------------------------------------------------------------------
# 频道名归一化
# --------------------------------------------------------------------------
_STRIP_TOKENS = ("高清", "超高", "HD", "标清", "频道", "-", " ", "PLUS", "＋", "(", ")")
_CCTV_TAI_RE = re.compile(r"CCTV(\d+)台")

_NAME_MAP = {
    "CCTV1综合": "CCTV1", "CCTV2财经": "CCTV2", "CCTV3综艺": "CCTV3",
    "CCTV4国际": "CCTV4", "CCTV4中文国际": "CCTV4", "CCTV4欧洲": "CCTV4",
    "CCTV5体育": "CCTV5", "CCTV6电影": "CCTV6", "CCTV7军事": "CCTV7",
    "CCTV7军农": "CCTV7", "CCTV7农业": "CCTV7", "CCTV7国防军事": "CCTV7",
    "CCTV8电视剧": "CCTV8", "CCTV9记录": "CCTV9", "CCTV9纪录": "CCTV9",
    "CCTV10科教": "CCTV10", "CCTV11戏曲": "CCTV11", "CCTV12社会与法": "CCTV12",
    "CCTV13新闻": "CCTV13", "CCTV新闻": "CCTV13", "CCTV14少儿": "CCTV14",
    "CCTV15音乐": "CCTV15", "CCTV16奥林匹克": "CCTV16",
    "CCTV17农业农村": "CCTV17", "CCTV17农业": "CCTV17",
    "CCTV5+体育赛视": "CCTV5+", "CCTV5+体育赛事": "CCTV5+", "CCTV5+体育": "CCTV5+",
}


def channel_name_normalize(name: str) -> str:
    """去掉清晰度后缀等噪声，并把 CCTVx某频道 收敛成 CCTVx。"""
    for token in _STRIP_TOKENS:
        name = name.replace(token, "+" if token in ("PLUS", "＋") else "")
    name = _CCTV_TAI_RE.sub(r"CCTV\1", name)
    return _NAME_MAP.get(name, name)


# --------------------------------------------------------------------------
# 会话复用（复用 TCP 连接，避免每个请求都重新握手）
# --------------------------------------------------------------------------
_thread_local = threading.local()


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=4, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session


# --------------------------------------------------------------------------
# URL 拆解与候选生成
# --------------------------------------------------------------------------
def split_url_parts(url: str) -> tuple[str, str, str]:
    """拆成 (scheme+preamble, ip, port_and_rest)，与原始切片语义一致。"""
    scheme_end = url.find("//") + 2
    port_start = url.find(":", scheme_end)
    if port_start == -1:
        port_start = len(url)
    return url[:scheme_end], url[scheme_end:port_start], url[port_start:]


def build_c_segment_urls(prefix_url: str, ip: str, port: str, suffix: str = "") -> list[str]:
    """生成同一 C 段（a.b.c.1 ~ a.b.c.255）的全部候选地址。"""
    octets = ip.split(".")
    if len(octets) < 3:
        return []
    c_prefix = ".".join(octets[:3])
    return [f"{prefix_url}{c_prefix}.{i}{port}{suffix}" for i in range(1, 256)]


def parse_link(url: str) -> tuple[str, str, str]:
    """把 txiptv 的 link 拆成 (scheme://, host, :port)，忽略 path/query。"""
    if "://" not in url:
        url = "http://" + url
    parts = urlsplit(url)
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://", parts.hostname or "", port


def origin_of(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


# --------------------------------------------------------------------------
# 同步探测
# --------------------------------------------------------------------------
def is_url_accessible(url: str, retries: int = MAX_PROBE_RETRIES,
                      timeout: float = PROBE_TIMEOUT) -> str | None:
    """可用则返回原 url，否则 None；连接异常最多尝试 retries 次（同原逻辑）。"""
    for _ in range(max(1, retries)):
        try:
            response = _session().get(url, timeout=timeout)
        except requests.RequestException:
            continue
        return url if response.status_code == 200 else None
    return None


def check_urls_concurrent(urls, retries: int = MAX_PROBE_RETRIES,
                          workers: int = SYNC_CONCURRENCY) -> list[str]:
    candidates = list(dict.fromkeys(urls))
    if not candidates:
        return []

    valid_urls: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for result in pool.map(lambda u: is_url_accessible(u, retries), candidates):
            if result:
                valid_urls.append(result)
                print(result)
    return valid_urls


# --------------------------------------------------------------------------
# jsmpeg-streamer
# --------------------------------------------------------------------------
def get_channels_alltv(csv_file: str, retries: int = MAX_PROBE_RETRIES,
                       workers: int = SYNC_CONCURRENCY) -> list[tuple[str, str]]:
    hosts: set[str] = set()
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "host" not in (reader.fieldnames or []):
            raise ValueError("CSV文件缺少host列")
        for row in reader:
            host = (row.get("host") or "").strip()
            if host:
                hosts.add(host if host.startswith(("http://", "https://")) else f"http://{host}")

    candidates: list[str] = []
    for url in hosts:
        prefix_url, ip, port = split_url_parts(url)
        candidates.extend(build_c_segment_urls(prefix_url, ip, port))

    valid_urls = check_urls_concurrent(candidates, retries=retries, workers=workers)

    channels: list[tuple[str, str]] = []
    for url in valid_urls:
        host = url.rstrip("/")
        try:
            payload = _session().get(f"{host}/streamer/list", timeout=PROBE_TIMEOUT).json()
        except (requests.RequestException, ValueError, TypeError):
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            name = (item.get("name") or "").strip()
            key = (item.get("key") or "").strip()
            if name and key:
                channels.append((channel_name_normalize(name), f"{host}/hls/{key}/index.m3u8"))
    return channels


# --------------------------------------------------------------------------
# zhgxtv
# --------------------------------------------------------------------------
def get_channels_hgxtv(csv_file: str, retries: int = MAX_PROBE_RETRIES,
                       workers: int = SYNC_CONCURRENCY) -> list[tuple[str, str]]:
    hosts: set[str] = set()
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            host = (row.get("host") or "").strip()
            if host:
                hosts.add(host if host.startswith(("http://", "https://"))
                          else f"http://{host}{':80' if ':' not in host else ''}")

    candidates: list[str] = []
    for url in hosts:
        prefix_url, ip, port = split_url_parts(url)
        candidates.extend(build_c_segment_urls(prefix_url, ip, port, ZHGXTV_SUFFIX))

    valid_urls = check_urls_concurrent(candidates, retries=retries, workers=workers)

    channels: list[tuple[str, str]] = []
    for url in valid_urls:
        try:
            text = _session().get(url, timeout=PROBE_TIMEOUT).content.decode("utf-8")
            url_netloc = url.split("/", 3)[2]
            for line in text.split("\n"):
                line = line.strip()
                if not line:
                    continue
                name, channel_url = line.split(",")
                parts = channel_url.split("/", 3)
                urld = (f"{parts[0]}//{url_netloc}/{parts[3]}"
                        if len(parts) >= 4 else f"{parts[0]}//{url_netloc}")
                channels.append((channel_name_normalize(name), urld))
        except Exception:
            continue
    return channels


# --------------------------------------------------------------------------
# txiptv（异步）
# --------------------------------------------------------------------------
async def _probe_ok(session: aiohttp.ClientSession, url: str,
                    semaphore: asyncio.Semaphore) -> str | None:
    async with semaphore:
        try:
            async with session.get(url) as response:
                return url if response.status == 200 else None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None


async def _fetch_txiptv_channels(session: aiohttp.ClientSession, url: str,
                                 semaphore: asyncio.Semaphore) -> list[tuple[str, str]]:
    async with semaphore:
        try:
            async with session.get(url) as response:
                payload = await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            return []

    if not isinstance(payload, dict):
        return []

    origin = origin_of(url)
    channels: list[tuple[str, str]] = []
    for item in payload.get("data", []):
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        path = item.get("url")
        if not name or not path:
            continue
        if "," in path:          # 原实现会把这类条目变成无效地址，这里直接丢弃
            continue
        full_url = path if "http" in path else f"{origin}{path}"
        channels.append((channel_name_normalize(name), full_url))
    return channels


async def get_channels_txiptv(csv_file: str,
                              concurrency: int = ASYNC_CONCURRENCY) -> list[tuple[str, str]]:
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        links = {(row.get("link") or "").strip() for row in csv.DictReader(f)}
    links.discard("")

    probes: list[str] = []
    for link in links:
        prefix_url, ip, port = parse_link(link)
        probes.extend(build_c_segment_urls(prefix_url, ip, port, TXIPTV_SUFFIX))
    probes = list(dict.fromkeys(probes))
    if not probes:
        return []

    semaphore = asyncio.Semaphore(max(1, concurrency))
    timeout = aiohttp.ClientTimeout(total=PROBE_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        valid_urls = [u for u in await asyncio.gather(
            *(_probe_ok(session, url, semaphore) for url in probes)) if u]
        for url in valid_urls:
            print(url)
        results = await asyncio.gather(
            *(_fetch_txiptv_channels(session, url, semaphore) for url in valid_urls))

    return [channel for sublist in results for channel in sublist]


# --------------------------------------------------------------------------
# 测速 + 输出
# --------------------------------------------------------------------------
def _measure_channel_speed(channel_url: str) -> str:
    """返回 'x.xxx MB/s'；失败抛异常。"""
    base = channel_url.rstrip(channel_url.split("/")[-1])
    text = _session().get(channel_url, timeout=PROBE_TIMEOUT).text
    segments = [line for line in text.strip().split("\n") if not line.startswith("#")]
    if not segments:
        raise ValueError("No valid TS files found.")
    ts_url = base + segments[0].split("/")[-1]

    start = time.perf_counter()
    content = _session().get(ts_url, timeout=STREAM_TIMEOUT).content
    elapsed = time.perf_counter() - start
    if not content:
        raise ValueError("empty segment")

    download_speed = len(content) / elapsed / 1024          # KB/s
    normalized = min(max(download_speed / 1024, 0.001), 100)  # MB/s，夹在 0.001~100
    return f"{normalized:.3f} MB/s"


def _genre_of(name: str) -> str | None:
    if "CCTV" in name:
        return "央视频道"
    if "卫视" in name:
        return "卫视频道"
    if "测试" in name:
        return None
    return "其他频道"


def _sort_key(item: tuple[str, str, str]):
    name = item[0]
    if name.startswith("CCTV"):
        num = re.search(r"\d+", name)
        return (0, int(num.group())) if num else (0, float("inf"))
    return (1, name)


def test_speed_and_output(channels: list[tuple[str, str]],
                          output_prefix: str = "itvlist",
                          threads: int = SPEED_THREADS) -> None:
    total = len(channels)
    if total == 0:
        return

    lock = threading.Lock()
    speed_results: list[tuple[str, str, str]] = []
    error_channels: list[tuple[str, str]] = []
    done = 0

    def run_one(channel: tuple[str, str]) -> None:
        nonlocal done
        name, url = channel
        try:
            speed = _measure_channel_speed(url)
        except Exception:
            with lock:
                error_channels.append((name, url))
        else:
            with lock:
                speed_results.append((name, url, speed))
        finally:
            with lock:
                done += 1
                print(f"可用频道：{len(speed_results)} 个 , 不可用频道：{len(error_channels)} 个 , "
                      f"总频道：{total} 个 ,总进度：{done / total * 100:.2f} %。")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        for _ in pool.map(run_one, channels):
            pass

    # 每个频道按速度取前 N 个源
    channel_sources: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for name, url, speed in speed_results:
        channel_sources[name].append((url, speed))

    optimized: list[tuple[str, str, str]] = []
    for name, sources in channel_sources.items():
        best = sorted(sources, key=lambda x: float(x[1].split()[0]), reverse=True)
        optimized.extend((name, url, speed) for url, speed in best[:MAX_SOURCES_PER_CHANNEL_SORTED])

    # 去重
    unique_channels: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for name, url, speed in optimized:
        key = (name, url)
        if key not in seen:
            seen.add(key)
            unique_channels.append((name, url, speed))

    unique_channels.sort(key=_sort_key)

    # 分组并限制每频道条数
    grouped: dict[str, list[tuple[str, str]]] = {genre: [] for genre in GENRES}
    counters: dict[str, dict[str, int]] = {genre: defaultdict(int) for genre in GENRES}
    for name, url, _speed in unique_channels:
        genre = _genre_of(name)
        if genre is None:
            continue
        if counters[genre][name] < MAX_SOURCES_PER_CHANNEL:
            counters[genre][name] += 1
            grouped[genre].append((name, url))

    with open(f"{output_prefix}.txt", "w", encoding="utf-8") as txt_file:
        for genre in GENRES:
            txt_file.write(f"{genre},#genre#\n")
            for name, url in grouped[genre]:
                txt_file.write(f"{name},{url}\n")

    with open(f"{output_prefix}.m3u", "w", encoding="utf-8") as m3u_file:
        m3u_file.write("#EXTM3U\n")
        for genre in GENRES:
            for name, url in grouped[genre]:
                m3u_file.write(f'#EXTINF:-1 group-title="{genre}",{name}\n')
                m3u_file.write(f"{url}\n")

    with open("speed.txt", "w", encoding="utf-8") as speed_file:
        for row in unique_channels:
            speed_file.write(f"{','.join(row)}\n")


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="多模式IPTV频道批量探测与测速")
    parser.add_argument("--jsmpeg", help="jsmpeg-streamer模式csv文件")
    parser.add_argument("--txiptv", help="txiptv模式csv文件")
    parser.add_argument("--zhgxtv", help="zhgxtv模式csv文件")
    parser.add_argument("--output", default="itvlist", help="输出文件前缀")
    parser.add_argument("--workers", type=int, default=SYNC_CONCURRENCY,
                        help=f"同步探测线程数（默认 {SYNC_CONCURRENCY}）")
    parser.add_argument("--async-limit", type=int, default=ASYNC_CONCURRENCY,
                        help=f"txiptv 异步并发数（默认 {ASYNC_CONCURRENCY}）")
    parser.add_argument("--retries", type=int, default=MAX_PROBE_RETRIES,
                        help=f"探测重试次数（默认 {MAX_PROBE_RETRIES}）")
    parser.add_argument("--speed-threads", type=int, default=SPEED_THREADS,
                        help=f"测速线程数（默认 {SPEED_THREADS}）")
    args = parser.parse_args()

    channels: list[tuple[str, str]] = []
    if args.jsmpeg:
        channels.extend(get_channels_alltv(args.jsmpeg, args.retries, args.workers))
    if args.zhgxtv:
        channels.extend(get_channels_hgxtv(args.zhgxtv, args.retries, args.workers))
    if args.txiptv:
        channels.extend(asyncio.run(get_channels_txiptv(args.txiptv, args.async_limit)))

    if not channels:
        print("请至少指定一个csv文件")
        return

    test_speed_and_output(channels, args.output, args.speed_threads)


if __name__ == "__main__":
    main()
