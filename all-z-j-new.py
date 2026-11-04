# -*- coding: utf-8 -*-
"""多模式 IPTV 频道批量探测与测速（异步高性能版）

相对 all-z-j-new.py 的关键改动（输出格式、命令行参数完全兼容）：

【采集】
1. 探测与数据拉取合并为同一次请求：旧版先 GET "/" 判存活、再 GET 数据接口，
   请求数翻倍；本版直接请求数据接口，2xx 即视为存活并复用响应体。
2. requests + 100 线程 -> aiohttp 单会话 + 信号量高并发，消除每请求重建
   TCP 的开销，并去掉 3 次串行重试（1s 内不通的主机重试也不会通）。
3. 三种模式并发采集，总耗时取"最长"而不是"相加"。

【校验与测速】
4. 分片多路尝试：旧版只取播放列表第一个分片，直播场景里首个分片常已过窗
   而 404，会把好源误杀。本版按顺序最多试 3 个分片，命中即止。
5. 放宽状态码到 2xx（含 206 分段响应），并在采样超时时保留已收到的字节，
   把"慢但活着"的高码率源保住。
6. 两级有效性：真正读到分片字节 = 已校验（有实测速度）；只有播放列表可用
   = 备用源（速度记 0.001），仅在已校验源不足时才用于补位。
7. 采样测速：只读首个分片的前 N KB（时间 + 字节双上限），旧版是 5s 超时
   全量下载整个分片，慢源白吃 5 秒。计时从 os.times()[0]（进程 CPU 时间，
   多线程互相污染）改为 time.monotonic() 墙钟。

【选源】
8. 清晰度优先：从原始频道名与地址中识别 4K/1080/720/标清，排序时清晰度
   优先、速度次之，避免低码率源把 1080 源挤掉。
9. 每个频道最多保留 --max-sources 个（默认 3），并可按 --max-candidates
   限制每频道参与测速的候选数（默认 12，0 表示全测），大幅压缩测速耗时。
10. 进度输出节流（TTY 0.5s / 日志 3s 一次），并输出每频道源数分布，便于
    排查"某个节目只有 1 个源"这类问题。

用法（与原版一致）：
    python all-z-j-new-fast.py --jsmpeg jsmpeg.csv --txiptv txiptv.csv \\
        --zhgxtv zhgxtv.csv --output output/itvlist
"""

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlsplit

import aiohttp

# ---------------------------------------------------------------- 可调参数 --
PROBE_CONCURRENCY = 600          # 探测并发（每个主机展开 255 个任务）
SPEED_CONCURRENCY = 200          # 测速并发
PROBE_TIMEOUT = 1.5              # 探测单请求总超时（秒）
PLAYLIST_TIMEOUT = 3.0           # 拉取 m3u8 超时（秒）
SAMPLE_BYTES = 384 * 1024        # 每个源最多采样字节
SAMPLE_SECONDS = 2.0             # 采样时间上限（秒）
MAX_BODY = 4 * 1024 * 1024       # 数据接口响应体上限，防异常大响应
PLAYLIST_MAX_BODY = 512 * 1024   # m3u8 响应体上限
SEGMENT_TRIES = 3                # 每个源最多尝试几个分片
MAX_SOURCES_PER_CHANNEL = 3      # 每个频道保留的源数量（txt / m3u / speed.txt 一致）
MAX_CANDIDATES_PER_CHANNEL = 12  # 每个频道最多测几个候选源，0 = 全测
SPEED_FILE = "speed.txt"         # 与原版一致：写在当前工作目录
USER_AGENT = "okhttp/3.12.1"     # 多数 IPTV 服务端按播放器 UA 放行
GENRES = ("央视频道", "卫视频道", "其他频道")

# 三种模式的数据接口
EP_JSMSEG = "/streamer/list"
EP_ZHGXTV = "/zhgxtv/Public/json/live_interface.txt"
EP_TXIPTV = "/iptv/live/1000.json?key=txiptv"


# ------------------------------------------------------------ 频道名归一化 --
_NORMALIZE_REPS = (
    # 清晰度标记要一并剥离，否则 "CCTV5体育1080" 会另立门户，
    # 高清源无法归并到同一个频道名下参与排序
    ("2160P", ""), ("2160p", ""), ("2160", ""), ("4K", ""), ("4k", ""),
    ("1080P", ""), ("1080p", ""), ("1080", ""), ("FHD", ""), ("fhd", ""),
    ("720P", ""), ("720p", ""), ("720", ""),
    ("高清", ""), ("超清", ""), ("蓝光", ""), ("超高", ""), ("HD", ""), ("标清", ""),
    ("频道", ""), ("-", ""), (" ", ""), ("PLUS", "+"), ("＋", "+"),
    ("(", ""), (")", ""),
)

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

# 清晰度识别：命中任意关键词即记为该档位，取最高档
_QUALITY_RULES = (
    (2160, ("2160", "4k", "uhd")),
    (1080, ("1080", "fhd", "fullhd", "full_hd", "full-hd", "高清", "超清", "蓝光", "hd")),
    (720, ("720",)),
    (576, ("标清", "-sd", "_sd")),
)
QUALITY_LABEL = {2160: "4K", 1080: "1080P", 720: "720P", 576: "标清", 0: "未知"}

_IPV4_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_CCTV_NO_RE = re.compile(r"CCTV(\d+)台")


def channel_name_normalize(name: str) -> str:
    """与旧版逐条等价：去噪词 + CCTV 番号映射。"""
    for old, new in _NORMALIZE_REPS:
        name = name.replace(old, new)
    name = _CCTV_NO_RE.sub(r"CCTV\1", name)
    return _NAME_MAP.get(name, name)


def quality_score(*texts: Optional[str]) -> int:
    """从原始频道名 / 地址 / key 里识别清晰度档位，无法识别返回 0。"""
    blob = " ".join(t for t in texts if t).lower()
    if not blob:
        return 0
    for score, tokens in _QUALITY_RULES:
        for token in tokens:
            if token in blob:
                return score
    return 0


# ------------------------------------------------------------------ 目标生成 --
def _split_base(raw: str) -> Optional[Tuple[str, str, str]]:
    """把 CSV 里的一行拆成 (scheme, host, ':port')；非法返回 None。"""
    raw = (raw or "").strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "http://" + raw
    try:
        parts = urlsplit(raw)
    except ValueError:
        return None
    netloc = parts.netloc
    if not netloc:
        return None
    host, _, port = netloc.partition(":")
    if not host:
        return None
    return parts.scheme, host, (":" + port if port else "")


def build_targets(raw_hosts: Iterable[str], endpoint: str) -> List[str]:
    """同 C 段展开 1~255 到指定数据接口；非 IPv4 主机只探该主机本身。"""
    targets: List[str] = []
    seen = set()
    for raw in raw_hosts:
        base = _split_base(raw)
        if not base:
            continue
        scheme, host, port = base
        if _IPV4_RE.match(host):
            prefix = ".".join(host.split(".")[:3])
            for i in range(1, 256):
                url = "{0}://{1}.{2}{3}{4}".format(scheme, prefix, i, port, endpoint)
                if url not in seen:
                    seen.add(url)
                    targets.append(url)
        else:
            url = "{0}://{1}{2}{3}".format(scheme, host, port, endpoint)
            if url not in seen:
                seen.add(url)
                targets.append(url)
    return targets


def origin_of(url: str) -> str:
    """取 http://host:port，用于拼接频道地址。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    return "{0}://{1}".format(parts.scheme or "http", parts.netloc)


def _first_field(value: str) -> str:
    """txiptv 部分源把多个地址用逗号拼在一起，只取第一个。"""
    return (value or "").split(",")[0].strip()


def rebase_url(raw: str, origin: str) -> Optional[str]:
    """把频道地址改挂到实际探测到的主机上（zhgxtv 旧逻辑的等价实现）。"""
    raw = (raw or "").strip()
    if not raw.startswith(("http://", "https://")):
        return None
    scheme, _, after = raw.partition("://")
    slash = after.find("/")
    path = after[slash + 1:] if slash >= 0 else ""
    if path:
        return "{0}://{1}/{2}".format(scheme, origin.split("://")[-1], path)
    return "{0}://{1}".format(scheme, origin.split("://")[-1])


# -------------------------------------------------------------------- 网络层 --
def _timeout(total: float, sock_read: Optional[float] = None) -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(
        total=total,
        sock_connect=min(total, 3.0),
        sock_read=sock_read if sock_read is not None else total,
    )


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    timeout: float,
    limit: int = MAX_BODY,
) -> Optional[bytes]:
    """一次请求取回响应体；非 2xx 或任何异常返回 None。"""
    async with sem:
        try:
            async with session.get(
                url, timeout=_timeout(timeout), allow_redirects=True
            ) as resp:
                if resp.status >= 300:
                    resp.close()
                    return None
                return await resp.content.read(limit)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError,
                ValueError, UnicodeError):
            return None


# ------------------------------------------------------------- 三种采集模式 --
def _add_channel(bucket, raw_name, url, extra=None):
    name = (raw_name or "").strip()
    if not name or not url:
        return
    bucket.append((channel_name_normalize(name), url,
                   quality_score(name, url, extra)))


async def _resolve_all(session, targets, sem, timeout):
    if not targets:
        return []
    bodies = await asyncio.gather(
        *(fetch(session, url, sem, timeout) for url in targets)
    )
    return list(zip(targets, bodies))


async def collect_jsmpeg(
    session: aiohttp.ClientSession, csv_file: str,
    sem: asyncio.Semaphore, timeout: float,
) -> List[Tuple[str, str, int]]:
    hosts = read_csv_column(csv_file, "host")
    targets = build_targets(hosts, EP_JSMSEG)
    if not targets:
        return []
    print("[jsmpeg] 待探测 {0} 个接口".format(len(targets)))

    channels: List[Tuple[str, str, int]] = []
    alive = 0
    for target, body in await _resolve_all(session, targets, sem, timeout):
        if not body:
            continue
        try:
            data = json.loads(body.decode("utf-8", errors="ignore"))
        except ValueError:
            continue
        if not isinstance(data, list):
            continue
        alive += 1
        origin = origin_of(target)
        for item in data:
            if not isinstance(item, dict):
                continue
            key = (item.get("key") or "").strip()
            if key:
                _add_channel(channels, item.get("name"),
                             "{0}/hls/{1}/index.m3u8".format(origin, key), key)
    print("[jsmpeg] 存活 {0} 个，采集频道 {1} 条".format(alive, len(channels)))
    return channels


async def collect_zhgxtv(
    session: aiohttp.ClientSession, csv_file: str,
    sem: asyncio.Semaphore, timeout: float,
) -> List[Tuple[str, str, int]]:
    hosts = read_csv_column(csv_file, "host")
    targets = build_targets(hosts, EP_ZHGXTV)
    if not targets:
        return []
    print("[zhgxtv] 待探测 {0} 个接口".format(len(targets)))

    channels: List[Tuple[str, str, int]] = []
    alive = 0
    for target, body in await _resolve_all(session, targets, sem, timeout):
        if not body:
            continue
        text = body.decode("utf-8", errors="ignore")
        if "," not in text:                 # 不是频道表，多半是错误页
            continue
        alive += 1
        origin = origin_of(target)
        for line in text.splitlines():
            line = line.strip()
            if not line or "," not in line:
                continue
            name, _, raw_url = line.partition(",")
            _add_channel(channels, name, rebase_url(raw_url, origin))
    print("[zhgxtv] 存活 {0} 个，采集频道 {1} 条".format(alive, len(channels)))
    return channels


async def collect_txiptv(
    session: aiohttp.ClientSession, csv_file: str,
    sem: asyncio.Semaphore, timeout: float,
) -> List[Tuple[str, str, int]]:
    hosts = read_csv_column(csv_file, "link")
    targets = build_targets(hosts, EP_TXIPTV)
    if not targets:
        return []
    print("[txiptv] 待探测 {0} 个接口".format(len(targets)))

    channels: List[Tuple[str, str, int]] = []
    alive = 0
    for target, body in await _resolve_all(session, targets, sem, timeout):
        if not body:
            continue
        try:
            data = json.loads(body.decode("utf-8", errors="ignore"))
        except ValueError:
            continue
        items = data.get("data") if isinstance(data, dict) else None
        if not isinstance(items, list):
            continue
        alive += 1
        origin = origin_of(target)
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_url = _first_field(item.get("url"))
            if not raw_url:
                continue
            if raw_url.startswith(("http://", "https://")):
                url = raw_url
            else:
                url = origin + (raw_url if raw_url.startswith("/") else "/" + raw_url)
            _add_channel(channels, item.get("name"), url)
    print("[txiptv] 存活 {0} 个，采集频道 {1} 条".format(alive, len(channels)))
    return channels


# ------------------------------------------------------------------ CSV 读取 --
def read_csv_column(csv_file: str, column: str) -> List[str]:
    if not csv_file:
        return []
    if not os.path.exists(csv_file):
        print("[警告] 找不到 {0}，跳过该模式".format(csv_file), file=sys.stderr)
        return []
    values: List[str] = []
    seen = set()
    with open(csv_file, "r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        if column not in (reader.fieldnames or []):
            print("[警告] {0} 缺少 {1} 列，跳过该模式".format(csv_file, column),
                  file=sys.stderr)
            return []
        for row in reader:
            value = (row.get(column) or "").strip()
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return values


# --------------------------------------------------------------------- 测速 --
class Progress(object):
    """节流进度条：TTY 单行刷新，非 TTY（CI 日志）按行输出。"""

    def __init__(self, total: int, label: str):
        self.total = max(total, 1)
        self.label = label
        self.done = 0
        self.ok = 0
        self.tty = sys.stdout.isatty()
        self.interval = 0.5 if self.tty else 3.0
        self._last = time.monotonic()

    def tick(self, success: bool) -> None:
        self.done += 1
        if success:
            self.ok += 1
        now = time.monotonic()
        if now - self._last < self.interval and self.done < self.total:
            return
        self._last = now
        line = "{0} {1}/{2} ({3:.1f}%) 可用 {4} 个".format(
            self.label, self.done, self.total,
            self.done * 100.0 / self.total, self.ok)
        if self.tty:
            sys.stdout.write("\r" + line + "   ")
        else:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()

    def close(self) -> None:
        if self.tty:
            sys.stdout.write("\n")
            sys.stdout.flush()


def _playlist_lines(body: bytes, base: str) -> Tuple[List[str], List[str]]:
    """拆出 (分片地址, 子播放列表地址)。"""
    segments: List[str] = []
    variants: List[str] = []
    for line in body.decode("utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        url = urljoin(base, line)
        if url.split("?")[0].endswith(".m3u8"):
            variants.append(url)
        else:
            segments.append(url)
    return segments, variants


async def candidate_segments(
    session: aiohttp.ClientSession, playlist_url: str,
    sem: asyncio.Semaphore, want: int = SEGMENT_TRIES,
) -> Optional[List[str]]:
    """返回待尝试的分片地址列表。

    None  -> 播放列表本身不可用（该源直接丢弃）
    []    -> 播放列表可用但没解析出分片（源保留为备用）
    """
    body = await fetch(session, playlist_url, sem, PLAYLIST_TIMEOUT,
                       limit=PLAYLIST_MAX_BODY)
    if not body:
        return None
    segments, variants = _playlist_lines(body, playlist_url)
    if not segments and variants:      # 主播放列表：降一层拿真实分片
        body = await fetch(session, variants[0], sem, PLAYLIST_TIMEOUT,
                           limit=PLAYLIST_MAX_BODY)
        if body:
            segments, _ = _playlist_lines(body, variants[0])
    return segments[:want]


async def sample_throughput(
    session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore,
    sample_bytes: int, sample_seconds: float,
) -> Optional[float]:
    """读取前 sample_bytes 或 sample_seconds，返回 MB/s；一个字节都没收到返回 None。"""
    async with sem:
        start = time.monotonic()
        total = 0
        try:
            async with session.get(
                url,
                timeout=_timeout(sample_seconds + 3.0, sock_read=sample_seconds + 1.0),
                allow_redirects=True,
            ) as resp:
                if resp.status >= 300:
                    resp.close()
                    return None
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    total += len(chunk)
                    if total >= sample_bytes:
                        break
                    if time.monotonic() - start >= sample_seconds:
                        break
        except asyncio.TimeoutError:
            pass                        # 超时但已收到数据，按已收字节计速
        except (aiohttp.ClientError, OSError, ValueError):
            pass
        if not total:
            return None
        return total / max(time.monotonic() - start, 1e-3) / 1048576.0


def select_candidates(
    channels: Sequence[Tuple[str, str, int]], per_channel: int
) -> List[Tuple[str, str, int]]:
    """去重，并按频道挑选参与测速的候选：清晰度高的先测。"""
    unique: List[Tuple[str, str, int]] = []
    seen = set()
    for name, url, quality in channels:
        key = (name, url)
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, url, quality))

    if per_channel <= 0:
        return unique

    grouped: Dict[str, List[Tuple[str, str, int]]] = defaultdict(list)
    for item in unique:
        grouped[item[0]].append(item)

    picked: List[Tuple[str, str, int]] = []
    for name in sorted(grouped):
        group = sorted(grouped[name], key=lambda x: -x[2])   # 清晰度优先
        picked.extend(group[:per_channel])
    return picked


async def run_speed_stage(
    channels: Sequence[Tuple[str, str, int]],
    probe_concurrency: int, speed_concurrency: int,
    sample_bytes: int, sample_seconds: float,
) -> List[Tuple[str, str, int, float, bool]]:
    """返回 (名称, 地址, 清晰度, 速度, 是否已校验)。"""
    probe_sem = asyncio.Semaphore(max(probe_concurrency, 1))
    speed_sem = asyncio.Semaphore(max(speed_concurrency, 1))
    progress = Progress(len(channels), "测速进度")
    results: List[Tuple[str, str, int, float, bool]] = []

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT},
        connector=aiohttp.TCPConnector(
            limit=0, limit_per_host=0, ttl_dns_cache=600,
            enable_cleanup_closed=True, force_close=False),
    ) as session:
        async def worker(name: str, url: str, quality: int):
            try:
                segments = await candidate_segments(session, url, probe_sem)
                if segments is None:            # 播放列表都打不开
                    progress.tick(False)
                    return None
                speed = None
                for segment in segments:
                    speed = await sample_throughput(
                        session, segment, speed_sem, sample_bytes, sample_seconds)
                    if speed is not None:
                        break
                if speed is not None:
                    progress.tick(True)
                    return name, url, quality, min(max(speed, 0.001), 100.0), True
                progress.tick(False)
                # 播放列表可用但分片没拿到字节：留作备用源，排最后
                return name, url, quality, 0.001, False
            except Exception:                   # 单源异常不影响整批
                progress.tick(False)
                return None

        tasks = [asyncio.ensure_future(worker(*item)) for item in channels]
        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    result = await fut
                except Exception:
                    result = None
                if result:
                    results.append(result)
        finally:
            progress.close()
            for task in tasks:
                if not task.done():
                    task.cancel()
    return results


# ------------------------------------------------------------------ 结果整理 --
def aggregate(
    results: Sequence[Tuple[str, str, int, float, bool]],
    limit: int = MAX_SOURCES_PER_CHANNEL,
) -> List[Tuple[str, str, str]]:
    """每个频道取最快的 limit 个源：清晰度优先、速度次之；已校验源优先于备用源。"""
    grouped: Dict[str, List[Tuple[str, str, int, float, bool]]] = defaultdict(list)
    for item in results:
        grouped[item[0]].append(item)

    kept: List[Tuple[str, str, float]] = []
    for name in sorted(grouped):
        group = grouped[name]
        verified = sorted([x for x in group if x[4]],
                          key=lambda x: (-x[2], -x[3]))
        fallback = sorted([x for x in group if not x[4]],
                          key=lambda x: -x[2])
        for url, speed in [(x[1], x[3]) for x in (verified + fallback)[:limit]]:
            kept.append((name, url, speed))

    seen = set()
    unique: List[Tuple[str, str, str]] = []
    for name, url, speed in kept:
        key = (name, url)
        if key in seen:
            continue
        seen.add(key)
        unique.append((name, url, "{0:.3f} MB/s".format(speed)))

    # stable sort：只按频道名重排，频道内保留"清晰度优先、速度从快到慢"的顺序
    unique.sort(key=_sort_key)
    return unique


def _sort_key(item: Tuple[str, str, str]):
    name = item[0]
    if name.startswith("CCTV"):
        match = re.search(r"\d+", name)
        return (0, int(match.group()) if match else float("inf"))
    return (1, name)


def in_genre(name: str, genre: str) -> bool:
    if genre == "央视频道":
        return "CCTV" in name
    if genre == "卫视频道":
        return "卫视" in name
    return "CCTV" not in name and "卫视" not in name and "测试" not in name


def iter_genre(channels: Sequence[Tuple[str, str, str]], genre: str, limit: int):
    counters: Dict[str, int] = {}
    for name, url, _ in channels:
        if not in_genre(name, genre):
            continue
        used = counters.get(name, 0)
        if used >= limit:
            continue
        counters[name] = used + 1
        yield name, url


def write_outputs(
    channels: Sequence[Tuple[str, str, str]], prefix: str, limit: int
) -> None:
    parent = os.path.dirname(os.path.abspath(prefix))
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(prefix + ".txt", "w", encoding="utf-8") as fp:
        for genre in GENRES:
            fp.write("{0},#genre#\n".format(genre))
            for name, url in iter_genre(channels, genre, limit):
                fp.write("{0},{1}\n".format(name, url))

    with open(prefix + ".m3u", "w", encoding="utf-8") as fp:
        fp.write("#EXTM3U\n")
        for genre in GENRES:
            for name, url in iter_genre(channels, genre, limit):
                fp.write('#EXTINF:-1 group-title="{0}",{1}\n'.format(genre, name))
                fp.write(url + "\n")

    with open(SPEED_FILE, "w", encoding="utf-8") as fp:
        for row in channels:
            fp.write(",".join(row) + "\n")


def report_distribution(
    final: Sequence[Tuple[str, str, str]], limit: int
) -> None:
    """打印每频道源数分布，一眼看出哪个节目没凑够源。"""
    counts = Counter(name for name, _, _ in final)
    if not counts:
        return
    dist = Counter(counts.values())
    summary = "，".join("{0} 个源×{1}".format(k, dist[k]) for k in sorted(dist))
    print("[统计] 有效频道 {0} 个：{1}".format(len(counts), summary))
    short = [n for n, c in counts.items() if c < limit]
    if short:
        head = "、".join(short[:12])
        more = "" if len(short) <= 12 else " 等 {0} 个".format(len(short))
        print("  源数不足 {0} 个的频道：{1}{2}".format(limit, head, more))


# --------------------------------------------------------------------- 主流程 --
def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="多模式IPTV频道批量探测与测速")
    parser.add_argument("--jsmpeg", help="jsmpeg-streamer模式csv文件")
    parser.add_argument("--txiptv", help="txiptv模式csv文件")
    parser.add_argument("--zhgxtv", help="zhgxtv模式csv文件")
    parser.add_argument("--output", default="itvlist", help="输出文件前缀")
    parser.add_argument("--probe-concurrency", type=int, default=PROBE_CONCURRENCY,
                        help="探测并发数，默认 %d" % PROBE_CONCURRENCY)
    parser.add_argument("--speed-concurrency", type=int, default=SPEED_CONCURRENCY,
                        help="测速并发数，默认 %d" % SPEED_CONCURRENCY)
    parser.add_argument("--probe-timeout", type=float, default=PROBE_TIMEOUT,
                        help="探测超时秒数，默认 %.1f" % PROBE_TIMEOUT)
    parser.add_argument("--sample-kb", type=int, default=SAMPLE_BYTES // 1024,
                        help="测速采样上限 KB，默认 %d" % (SAMPLE_BYTES // 1024))
    parser.add_argument("--sample-seconds", type=float, default=SAMPLE_SECONDS,
                        help="测速采样时间上限秒，默认 %.1f" % SAMPLE_SECONDS)
    parser.add_argument("--max-sources", type=int,
                        default=MAX_SOURCES_PER_CHANNEL,
                        help="每个频道保留的源数量，默认 %d"
                             % MAX_SOURCES_PER_CHANNEL)
    parser.add_argument("--max-candidates", type=int,
                        default=MAX_CANDIDATES_PER_CHANNEL,
                        help="每个频道最多测几个候选源（清晰度高的优先），"
                             "默认 %d，0 表示全部测" % MAX_CANDIDATES_PER_CHANNEL)
    return parser.parse_args(argv)


async def run(args: argparse.Namespace) -> int:
    started = time.monotonic()
    probe_concurrency = max(args.probe_concurrency, 1)
    timeout = max(args.probe_timeout, 0.2)
    sample_bytes = max(args.sample_kb, 16) * 1024
    sample_seconds = max(args.sample_seconds, 0.2)
    limit = max(args.max_sources, 1)

    channels: List[Tuple[str, str, int]] = []
    sem = asyncio.Semaphore(probe_concurrency)
    connector = aiohttp.TCPConnector(
        limit=0, limit_per_host=0, ttl_dns_cache=600,
        enable_cleanup_closed=True, force_close=False)

    t0 = time.monotonic()
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        # 三种模式互相独立，并发跑：扫段的总耗时取"最长"而不是"相加"
        jobs = []
        if args.jsmpeg:
            jobs.append(collect_jsmpeg(session, args.jsmpeg, sem, timeout))
        if args.zhgxtv:
            jobs.append(collect_zhgxtv(session, args.zhgxtv, sem, timeout))
        if args.txiptv:
            jobs.append(collect_txiptv(session, args.txiptv, sem, timeout))
        if jobs:
            for group in await asyncio.gather(*jobs, return_exceptions=True):
                if isinstance(group, Exception):
                    print("[警告] 某个模式采集失败：{0}".format(group),
                          file=sys.stderr)
                    continue
                channels.extend(group)
    print("[采集] 耗时 {0:.1f}s，共 {1} 条频道源".format(
        time.monotonic() - t0, len(channels)))

    if not channels:
        print("未采集到任何频道，请检查 csv 文件")
        return 1

    candidates = select_candidates(channels, args.max_candidates)
    print("[候选] {0} 个频道、{1} 个待测源（每频道上限 {2}）".format(
        len(set(c[0] for c in candidates)), len(candidates),
        args.max_candidates if args.max_candidates > 0 else "不限"))

    t1 = time.monotonic()
    results = await run_speed_stage(
        candidates, probe_concurrency, args.speed_concurrency,
        sample_bytes, sample_seconds)
    verified = sum(1 for r in results if r[4])
    print("[测速] 耗时 {0:.1f}s，可用 {1} 个（其中实测通过 {2} 个）".format(
        time.monotonic() - t1, len(results), verified))

    final = aggregate(results, limit)
    write_outputs(final, args.output, limit)
    report_distribution(final, limit)
    print("[完成] 输出 {0}.txt / {0}.m3u / {1}，共 {2} 条，总耗时 {3:.1f}s".format(
        args.output, SPEED_FILE, len(final), time.monotonic() - started))
    return 0


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\n已中断")
        return 130


if __name__ == "__main__":
    sys.exit(main())
