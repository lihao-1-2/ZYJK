# -*- coding: utf-8 -*-
# 优化版：全异步高并发探测 + 测速
# 与原版 all-z-j-new.py 命令行参数、输出格式完全兼容
# 主要改动：
#   1. jsmpeg / zhgxtv 的 C 段扫描由 requests+100线程 改为 aiohttp 异步 1000 并发
#   2. 取消死 IP 的 3 次重试（重试对已失效 IP 无意义，却让耗时 x3）
#   3. 测速阶段由 50 线程改为 300 并发异步，且用真实墙钟时间测速（原版误用 CPU 时间）
#   4. 会话级连接池不设上限，由信号量精确控制并发
import re
import csv
import os
import time
import asyncio
import argparse
import aiohttp
from collections import defaultdict

# ======== 性能参数（按需调整） ========
PROBE_CONCURRENCY = 1000   # C 段扫描并发数（原版等效 100）
PROBE_TIMEOUT = 1          # 扫描超时（秒），与原版一致
FETCH_TIMEOUT = 3          # 获取频道列表超时（秒）
SPEED_CONCURRENCY = 300    # 测速并发数（原版 50）
SPEED_TIMEOUT = 4          # 测速单个请求超时（秒），原版 5 秒

# 归一化频道名称
def channel_name_normalize(name):
    for rep in ["高清", "超高", "HD", "标清", "频道", "-", " ", "PLUS", "＋", "(", ")"]:
        name = name.replace(rep, "" if rep not in ["PLUS", "＋"] else "+")
    name = re.sub(r"CCTV(\d+)台", r"CCTV\1", name)
    name_map = {
        "CCTV1综合": "CCTV1", "CCTV2财经": "CCTV2", "CCTV3综艺": "CCTV3",
        "CCTV4国际": "CCTV4", "CCTV4中文国际": "CCTV4", "CCTV4欧洲": "CCTV4",
        "CCTV5体育": "CCTV5", "CCTV6电影": "CCTV6", "CCTV7军事": "CCTV7",
        "CCTV7军农": "CCTV7", "CCTV7农业": "CCTV7", "CCTV7国防军事": "CCTV7",
        "CCTV8电视剧": "CCTV8", "CCTV9记录": "CCTV9", "CCTV9纪录": "CCTV9",
        "CCTV10科教": "CCTV10", "CCTV11戏曲": "CCTV11", "CCTV12社会与法": "CCTV12",
        "CCTV13新闻": "CCTV13", "CCTV新闻": "CCTV13", "CCTV14少儿": "CCTV14",
        "CCTV15音乐": "CCTV15", "CCTV16奥林匹克": "CCTV16",
        "CCTV17农业农村": "CCTV17", "CCTV17农业": "CCTV17",
        "CCTV5+体育赛视": "CCTV5+", "CCTV5+体育赛事": "CCTV5+", "CCTV5+体育": "CCTV5+"
    }
    name = name_map.get(name, name)
    return name

# 生成同一C段的所有IP的URL
def generate_ip_range_urls(base_url, ip_address, port, suffix=None):
    ip_parts = ip_address.split('.')
    if len(ip_parts) < 3:
        return []
    c_prefix = '.'.join(ip_parts[:3])
    return [f"{base_url}{c_prefix}.{i}{port}{suffix if suffix else ''}" for i in range(1, 256)]

def load_hosts(csv_file, column='host'):
    hosts = set()
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if column not in reader.fieldnames:
            raise ValueError(f'CSV文件缺少{column}列')
        for row in reader:
            host = row[column].strip()
            if host:
                hosts.add(host if host.startswith(('http://', 'https://')) else f"http://{host}")
    return hosts

def new_session():
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    return aiohttp.ClientSession(connector=connector)

async def check_urls_async(session, urls, timeout=PROBE_TIMEOUT, sem_size=PROBE_CONCURRENCY, print_valid=False):
    """异步并发探测 URL 可用性（单次尝试，不重试死 IP）"""
    sem = asyncio.Semaphore(sem_size)
    req_timeout = aiohttp.ClientTimeout(total=timeout)

    async def check(url):
        async with sem:
            try:
                async with session.get(url, timeout=req_timeout) as resp:
                    if resp.status == 200:
                        resp.release()
                        return url
            except Exception:
                pass
            return None

    results = await asyncio.gather(*(check(u) for u in urls))
    valid_urls = [u for u in results if u]
    if print_valid:
        for u in valid_urls:
            print(u)
    return valid_urls

# jsmpeg模式获取频道（异步高并发）
async def get_channels_alltv(csv_file):
    urls = load_hosts(csv_file)

    ip_range_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        port = url[ip_end:]
        ip_range_urls.extend(generate_ip_range_urls(base_url, ip_address, port))

    channels = []
    async with new_session() as session:
        valid_urls = await check_urls_async(session, set(ip_range_urls))
        print(f"jsmpeg: C段扫描完成，存活主机 {len(valid_urls)} 个")

        sem = asyncio.Semaphore(PROBE_CONCURRENCY)

        async def fetch(url):
            host = url.rstrip('/')
            async with sem:
                try:
                    async with session.get(f"{host}/streamer/list",
                                           timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as resp:
                        json_data = await resp.json(content_type=None)
                except Exception:
                    return []
                out = []
                for item in json_data:
                    name = str(item.get('name', '')).strip()
                    key = str(item.get('key', '')).strip()
                    if name and key:
                        out.append((channel_name_normalize(name), f"{host}/hls/{key}/index.m3u8"))
                return out

        results = await asyncio.gather(*(fetch(u) for u in valid_urls))
    return [c for sub in results for c in sub]

# txiptv模式获取频道（异步）
async def get_channels_newnew(csv_file):
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        urls = list(set(row.get('link', '').strip() for row in reader if row.get('link')))

    async def modify_urls(url):
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        ip_parts = ip_address.split('.')
        if len(ip_parts) < 3:
            return []
        c_prefix = '.'.join(ip_parts[:3])
        port = url[ip_end:]
        ip_end = "/iptv/live/1000.json?key=txiptv"
        return [f"{base_url}{c_prefix}.{i}{port}{ip_end}" for i in range(1, 256)]

    x_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        ip_dot = url.find(".") + 1
        ip_address = url[ip_start:url.find(".", ip_dot, url.find(".", ip_dot + 1)) + 1]
        port = url[ip_end:]
        x_urls.append(f"{url[:ip_start]}{ip_address}1{port}")

    unique_urls = set(x_urls)
    sem = asyncio.Semaphore(PROBE_CONCURRENCY)
    channels = []
    async with new_session() as session:
        valid_urls = await check_urls_async(session, unique_urls, print_valid=True)
        print(f"txiptv: C段扫描完成，存活主机 {len(valid_urls)} 个")

        async def fetch_json(url):
            async with sem:
                try:
                    ip_start = url.find("//") + 2
                    ip_index = url.find("/", url.find(".") + 1)
                    base_url = url[:ip_start]
                    ip_address = url[ip_start:ip_index]
                    url_x = f"{base_url}{ip_address}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as resp:
                        json_data = await resp.json(content_type=None)
                    out = []
                    for item in json_data.get('data', []):
                        if isinstance(item, dict):
                            name = item.get('name')
                            urlx = item.get('url')
                            if urlx and ',' in urlx:
                                urlx = "aaaaaaaa"
                            urld = urlx if (urlx and 'http' in urlx) else f"{url_x}{urlx}"
                            if name and urlx:
                                out.append((channel_name_normalize(name), urld))
                    return out
                except Exception:
                    return []

        results = await asyncio.gather(*(fetch_json(u) for u in valid_urls))
    return [c for sub in results for c in sub]

# zhgxtv模式获取频道（异步高并发）
async def get_channels_hgxtv(csv_file):
    urls = load_hosts(csv_file)

    ip_range_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        port = url[ip_end:]
        ip_range_urls.extend(generate_ip_range_urls(base_url, ip_address, port,
                                                    "/zhgxtv/Public/json/live_interface.txt"))

    channels = []
    async with new_session() as session:
        valid_urls = await check_urls_async(session, set(ip_range_urls))
        print(f"zhgxtv: C段扫描完成，存活主机 {len(valid_urls)} 个")

        sem = asyncio.Semaphore(PROBE_CONCURRENCY)

        async def fetch(url):
            async with sem:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT)) as resp:
                        content = await resp.text()
                except Exception:
                    return []
                out = []
                for line in content.split('\n'):
                    line = line.strip()
                    if not line or ',' not in line:
                        continue
                    try:
                        name, channel_url = line.split(',', 1)
                        urls_parts = channel_url.split('/', 3)
                        url_data_parts = url.split('/', 3)
                        urld = (f"{urls_parts[0]}//{url_data_parts[2]}/{urls_parts[3]}"
                                if len(urls_parts) >= 4 else f"{urls_parts[0]}//{url_data_parts[2]}")
                        out.append((channel_name_normalize(name), urld))
                    except Exception:
                        continue
                return out

        results = await asyncio.gather(*(fetch(u) for u in valid_urls))
    return [c for sub in results for c in sub]

# 测试频道速度并输出结果（异步高并发）
async def test_speed_and_output(channels, output_prefix="itvlist"):
    speed_results = []
    error_channels = []
    total = len(channels)
    done = 0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(SPEED_CONCURRENCY)

    async with new_session() as session:
        async def worker(channel_name, channel_url):
            nonlocal done
            async with sem:
                ok = None
                try:
                    channel_url_t = channel_url[:channel_url.rfind('/') + 1]
                    async with session.get(channel_url, timeout=aiohttp.ClientTimeout(total=SPEED_TIMEOUT)) as resp:
                        text = await resp.text()
                    lines = text.strip().split('\n')
                    ts_lists = [line for line in lines if not line.startswith('#')]
                    if not ts_lists:
                        raise Exception("No valid TS files found.")
                    ts_url = channel_url_t + ts_lists[0].split('/')[-1]
                    start_time = time.monotonic()
                    async with session.get(ts_url, timeout=aiohttp.ClientTimeout(total=SPEED_TIMEOUT)) as resp:
                        content = await resp.read()
                    response_time = time.monotonic() - start_time
                    if content:
                        file_size = len(content)
                        download_speed = file_size / response_time / 1024
                        normalized_speed = min(max(download_speed / 1024, 0.001), 100)
                        ok = (channel_name, channel_url, f"{normalized_speed:.3f} MB/s")
                except Exception:
                    ok = None
                async with lock:
                    done += 1
                    if ok:
                        speed_results.append(ok)
                    else:
                        error_channels.append((channel_name, channel_url))
                    if done % 50 == 0 or done == total:
                        progress = done / total * 100
                        print(f"可用频道：{len(speed_results)} 个 , 不可用频道：{len(error_channels)} 个 , "
                              f"总频道：{total} 个 ,总进度：{progress:.2f} %。")

        await asyncio.gather(*(worker(n, u) for n, u in channels))

    # 按速度排序并筛选每个频道最多15个源
    channel_sources = defaultdict(list)
    for channel_name, channel_url, speed in speed_results:
        channel_sources[channel_name].append((channel_url, speed))

    optimized_sources = []
    for channel_name, sources in channel_sources.items():
        sorted_sources = sorted(sources, key=lambda x: float(x[1].split()[0]), reverse=True)[:15]
        for url, speed in sorted_sources:
            optimized_sources.append((channel_name, url, speed))

    # 去重
    unique_channels = []
    seen = set()
    for channel_name, channel_url, speed in optimized_sources:
        key = (channel_name, channel_url)
        if key not in seen:
            unique_channels.append((channel_name, channel_url, speed))
            seen.add(key)

    # 对频道进行排序
    def custom_sort_key(item):
        name = item[0]
        if name.startswith('CCTV'):
            num = re.search(r'\d+', name)
            if num:
                return (0, int(num.group()))
            return (0, float('inf'))
        return (1, name)

    unique_channels.sort(key=custom_sort_key)

    def write_to_file(file, results, genre):
        channel_counters = {}
        for result in results:
            channel_name, channel_url, _ = result
            if genre == '央视频道' and 'CCTV' in channel_name or \
                    genre == '卫视频道' and '卫视' in channel_name or \
                    genre == '其他频道' and 'CCTV' not in channel_name and '卫视' not in channel_name and '测试' not in channel_name:
                if channel_name in channel_counters:
                    if channel_counters[channel_name] < 8:
                        file.write(f"{channel_name},{channel_url}\n")
                        channel_counters[channel_name] += 1
                else:
                    file.write(f"{channel_name},{channel_url}\n")
                    channel_counters[channel_name] = 1

    with open(f"{output_prefix}.txt", 'w', encoding='utf-8') as txt_file:
        txt_file.write('央视频道,#genre#\n')
        write_to_file(txt_file, unique_channels, '央视频道')
        txt_file.write('卫视频道,#genre#\n')
        write_to_file(txt_file, unique_channels, '卫视频道')
        txt_file.write('其他频道,#genre#\n')
        write_to_file(txt_file, unique_channels, '其他频道')

    with open(f"{output_prefix}.m3u", 'w', encoding='utf-8') as m3u_file:
        m3u_file.write('#EXTM3U\n')

        def write_to_m3u(file, results, genre):
            channel_counters = {}
            for result in results:
                channel_name, channel_url, _ = result
                if genre == '央视频道' and 'CCTV' in channel_name or \
                        genre == '卫视频道' and '卫视' in channel_name or \
                        genre == '其他频道' and 'CCTV' not in channel_name and '卫视' not in channel_name and '测试' not in channel_name:
                    if channel_name in channel_counters:
                        if channel_counters[channel_name] < 8:
                            file.write(f"#EXTINF:-1 group-title=\"{genre}\",{channel_name}\n")
                            file.write(f"{channel_url}\n")
                            channel_counters[channel_name] += 1
                    else:
                        file.write(f"#EXTINF:-1 group-title=\"{genre}\",{channel_name}\n")
                        file.write(f"{channel_url}\n")
                        channel_counters[channel_name] = 1

        write_to_m3u(m3u_file, unique_channels, '央视频道')
        write_to_m3u(m3u_file, unique_channels, '卫视频道')
        write_to_m3u(m3u_file, unique_channels, '其他频道')

    with open("speed.txt", 'w', encoding='utf-8') as speed_file:
        for result in unique_channels:
            speed_file.write(f"{','.join(result)}\n")

# 主入口
async def run(args):
    start = time.monotonic()
    channels = []
    if args.jsmpeg:
        channels.extend(await get_channels_alltv(args.jsmpeg))
    if args.zhgxtv:
        channels.extend(await get_channels_hgxtv(args.zhgxtv))
    if args.txiptv:
        channels.extend(await get_channels_newnew(args.txiptv))

    if not channels:
        print('请至少指定一个csv文件')
        return

    print(f"共发现 {len(channels)} 个频道源，开始测速……")
    await test_speed_and_output(channels, args.output)
    print(f"全部完成，总耗时 {time.monotonic() - start:.0f} 秒。")

def main():
    parser = argparse.ArgumentParser(description='多模式IPTV频道批量探测与测速（异步加速版）')
    parser.add_argument('--jsmpeg', help='jsmpeg-streamer模式csv文件')
    parser.add_argument('--txiptv', help='txiptv模式csv文件')
    parser.add_argument('--zhgxtv', help='zhgxtv模式csv文件')
    parser.add_argument('--output', default='itvlist', help='输出文件前缀')
    args = parser.parse_args()
    asyncio.run(run(args))

if __name__ == "__main__":
    main()
