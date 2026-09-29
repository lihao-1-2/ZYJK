import os
import csv
import requests
from datetime import datetime

CSV_SOURCES = {
    "txiptv.csv": "https://raw.githubusercontent.com/alantang1977/TVCrazy/main/txiptv.csv",
    "jsmpeg.csv": "https://raw.githubusercontent.com/alantang1977/TVCrazy/main/jsmpeg.csv",
    "zhgxtv.csv": "https://raw.githubusercontent.com/alantang1977/TVCrazy/main/zhgxtv.csv"
}

HISTORY_DIR = "history"
os.makedirs(HISTORY_DIR, exist_ok=True)

def fetch_csv(file_name, url):
    """
    从网络获取 IPTV CSV 文件内容
    """
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.text

def save_history(file_path, content):
    """
    变更时自动备份文件到 history/，按时间戳命名
    """
    basename = os.path.basename(file_path)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    hist_file = os.path.join(HISTORY_DIR, f"{basename}.{timestamp}.bak")
    with open(hist_file, "w", encoding="utf-8") as f:
        f.write(content)

def is_valid_csv(content):
    """
    检查CSV文件内容是否有效（有表头且有数据行）
    """
    try:
        lines = [line for line in content.splitlines() if line.strip()]
        reader = csv.reader(lines)
        header = next(reader)
        row = next(reader)
        return bool(header) and bool(row)
    except Exception:
        return False

def update_csv(file_name, url):
    print(f"检查并同步: {file_name}")
    local_content = ""
    if os.path.exists(file_name):
        with open(file_name, "r", encoding="utf-8") as f:
            local_content = f.read()
    # 获取远程内容
    try:
        new_content = fetch_csv(file_name, url)
    except Exception as e:
        print(f"获取 {file_name} 失败: {e}")
        return
    if not is_valid_csv(new_content):
        print(f"{file_name} 下载内容无效，跳过。")
        return
    # 内容有变则备份并写入
    if new_content != local_content:
        print(f"{file_name} 内容有变，自动保存历史并更新。")
    #    if local_content.strip():
    #       save_history(file_name, local_content)
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(new_content)
    else:
        print(f"{file_name} 无变化。")

def main():
    for file_name, url in CSV_SOURCES.items():
        update_csv(file_name, url)

if __name__ == "__main__":
    main()
