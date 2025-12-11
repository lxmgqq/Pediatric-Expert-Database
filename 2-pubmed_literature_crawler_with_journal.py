import requests
from bs4 import BeautifulSoup
import csv
import time
import urllib.parse
import re
import random
import calendar
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
import math
import os
import glob


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

# -------------------------- 用户可在此处设置起始和终止日期 --------------------------
# 起始日期 (格式: YYYY/MM/DD)
START_DATE = "2015/01/01"

# 终止日期 (格式: YYYY/MM/DD 或 "Today" 表示今天)
END_DATE = "2025/08/26"  # 可改为具体日期如 "2024/06/30"
# ----------------------------------------------------------------------------------


def get_latest_version_file():
    """查找最新版本的CSV文件并正确提取日期"""
    pattern = './output/pubmed_results_ver.*.csv'
    files = glob.glob(pattern)

    if not files:
        print("未找到历史版本文件")
        return None, None

    # 按文件名中的日期排序
    files.sort(key=lambda x: extract_date_from_filename(x), reverse=True)
    latest_file = files[0]

    # 从文件名中提取版本日期
    version_date = extract_date_from_filename(latest_file)
    return latest_file, version_date


def extract_date_from_filename(file_path):
    """从文件名中提取日期字符串（YYYYMMDD）"""
    filename = os.path.basename(file_path)

    # 使用正则表达式匹配日期部分
    date_match = re.search(r'ver\.(\d{8})\.csv', filename)
    if date_match:
        return date_match.group(1)

    # 备用方法：尝试匹配8位数字
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        return date_match.group(1)

    # 如果都无法匹配，返回默认日期
    print(f"警告：无法从文件名 {filename} 中提取日期，使用默认日期")
    return "19700101"  # 默认日期


def load_existing_data(file_path):
    """加载现有数据并返回PMID集合和文献列表"""
    existing_pmids = set()
    existing_articles = []
    if not os.path.exists(file_path):
        return existing_pmids, existing_articles

    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'PMID' in row and row['PMID']:
                existing_pmids.add(row['PMID'])
                existing_articles.append(row)
    return existing_pmids, existing_articles


def get_total_results(search_url):
    """获取时间段内的总文献数"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 动态User-Agent
            headers[
                'User-Agent'] = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.0.0 Safari/537.36'

            response = requests.get(f"{search_url}&page=1", headers=headers, timeout=15)
            response.raise_for_status()

            if "CAPTCHA" in response.text:
                raise RuntimeError("触发反爬验证码")

            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取总文献数
            results_info = soup.find('span', class_='value')
            if not results_info:
                results_info = soup.find('span', class_='total-pages')

            if not results_info:
                raise ValueError("结果统计元素未找到")

            total_text = results_info.get_text(strip=True).replace(',', '')
            if total_text.isdigit():
                return int(total_text)
            else:
                raise ValueError(f"无效的结果数: {total_text}")

        except Exception as e:
            print(f"获取总结果数失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = 1.5 + random.random() * 3
                print(f"等待 {sleep_time:.1f} 秒后重试...")
                time.sleep(sleep_time)

    raise RuntimeError("无法获取总结果数")


def parse_page(page_url):
    """带重试机制的页面解析，提取期刊信息"""
    max_retries = 2
    backoff_factor = 3

    for attempt in range(max_retries):
        try:
            # 动态User-Agent
            headers[
                'User-Agent'] = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.0.0 Safari/537.36'

            response = requests.get(page_url, headers=headers, timeout=15)
            response.raise_for_status()

            if "CAPTCHA" in response.text:
                raise RuntimeError("触发反爬验证码")

            soup = BeautifulSoup(response.text, 'html.parser')
            articles = []

            for entry in soup.find_all('article', class_='full-docsum'):
                try:
                    # 标题处理
                    title_tag = entry.find('a', class_='docsum-title')
                    raw_title = ' '.join(title_tag.stripped_strings)
                    title = re.sub(r'\s+([.,;])', r'\1', raw_title).strip()

                    # 作者处理
                    authors_tag = entry.find('span', class_='docsum-authors')
                    authors = ' '.join(authors_tag.stripped_strings).strip() if authors_tag else ""

                    # PMID提取
                    pmid_span = entry.find('span', class_='docsum-pmid')
                    if pmid_span:
                        pmid = pmid_span.get_text(strip=True)
                    else:
                        pmid_link = entry.find('a', class_='docsum-title')['href']
                        pmid = urllib.parse.urlparse(pmid_link).path.split('/')[-1]
                        print(f"警告：未找到docsum-pmid元素，从URL提取PMID: {pmid}")

                    # 提取期刊、日期信息
                    date_span = entry.find('span', class_='docsum-journal-citation')
                    date_text = ""
                    journal_text = ""
                    
                    if date_span:
                        citation_text = date_span.get_text(strip=True)
                        
                        # 提取期刊信息
                        journal_match = re.search(r'^([^.;]+)', citation_text)
                        if journal_match:
                            journal_text = journal_match.group(1).strip()
                        
                        # 提取日期信息
                        date_match = re.search(r'(\d{4}\s+[A-Z][a-z]{2})', citation_text)
                        if date_match:
                            date_text = date_match.group(1)
                        else:
                            date_match = re.search(r'(\d{4}\s+\d{1,2})', citation_text)
                            if date_match:
                                year, month_num = date_match.group(1).split()
                                try:
                                    month_name = calendar.month_abbr[int(month_num)]
                                    date_text = f"{year} {month_name}"
                                except (ValueError, IndexError):
                                    date_text = f"{year} {month_num}"
                            else:
                                year_match = re.search(r'\b(19|20)\d{2}\b', citation_text)
                                if year_match:
                                    date_text = year_match.group(0)

                    articles.append({
                        'Title': title,
                        'Authors': authors,
                        'PMID': pmid,
                        'Journal': journal_text,
                        'Date': date_text
                    })
                except Exception as e:
                    print(f"解析条目失败: {str(e)}")
                    continue

            return articles

        except Exception as e:
            print(f"页面请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor ** (attempt + 1) + random.uniform(0, 1)
                print(f"等待 {sleep_time:.1f} 秒后重试...")
                time.sleep(sleep_time)

    print(f"无法获取页面: {page_url}")
    return []


def crawl_time_interval(search_url, total_results, existing_pmids):
    """爬取指定时间段内的所有文献，过滤已存在的PMID"""
    articles = []
    total_pages = math.ceil(total_results / 200)
    actual_pages = min(total_pages, 50)

    print(f"总文献数: {total_results} | 预计爬取 {actual_pages} 页")

    for page in range(1, actual_pages + 1):
        page_url = f"{search_url}&page={page}"
        print(f"爬取页面 {page}/{actual_pages}")
        page_articles = parse_page(page_url)

        if page_articles:
            # 过滤已存在的PMID
            new_articles = [article for article in page_articles if article['PMID'] not in existing_pmids]

            if len(page_articles) != len(new_articles):
                duplicates = len(page_articles) - len(new_articles)
                print(f"过滤掉 {duplicates} 篇已存在文献")

            if new_articles:
                articles.extend(new_articles)
                print(f"获取到 {len(new_articles)} 篇新文献")
            else:
                print("本页所有文献已存在，跳过")
        else:
            print("未获取到文献，可能遇到反爬措施")

        # 动态延迟
        delay = 2 + random.random() * 1
        print(f"等待 {delay:.1f} 秒后继续...")
        time.sleep(delay)

    pmid_count = sum(1 for article in articles if article['PMID'])
    print(f"成功提取PMID的新文献: {pmid_count}/{len(articles)} ({pmid_count / len(articles) * 100:.1f}%)")

    date_count = sum(1 for article in articles if article['Date'])
    print(f"成功提取日期的新文献: {date_count}/{len(articles)} ({date_count / len(articles) * 100:.1f}%)")
    
    journal_count = sum(1 for article in articles if article['Journal'])
    print(f"成功提取期刊的新文献: {journal_count}/{len(articles)} ({journal_count / len(articles) * 100:.1f}%)")

    return articles


def clean_base_url(original_url):
    """清理基础URL，移除分页和过滤器参数"""
    parsed = urlparse(original_url)
    query_params = parse_qs(parsed.query)

    for param in ['page', 'filter']:
        if param in query_params:
            del query_params[param]

    new_query = urlencode(query_params, doseq=True)
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    ))


def format_date(date_obj):
    """将日期对象格式化为YYYY/MM/DD字符串"""
    return date_obj.strftime("%Y/%m/%d")


def get_mid_date(start_date, end_date):
    """计算两个日期之间的中间日期"""
    start = datetime.strptime(start_date, "%Y/%m/%d")
    end = datetime.strptime(end_date, "%Y/%m/%d")

    delta = (end - start) / 2
    mid_date = start + delta

    return format_date(mid_date)


def process_time_interval(base_url, start_date, end_date, all_articles, existing_pmids, depth=0):
    """递归处理时间段"""
    indent = "  " * depth
    print(f"{indent}处理时间段 [{start_date} 至 {end_date}] (深度 {depth})")

    # URL编码日期
    start_encoded = urllib.parse.quote(start_date, safe='')
    end_encoded = urllib.parse.quote(end_date, safe='')

    # 构建带时间过滤器的URL
    time_filter = f"&filter=dates.{start_encoded}-{end_encoded}"
    search_url = base_url + time_filter

    try:
        # 获取该时间段的总结果数
        total_results = get_total_results(search_url)
        print(f"{indent}时间段内找到 {total_results} 篇文献")

        if total_results == 0:
            print(f"{indent}跳过无结果的时间段")
            return

        # 如果不超过10,000篇，直接爬取
        if total_results <= 10000:
            articles = crawl_time_interval(search_url, total_results, existing_pmids)
            if articles:
                all_articles.extend(articles)
                print(f"{indent}成功爬取 {len(articles)} 篇新文献")
            return

        # 如果超过10,000篇，分割时间段
        print(f"{indent}结果超过10,000条 ({total_results})，分割时间段...")

        # 计算中间日期
        mid_date = get_mid_date(start_date, end_date)

        # 递归处理前半段
        process_time_interval(base_url, start_date, mid_date, all_articles, existing_pmids, depth + 1)

        # 递归处理后半段
        next_day = (datetime.strptime(mid_date, "%Y/%m/%d") + timedelta(days=1)).strftime("%Y/%m/%d")
        process_time_interval(base_url, next_day, end_date, all_articles, existing_pmids, depth + 1)

    except Exception as e:
        print(f"{indent}处理时间段失败: {str(e)}")

    # 时间段之间的延迟
    delay = 2 + random.random() * 1
    print(f"{indent}等待 {delay:.1f} 秒后继续...")
    time.sleep(delay)


def remove_duplicate_pmids(csv_file):
    """读取CSV文件，删除重复PMID的行"""
    unique_articles = []
    seen_pmids = set()
    duplicate_count = 0

    # 读取文件内容
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pmid = row['PMID']

            # 检查PMID是否为空或已存在
            if not pmid or pmid in seen_pmids:
                duplicate_count += 1
                if pmid:
                    print(f"发现重复PMID: {pmid}，标题: {row['Title']}")
                else:
                    print(f"发现空PMID的条目，标题: {row['Title']}")
                continue

            seen_pmids.add(pmid)
            unique_articles.append(row)

    if duplicate_count == 0:
        print("未发现重复PMID的条目")
        return

    # 重新写入文件（覆盖原文件）
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(unique_articles)

    print(f"已删除 {duplicate_count} 个重复条目，保留 {len(unique_articles)} 个唯一条目")


def merge_new_articles(existing_articles, new_articles):
    """合并新旧文献数据，保留所有字段"""
    # 创建PMID到文献的映射
    pmid_to_article = {article['PMID']: article for article in existing_articles}

    # 更新或添加新文献
    for new_article in new_articles:
        pmid = new_article['PMID']
        if pmid in pmid_to_article:
            # 如果已有该PMID的文献，合并字段
            existing = pmid_to_article[pmid]
            # 保留新数据中的期刊信息（如果存在）
            if 'Journal' in new_article and new_article['Journal']:
                existing['Journal'] = new_article['Journal']
            existing.update(new_article)  # 用新数据更新旧数据
        else:
            # 否则添加新文献
            pmid_to_article[pmid] = new_article

    # 返回合并后的文献列表
    return list(pmid_to_article.values())


def validate_date(date_str, is_end_date=False, start_date=None):
    """验证日期的合法性"""
    # 验证格式
    try:
        date = datetime.strptime(date_str, "%Y/%m/%d")
    except ValueError:
        raise ValueError(f"日期格式错误！请使用 YYYY/MM/DD 格式（例如：2024/05/31）")

    # 如果是终止日期且提供了起始日期，验证终止日期不早于起始日期
    if is_end_date and start_date:
        start = datetime.strptime(start_date, "%Y/%m/%d")
        if date < start:
            raise ValueError(f"终止日期 {date_str} 不能早于起始日期 {start_date}")

    return date_str  # 验证通过，返回原字符串


def main():
    # 获取当前日期
    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    current_date = format_date(now)

    # 处理终止日期：如果是"Today"则使用当前日期
    if END_DATE.strip().lower() == "today":
        end_date = current_date
        print(f"终止日期设置为今天: {end_date}")
    else:
        end_date = END_DATE
        print(f"使用用户指定的终止日期: {end_date}")

    # 验证起始日期和终止日期
    try:
        # 验证起始日期
        validated_start_date = validate_date(START_DATE)
        # 验证终止日期，确保不早于起始日期
        validated_end_date = validate_date(end_date, is_end_date=True, start_date=validated_start_date)
    except ValueError as e:
        print(f"日期验证失败：{e}")
        return  # 验证失败，退出程序

    # 读取基础URL
    with open('./output/pubmed_urls.txt', 'r', encoding='utf-8') as f:
        original_url = f.read().strip()

    base_url = clean_base_url(original_url)
    print(f"清理后的基础URL: {base_url}")

    # 查找最新版本文件
    latest_file, version_date = get_latest_version_file()
    existing_pmids = set()
    existing_articles = []
    all_articles = []

    # 处理历史爬取日期
    last_crawl_date = None
    if latest_file and version_date:
        try:
            last_crawl_date = datetime.strptime(version_date, "%Y%m%d").strftime("%Y/%m/%d")
            print(f"找到最新版本文件: {latest_file} (版本日期: {version_date})")
            print(f"历史爬取日期: {last_crawl_date}")
        except ValueError:
            print(f"警告：无效的历史日期格式 '{version_date}'，忽略历史日期")
            last_crawl_date = None

        # 加载现有PMID和文献数据
        existing_pmids, existing_articles = load_existing_data(latest_file)
        print(f"加载 {len(existing_pmids)} 个现有PMID, {len(existing_articles)} 篇现有文献")

    # 确定实际爬取的起始日期
    if last_crawl_date and last_crawl_date >= validated_start_date:
        # 有历史数据，且历史爬取日期在起始日期之后，使用历史爬取日期作为起始点（增量爬取）
        actual_start_date = last_crawl_date
        print(f"将进行增量爬取: {actual_start_date} 至 {validated_end_date}")
    else:
        # 无历史数据或历史爬取日期在起始日期之前，使用设定的起始日期（完整爬取）
        actual_start_date = validated_start_date
        if latest_file:
            print(f"将进行补充爬取: {actual_start_date} 至 {validated_end_date}")
        else:
            print(f"将进行完整爬取: {actual_start_date} 至 {validated_end_date}")

    # 确保爬取的起始日期不晚于终止日期
    if actual_start_date <= validated_end_date:
        # 爬取文献
        new_articles = []
        process_time_interval(base_url, actual_start_date, validated_end_date, new_articles, existing_pmids)

        if new_articles:
            print(f"获取到 {len(new_articles)} 篇新文献")
            # 合并新旧文献数据
            all_articles = merge_new_articles(existing_articles, new_articles)
        else:
            print("没有发现新文献")
            all_articles = existing_articles
    else:
        print(f"无需爬取新文献（起始日期 {actual_start_date} 已晚于终止日期 {validated_end_date}）")
        all_articles = existing_articles

    # 生成新版本文件名
    new_filename = f"./output/pubmed_results_ver.{today_str}.csv"

    # 保存结果
    if all_articles:
        # 确定所有可能的字段名，确保Journal字段存在
        all_fields = set()
        for article in all_articles:
            all_fields.update(article.keys())

        # 确保基础字段在前面，包含Journal字段
        base_fields = ['Title', 'Authors', 'PMID', 'Journal', 'Date']
        fieldnames = base_fields + [field for field in all_fields if field not in base_fields]

        # 保存CSV
        with open(new_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_articles)

        # 去重
        print(f"完成！共保存 {len(all_articles)} 篇文献到 {new_filename}")
        print("开始检查并删除重复PMID的条目...")
        remove_duplicate_pmids(new_filename)
    else:
        print("没有文献数据可保存")


if __name__ == "__main__":
    main()
    