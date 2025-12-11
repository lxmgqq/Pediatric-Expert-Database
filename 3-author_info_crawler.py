'''
20250714 2050:
目前的代码功能基本是完善的，目前正在测试从头开始爬取的功能；后续需要测试一下“增量更新”的功能。
还有，看一下pubmed_results_ver.*.csv文件中摘要和关键字的爬取是否正确。
还有，爬取的author_info_ver.*.csv文件中是否会有PMID重复的情况。
'''


import os
import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup, UnicodeDammit
from datetime import datetime
import re
import glob
import time
import ast

# --- 配置区 ---
OUTPUT_DIR = 'output'
BASE_URL = 'https://pubmed.ncbi.nlm.nih.gov/'
TODAY_STR = datetime.now().strftime('%Y%m%d')
TEST_MODE_LIMIT = 30

# --- 核心功能函数 ---

def parse_pubmed_date(date_str):
    if pd.isna(date_str):
        return None
    date_str = str(date_str).strip()
    try:
        return datetime.strptime(date_str, '%Y %b')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y')
        except ValueError:
            return None

def detect_encoding(content, headers=None):
    if headers and 'content-type' in headers:
        content_type = headers['content-type'].lower()
        for part in content_type.split(';'):
            part = part.strip()
            if part.startswith('charset='):
                return part[8:].strip()
    
    dammit = UnicodeDammit(content)
    return dammit.original_encoding

def scrape_pmid_details(pmid):
    url = f"{BASE_URL}{pmid}/"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        
        encoding = detect_encoding(response.content, response.headers)
        if encoding:
            response.encoding = encoding
            
        html_content = response.text
        
    except requests.RequestException as e:
        print(f"  -! 无法访问 PMID {pmid} 页面: {e}")
        return None

    soup = BeautifulSoup(html_content, 'html.parser')

    abstract_div = soup.find('div', {'id': 'eng-abstract'})
    abstract_text = ''
    if abstract_div:
        abstract_text = abstract_div.get_text(separator=' ', strip=True)

    keywords_div = soup.find('div', class_='keywords')
    keywords_text = ''
    if keywords_div and keywords_div.find('p'):
        keywords_text = ', '.join([kw.text.strip() for kw in keywords_div.find_all(class_='keyword-item')])

    affiliation_map = {}
    affiliation_list = soup.select('div.affiliations ul.item-list li')
    for li in affiliation_list:
        key_sup = li.find('sup')
        if key_sup:
            key = key_sup.get_text(strip=True)
            key_sup.decompose()
            affiliation_map[key] = li.get_text(strip=True)

    authors_data = []
    authors_list_div = soup.find('div', class_='authors-list')
    if authors_list_div:
        for item in authors_list_div.find_all('span', class_='authors-list-item'):
            name_tag = item.find('a', class_='full-name')
            if not name_tag:
                continue
            
            author_name = name_tag.get_text(strip=True)
            
            affiliation_keys = [a.get_text(strip=True) for a in item.select('sup.affiliation-links a')]
            author_affiliations = [affiliation_map.get(key, '') for key in affiliation_keys]
            author_affiliations_str = '; '.join(filter(None, author_affiliations))

            authors_data.append({
                'name': author_name,
                'affiliation': author_affiliations_str
            })

    return {
        'abstract': abstract_text,
        'keywords': keywords_text,
        'authors': authors_data
    }

def main(test_mode=False, specific_pmids=None):
    print("--- 开始执行PubMed数据更新与爬取任务 ---")
    
    if test_mode:
        print("⚠️ 测试模式已启用，将仅爬取前30篇文献 ⚠️")
    
    if specific_pmids:
        print(f"⚠️ 指定PMID模式: 将仅爬取以下PMID的文献: {specific_pmids}")
    
    if not os.path.exists(OUTPUT_DIR):
        print(f"创建文件夹: '{OUTPUT_DIR}'")
        os.makedirs(OUTPUT_DIR)

    pubmed_results_files = glob.glob(os.path.join(OUTPUT_DIR, 'pubmed_results_ver.*.csv'))
    if not pubmed_results_files:
        print(f"!!错误: 在 '{OUTPUT_DIR}' 文件夹中未找到 'pubmed_results_ver.*.csv' 文件。")
        return
    latest_pubmed_file = max(pubmed_results_files, key=os.path.getctime)
    print(f"-> 读取主数据文件: {latest_pubmed_file}")
    df_pubmed = pd.read_csv(latest_pubmed_file)

    target_pmids = []
    existing_author_df = None
    start_date_for_filter = datetime(2015, 1, 1)

    author_info_files = glob.glob(os.path.join(OUTPUT_DIR, 'author_info_ver.*.csv'))
    latest_author_file = max(author_info_files, key=os.path.getctime) if author_info_files else None

    if latest_author_file:
        print(f"-> 发现已存在的作者信息文件: {os.path.basename(latest_author_file)}")
        file_date_match = re.search(r'(\d{8})', os.path.basename(latest_author_file))
        
        if file_date_match.group(1) == TODAY_STR:
            print("== 今日的作者信息文件已存在。脚本将不会重复执行。")
            return
        
        print("-> 模式: 增量更新")
        last_run_date = datetime.strptime(file_date_match.group(1), '%Y%m%d')
        start_date_for_filter = last_run_date.replace(day=1)
        print(f"-> 起始日期: {start_date_for_filter.strftime('%Y-%m-%d')}")
        
        try:
            existing_author_df = pd.read_csv(latest_author_file, converters={'PMID': ast.literal_eval})
        except (ValueError, SyntaxError):
             print(f"  -! 警告: 无法解析 PMID 列，将作为纯文本处理。")
             existing_author_df = pd.read_csv(latest_author_file)

    else:
        print("-> 未发现作者信息文件。")
        print("-> 模式: 首次全量爬取")
        print(f"-> 将爬取所有自 2015-01-01 以来的文献。")

    if specific_pmids:
        # 使用指定的PMID列表
        print(f"-> 将爬取 {len(specific_pmids)} 个指定的PMID")
        # 过滤掉不在主文件中的PMID
        valid_pmids = [pmid for pmid in specific_pmids if pmid in df_pubmed['PMID'].values]
        invalid_pmids = [pmid for pmid in specific_pmids if pmid not in df_pubmed['PMID'].values]
        
        if invalid_pmids:
            print(f"  -! 警告: 以下PMID不在主文件中: {invalid_pmids}")
            
        if not valid_pmids:
            print("  -! 错误: 指定的PMID中没有有效的PMID")
            return
            
        df_to_scrape = df_pubmed[df_pubmed['PMID'].isin(valid_pmids)].copy()
        
        # 即使指定了PMID，也只处理那些没有摘要的记录（除非强制覆盖）
        if 'Abstract' in df_pubmed.columns:
            pmids_with_abstract = df_pubmed.dropna(subset=['Abstract'])['PMID'].unique()
            df_to_scrape = df_to_scrape[~df_to_scrape['PMID'].isin(pmids_with_abstract)]
            
        target_pmids = df_to_scrape['PMID'].unique().tolist()
        
        if len(target_pmids) < len(valid_pmids):
            print(f"  -! 注意: 指定的PMID中已有 {len(valid_pmids) - len(target_pmids)} 个有摘要内容，将跳过这些PMID")
            
    else:
        # 常规模式：根据日期筛选
        df_pubmed['parsed_date'] = df_pubmed['Date'].apply(parse_pubmed_date)
        df_to_scrape = df_pubmed[df_pubmed['parsed_date'] >= start_date_for_filter].copy()
        
        if 'Abstract' in df_pubmed.columns:
            pmids_with_abstract = df_pubmed.dropna(subset=['Abstract'])['PMID'].unique()
            df_to_scrape = df_to_scrape[~df_to_scrape['PMID'].isin(pmids_with_abstract)]

        target_pmids = df_to_scrape['PMID'].unique().tolist()

    if not target_pmids:
        print("\n== 没有发现需要爬取的新文献。即将退出。")
        if latest_author_file:
             new_author_filename = os.path.join(OUTPUT_DIR, f'author_info_ver.{TODAY_STR}.csv')
             pd.read_csv(latest_author_file).to_csv(new_author_filename, index=False, encoding='utf-8-sig')
             print(f"-> 已将旧数据更新为今日版本: {new_author_filename}")
        return

    if test_mode and len(target_pmids) > TEST_MODE_LIMIT:
        print(f"-> 测试模式: 从 {len(target_pmids)} 篇文献中选取前 {TEST_MODE_LIMIT} 篇进行爬取")
        target_pmids = target_pmids[:TEST_MODE_LIMIT]
    else:
        print(f"\n--- 共发现 {len(target_pmids)} 篇新文献需要处理 ---")

    new_abstracts = {}
    new_keywords = {}
    all_new_author_entries = []

    for i, pmid in enumerate(target_pmids):
        print(f"[{i+1}/{len(target_pmids)}] 正在爬取 PMID: {pmid}")
        details = scrape_pmid_details(pmid)
        if details:
            new_abstracts[pmid] = details['abstract']
            new_keywords[pmid] = details['keywords']
            for author in details['authors']:
                all_new_author_entries.append({
                    'Author': author['name'],
                    'Affiliation': author['affiliation'],
                    'PMID': str(pmid)
                })
        time.sleep(0.5)

    print("\n--- 爬取完成，开始处理和保存数据 ---")

    print(f"-> 正在更新主文件: {os.path.basename(latest_pubmed_file)} ...")
    if 'Abstract' not in df_pubmed.columns:
        df_pubmed['Abstract'] = pd.NA
    if 'Keywords' not in df_pubmed.columns:
        df_pubmed['Keywords'] = pd.NA

    df_pubmed.set_index('PMID', inplace=True)
    df_pubmed['Abstract'].update(pd.Series(new_abstracts))
    df_pubmed['Keywords'].update(pd.Series(new_keywords))
    df_pubmed.reset_index(inplace=True)
    
    if 'parsed_date' in df_pubmed.columns:
        df_pubmed.drop(columns=['parsed_date'], inplace=True)
    
    df_pubmed.to_csv(latest_pubmed_file, index=False, encoding='utf-8-sig')
    print(f"-> 主文件更新成功！")

    if not all_new_author_entries:
        print("-> 本次没有爬取到新的作者信息，作者文件将不被更新。")
        return
        
    print("-> 正在生成作者信息文件...")
    new_author_df = pd.DataFrame(all_new_author_entries)

    if existing_author_df is not None:
        if not (isinstance(existing_author_df['PMID'].iloc[0], list)):
            print("  -! 警告: 旧作者文件中的PMID列不是列表格式，聚合功能可能受限。")
            existing_author_df['PMID'] = existing_author_df['PMID'].apply(lambda x: [x] if not isinstance(x, list) else x)

        new_author_df['PMID'] = new_author_df['PMID'].apply(lambda x: [x])
        combined_df = pd.concat([existing_author_df, new_author_df], ignore_index=True)
    else:
        combined_df = new_author_df
        combined_df['PMID'] = combined_df['PMID'].apply(lambda x: [x])

    aggregated_authors = combined_df.groupby(['Author', 'Affiliation'])['PMID'].sum().reset_index()

    aggregated_authors['PMID'] = aggregated_authors['PMID'].apply(lambda pmid_list: sorted(list(set(pmid_list))))
    aggregated_authors['PMID_Count'] = aggregated_authors['PMID'].apply(len)
    
    aggregated_authors['PMID'] = aggregated_authors['PMID'].astype(str)

    author_info_output_path = os.path.join(OUTPUT_DIR, f'author_info_ver.{TODAY_STR}.csv')
    aggregated_authors.to_csv(author_info_output_path, index=False, encoding='utf-8-sig')
    print(f"-> 作者信息文件生成成功: {author_info_output_path}")

    print("\n--- 所有任务执行完毕 ---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PubMed文献爬取工具')
    parser.add_argument('--test', action='store_true', help='启用测试模式，只爬取30篇文献')
    parser.add_argument('--pmids', nargs='+', help='指定要爬取的PMID列表，用空格分隔')
    
    args = parser.parse_args()
    
    # 确保PMID是字符串类型
    specific_pmids = [str(pmid) for pmid in args.pmids] if args.pmids else None
    
    main(test_mode=args.test, specific_pmids=specific_pmids)