import pandas as pd
import ast
from collections import Counter
import os

# ---------------- 配置区域 ----------------
AUTHOR_FILE = 'output/author_info_processed_updated.csv'
PUBMED_FILE = 'output/pubmed_results_with_keywords.csv'
OUTPUT_FILE = 'output/top_50_authors_research_hotspots.csv'

EXCLUDE_KEYWORDS = {
    'Pediatric Patients', 'Child', 'Male', 'Female', 'Humans', 'Childs',
    'postoperative complications', 'Retrospective Studies', 'Treatment Outcome',
    'Child, Preschool', 'Infant, Newborn', 'Prospective Studies',
    'Follow-Up Studies', 'Specialties, Surgical', 'Surveys and Questionnaires',
    'Surgical Procedures, Operative', 'Adolescent', 'Infant', 'Length of stay',
    'Risk Factors', 'United States', 'Quality of Life'
}

def parse_list_string(list_str):
    if pd.isna(list_str) or list_str == '':
        return []
    try:
        parsed = ast.literal_eval(list_str)
        if isinstance(parsed, list):
            return parsed
        return []
    except (ValueError, SyntaxError):
        return []

def get_keywords_for_paper(row):
    # 1. 尝试获取交集关键词
    kw_mesh_inter = parse_list_string(row.get('Keywords_and_MeSH_terms', ''))
    if kw_mesh_inter:
        return kw_mesh_inter
    
    # 2. 回退策略：使用 MeSH_API 并过滤
    mesh_api = parse_list_string(row.get('MeSH_API', ''))
    if mesh_api:
        filtered_mesh = [
            term for term in mesh_api 
            if term not in EXCLUDE_KEYWORDS
        ]
        return filtered_mesh
    return []

def read_csv_robust(file_path):
    """
    尝试多种编码格式读取 CSV 文件，解决 UnicodeDecodeError
    """
    encodings = ['utf-8', 'gbk', 'ISO-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"错误：找不到文件 - {file_path}")
            return None
    print(f"错误：无法解码文件 {file_path}，请检查文件格式。")
    return None

def main():
    print("正在读取数据...")
    
    # --- 修改点：使用增强的读取函数 ---
    df_authors = read_csv_robust(AUTHOR_FILE)
    df_papers = read_csv_robust(PUBMED_FILE)

    if df_authors is None or df_papers is None:
        return

    # --- 步骤 1: 构建 PMID -> 关键词 的映射字典 ---
    print("正在构建论文关键词索引...")
    pmid_to_keywords = {}
    
    # 确保 PMID 统一为字符串格式
    df_papers['PMID'] = df_papers['PMID'].astype(str)
    
    for _, row in df_papers.iterrows():
        pmid = row['PMID']
        keywords = get_keywords_for_paper(row)
        pmid_to_keywords[pmid] = keywords

    # --- 步骤 2: 获取发文量前 50 的作者 ---
    print("正在筛选前 50 名作者...")
    df_authors['PMID_Count'] = pd.to_numeric(df_authors['PMID_Count'], errors='coerce').fillna(0)
    
    top_50_authors = df_authors.sort_values(by='PMID_Count', ascending=False).head(50)

    # --- 步骤 3: 统计每位作者的热点方向 ---
    print("正在分析作者研究热点...")
    results = []

    for _, author_row in top_50_authors.iterrows():
        author_name = author_row['Author']
        pmid_list_str = author_row['PMID']
        pmid_list = parse_list_string(pmid_list_str)
        
        all_keywords = []
        for pmid in pmid_list:
            pmid_str = str(pmid)
            if pmid_str in pmid_to_keywords:
                all_keywords.extend(pmid_to_keywords[pmid_str])
        
        if all_keywords:
            top_5_keywords = Counter(all_keywords).most_common(5)
            hotspots_str = "; ".join([f"{k} ({v})" for k, v in top_5_keywords])
        else:
            hotspots_str = "无有效关键词数据"

        results.append({
            'Author': author_name,
            'AuthorID': author_row['AuthorID'],
            'Total_Papers': author_row['PMID_Count'],
            'Main_Affiliation': author_row.get('MainAffiliation', 'N/A'),
            'Country': author_row.get('Country', 'N/A'),
            'Top_5_Research_Hotspots': hotspots_str
        })

    # --- 步骤 4: 保存结果 ---
    result_df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # 保存时使用 utf-8-sig 以便 Excel 打开不乱码
    result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"处理完成！结果已保存至: {OUTPUT_FILE}")
    print("前 5 名结果预览：")
    print(result_df[['Author', 'Total_Papers', 'Top_5_Research_Hotspots']].head().to_string())

if __name__ == "__main__":
    main()