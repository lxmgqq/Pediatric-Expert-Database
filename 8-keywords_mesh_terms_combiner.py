import pandas as pd
import ast
import difflib
import re
import numpy as np

# ================== 关键配置 ==================
EXCLUDE_KEYWORDS = [
    'Pediatric Patients',
    'Child',
    'Male',
    'Female',
    'Humans',
    'Childs',
    'postoperative complications',
    'Retrospective Studies',
    'Treatment Outcome',
    'Child, Preschool',
    'Infant, Newborn',
    'Prospective Studies',
    'Follow-Up Studies',
    'Specialties, Surgical',
    'Surveys and Questionnaires',
    'Surgical Procedures, Operative',
    'Adolescent',
    'Infant',
    'Length of stay',
    'Risk Factors',
    'United States',
    'Quality of Life'
]
SIMILARITY_THRESHOLD = 0.6
MAX_KEYWORDS = 5

def clean_keyword(kw):
    """清洗关键词（去除标点、转小写、去空格）"""
    return re.sub(r'[^\w\s]', '', kw).lower().strip()

def is_similar_to_excluded(clean_term, exclude_list, threshold=SIMILARITY_THRESHOLD):
    """检查当前清洗后的词是否与排除列表中任何词的相似度≥阈值"""
    if not exclude_list:  # 排除列表为空时直接返回False
        return False
    # 计算与排除列表中每个词的相似度
    for excl in exclude_list:
        clean_excl = clean_keyword(excl)
        similarity = difflib.SequenceMatcher(None, clean_term, clean_excl).ratio()
        if similarity >= threshold:
            return True  # 有一个相似即返回True
    return False  # 所有都不相似返回False

def process_paper(row):
    # 解析关键词（处理空值）
    try:
        keywords = ast.literal_eval(row['Keywords']) if pd.notna(row['Keywords']) else []
    except:
        keywords = []
    
    try:
        mesh_terms = ast.literal_eval(row['MeSH_API']) if pd.notna(row['MeSH_API']) else []
    except:
        mesh_terms = []
    
    # 情况1：两者都为空 → 跳过
    if not keywords and not mesh_terms:
        return []
    
    # 情况2：Keywords为空，MeSH_API不为空（仅从Mesh提取有效词，保留原逻辑）
    if not keywords and mesh_terms:
        clean_mesh = [clean_keyword(term) for term in mesh_terms]
        valid_mesh = [
            term for term, clean in zip(mesh_terms, clean_mesh)
            if clean not in [clean_keyword(excl) for excl in EXCLUDE_KEYWORDS]  # 不在排除列表中
            and not is_similar_to_excluded(clean, EXCLUDE_KEYWORDS)  # 与排除词相似度低于阈值
        ]
        valid_mesh.sort(key=lambda x: len(x), reverse=True)  # 按长度倒序
        return valid_mesh[:MAX_KEYWORDS]
    
    # 情况3：MeSH_API为空，Keywords不为空（仅从Keywords提取有效词，保留原逻辑）
    if not mesh_terms and keywords:
        valid_keywords = [
            term for term in keywords
            if clean_keyword(term) not in [clean_keyword(excl) for excl in EXCLUDE_KEYWORDS]
            and not is_similar_to_excluded(clean_keyword(term), EXCLUDE_KEYWORDS)
        ]
        return valid_keywords[:MAX_KEYWORDS]
    
    # 情况4：两者都不为空（仅保留相似度匹配的关键词，不补充剩余）
    clean_keywords = [clean_keyword(kw) for kw in keywords]
    clean_mesh = [clean_keyword(term) for term in mesh_terms]
    
    matched_keywords = []
    matched_clean = set()  # 记录已匹配的Mesh词（避免重复）
    
    # 仅提取关键词与Mesh中相似度达标的词
    for kw, clean_kw in zip(keywords, clean_keywords):
        # 跳过排除词及相似词
        if clean_kw in [clean_keyword(excl) for excl in EXCLUDE_KEYWORDS] or is_similar_to_excluded(clean_kw, EXCLUDE_KEYWORDS):
            continue
            
        for mesh, clean_mesh_term in zip(mesh_terms, clean_mesh):
            # 跳过排除词及相似词
            if clean_mesh_term in [clean_keyword(excl) for excl in EXCLUDE_KEYWORDS] or is_similar_to_excluded(clean_mesh_term, EXCLUDE_KEYWORDS):
                continue
                
            # 计算关键词与Mesh词的相似度
            similarity = difflib.SequenceMatcher(None, clean_kw, clean_mesh_term).ratio()
            if similarity >= SIMILARITY_THRESHOLD:
                matched_keywords.append(kw)
                matched_clean.add(clean_mesh_term)  # 标记为已匹配，避免重复
                break  # 匹配到一个后跳出当前Mesh循环，避免同一关键词匹配多个Mesh词
    
    # 直接返回匹配结果（不补充，最多取MAX_KEYWORDS个）
    return matched_keywords[:MAX_KEYWORDS]

def main():
    input_path = "output/pubmed_results_with_keywords.csv"
    
    # 尝试多种编码解决解码错误
    try:
        df = pd.read_csv(input_path, encoding='utf-8-sig')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(input_path, encoding='latin-1')
        except UnicodeDecodeError:
            df = pd.read_csv(input_path, encoding='cp1252')
    
    # 删除所有以"Unnamed"开头的空列
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    
    if 'Keywords_and_MeSH_terms' not in df.columns:
        df['Keywords_and_MeSH_terms'] = np.nan
    
    df['Keywords_and_MeSH_terms'] = df.apply(process_paper, axis=1)
    
    # 保存结果
    output_path = "output/pubmed_results_with_keywords_processed.csv"
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"处理完成！结果已保存至: {output_path}")
    print(f"共处理 {len(df)} 篇论文，其中 {len(df[df['Keywords_and_MeSH_terms'].apply(len) == 0])} 篇跳过（两者为空或无匹配关键词）")

if __name__ == "__main__":
    main()