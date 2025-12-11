"""
代码功能：
使用大语言模型对作者单位信息进行判断并创建id列区分不同作者，
同时完整记录模型的思考过程用于分析，支持断点续传
"""

import pandas as pd
import requests
import json
import time
import re
import unicodedata
import logging
import os
import glob
from datetime import datetime
from typing import Tuple

# 设置日志记录（确保能捕获模型完整思考过程）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"author_merge_with_thoughts_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------- 并查集类用于高效合并 ----------------------
class UnionFind:
    """并查集类，用于高效合并相似记录"""
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank = [0] * n
        
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x, y):
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return
            
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1

# ---------------------- 辅助函数：提取地址中的"城市+州"信息 ----------------------
def extract_location(affiliation: str) -> str:
    """提取单位的"城市+州"信息（适配英文地址格式）"""
    if not affiliation:
        return ""
    location_pattern = r'([A-Za-z\s]+),\s*([A-Z]{2})\s*,?\s*(USA|United States|[\d\s]+)?'
    match = re.search(location_pattern, affiliation)
    if match:
        city = match.group(1).strip()
        state = match.group(2).strip()
        return f"{city},{state}"
    return ""

# ---------------------- 核心函数：调用模型并记录完整思考过程 ----------------------
def are_authors_same(affiliation1: str, affiliation2: str, author_name: str, model_endpoint: str) -> Tuple[bool, str]:
    """
    使用大语言模型判断两个同名作者是否为同一人
    返回：(是否为同一人, 模型完整思考过程)
    """
    if pd.isna(affiliation1) or pd.isna(affiliation2) or not affiliation1 or not affiliation2:
        logger.warning(f"作者 {author_name}：存在空单位信息，直接判定为不同人")
        return False, "存在空单位信息，直接判定为不同人"
    
    prompt = f"""
任务：仅基于单位信息，判断两位同名作者是否为同一人，严格遵循以下优先级规则：

1. 地理关联性规则：
   - 若两个单位的"城市+州"不同（如 Washington,DC vs Atlanta,GA），且无明确隶属关系（如同一医院分院、同一大学校区），判定为不同人；
   - 若"城市+州"相同，继续判断机构隶属关系。

2. 机构隶属关系规则：
   - 若两个单位属于同一医疗系统/大学（如"Children's National Medical Center"与"Children's National Health System"），判定为同一人；
   - 若属于完全不同机构（如"Children's National"与"Emory University"），判定为不同人。

3. 部门差异规则：
   - 仅部门不同（如"General & Thoracic Surgery" vs "Pediatric Surgery"）但机构+地理相同，判定为同一人。

请先详细分析两个单位的地理信息和机构关系，然后给出判断结果。
分析过程要清晰展示你的思考逻辑，最后必须以"判断结果：是"或"判断结果：否"结束。

作者姓名: {author_name}
单位信息1: {affiliation1}
单位信息2: {affiliation2}
"""
    
    try:
        response = requests.post(
            f"{model_endpoint}/api/generate",
            json={
                "model": "deepseek-r1:70b",
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "max_tokens": 500  # 增加token限制以确保捕获完整思考过程
                }
            },
            timeout=120  # 延长超时时间，确保模型有足够时间生成思考过程
        )
        
        if response.status_code == 200:
            result = response.json()
            full_response = result["response"].strip()  # 保存完整响应（包含思考过程）
            
            # 解析最终判断结果
            if "判断结果：是" in full_response:
                is_same = True
            elif "判断结果：否" in full_response:
                is_same = False
            else:
                logger.error(f"作者 {author_name}：模型未按要求格式返回结果，响应内容：{full_response}")
                return False, full_response
            
            # 详细记录完整思考过程（使用INFO级别确保被记录）
            logger.info(f"\n===== 模型思考过程（作者：{author_name}） =====")
            logger.info(f"单位1: {affiliation1}")
            logger.info(f"单位2: {affiliation2}")
            logger.info(f"思考过程:\n{full_response}")
            logger.info(f"最终判断: {'是' if is_same else '否'}\n=========================================\n")
            
            return is_same, full_response
        else:
            error_msg = f"模型请求失败，状态码: {response.status_code}"
            logger.error(error_msg)
            return False, error_msg
    except Exception as e:
        error_msg = f"调用模型时出错: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

# ---------------------- 文本标准化函数 ----------------------
def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""
    normalized = unicodedata.normalize('NFKD', str(text))
    return normalized.encode('ascii', 'ignore').decode('ascii')

# ---------------------- 时间格式化函数 ----------------------
def format_time(seconds: float) -> str:
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

# ---------------------- 作者分组处理函数 ----------------------
def process_author_group(author_df: pd.DataFrame, model_endpoint: str) -> pd.DataFrame:
    """处理作者分组，完整记录模型思考过程"""
    author_df = author_df.copy()
    author_name = author_df.iloc[0]["Author"] if len(author_df) > 0 else "Unknown"
    n = len(author_df)
    
    if n == 1:
        author_df["AuthorID"] = 0
        logger.info(f"作者 {author_name}：仅1条记录，分配AuthorID=0")
        return author_df
    
    uf = UnionFind(n)
    checked_pairs = set()
    
    # 1. 合并完全相同的单位
    logger.info(f"作者 {author_name}：开始处理完全相同单位的合并（共{len(author_df)}条记录）")
    for i in range(n):
        for j in range(i + 1, n):
            aff_i = author_df.iloc[i]["Affiliation"]
            aff_j = author_df.iloc[j]["Affiliation"]
            if aff_i and aff_j and aff_i.lower() == aff_j.lower():
                uf.union(i, j)
                checked_pairs.add((i, j))
                logger.info(f"作者 {author_name}：记录{i}与记录{j}单位完全相同，直接合并")
    
    # 2. 模型判断非完全相同的单位
    total_needed_pairs = n*(n-1)//2 - len(checked_pairs)
    logger.info(f"作者 {author_name}：需模型判断的记录对数量：{total_needed_pairs}")
    
    for i in range(n):
        for j in range(i + 1, n):
            if (i, j) in checked_pairs:
                continue
            if uf.find(i) == uf.find(j):
                logger.debug(f"作者 {author_name}：记录{i}与记录{j}已同组，跳过模型判断")
                continue
            
            aff_i = author_df.iloc[i]["Affiliation"]
            aff_j = author_df.iloc[j]["Affiliation"]
            loc_i = extract_location(aff_i)
            loc_j = extract_location(aff_j)
            pair_key = f"记录{i}↔记录{j}"
            
            if loc_i and loc_j and loc_i != loc_j:
                logger.warning(f"作者 {author_name}：{pair_key} 跨城市（{loc_i} vs {loc_j}），进入谨慎判断")
            
            # 调用模型并获取思考过程
            is_same, thought_process = are_authors_same(aff_i, aff_j, author_name, model_endpoint)
            
            if is_same:
                uf.union(i, j)
                logger.info(f"作者 {author_name}：{pair_key} 模型判定为同一人，执行合并")
            else:
                logger.info(f"作者 {author_name}：{pair_key} 模型判定为不同人，不合并")
            
            time.sleep(1)
    
    # 3. 分配AuthorID
    group_ids = {}
    next_id = 0
    author_ids = []
    group_details = {}
    
    for i in range(n):
        root = uf.find(i)
        if root not in group_ids:
            group_ids[root] = next_id
            sample_unit = author_df.iloc[i]["Affiliation"]
            if len(sample_unit) > 80:
                sample_unit = sample_unit[:80] + "..."
            group_details[next_id] = {
                "record_indices": [i],
                "sample_unit": sample_unit,
                "location": extract_location(author_df.iloc[i]["Affiliation"])
            }
            next_id += 1
        else:
            group_id = group_ids[root]
            group_details[group_id]["record_indices"].append(i)
    
    for i in range(n):
        root = uf.find(i)
        author_ids.append(group_ids[root])
    author_df["AuthorID"] = author_ids
    
    # 输出最终分组日志
    logger.info(f"作者 {author_name}：最终分组结果（共{next_id}个实际作者）")
    for group_id, details in group_details.items():
        indices = details["record_indices"]
        logger.info(f"  AuthorID={group_id}：包含记录{indices}，单位示例：{details['sample_unit']}")
    
    return author_df

# ---------------------- 主函数 ----------------------
def main():
    start_time = time.time()
    
    OLLAMA_ENDPOINT = "http://localhost:11434"
    input_path = "./output/author_info_ver.20250826.csv"
    output_path = "./output/author_info_with_ids.csv"
    temp_output_path = output_path.replace(".csv", "_temp.csv")
    
    try:
        # 读取CSV文件
        try:
            df = pd.read_csv(input_path, encoding='utf-8')
            logger.info(f"成功读取UTF-8编码文件，原始记录数：{len(df)}")
        except UnicodeDecodeError:
            df = pd.read_csv(input_path, encoding='latin-1')
            logger.info(f"成功读取latin-1编码文件，原始记录数：{len(df)}")
        
        # 过滤无单位信息的记录
        initial_count = len(df)
        df = df[df['Affiliation'].notna() & (df['Affiliation'] != '')].reset_index(drop=True)
        filtered_count = initial_count - len(df)
        logger.info(f"过滤无单位记录：{filtered_count}条，剩余有效记录数：{len(df)}")
        
        # 检查必要列
        required_cols = ["Author", "Affiliation", "PMID", "PMID_Count"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"CSV文件缺少必要列：{', '.join(missing_cols)}，终止程序")
            return
        
        # 文本标准化
        df["Author"] = df["Author"].apply(normalize_text)
        df["Affiliation"] = df["Affiliation"].apply(normalize_text)
        logger.info("完成作者名与单位信息的文本标准化")
        
        # 检查是否存在临时文件，实现断点续传
        processed_authors = set()
        results = []
        
        # 检查固定临时文件是否存在
        if os.path.exists(temp_output_path):
            logger.info(f"发现临时文件: {temp_output_path}")
            try:
                temp_df = pd.read_csv(temp_output_path, encoding='utf-8')
                # 获取已处理的作者列表
                processed_authors = set(temp_df["Author"].unique())
                results.append(temp_df)
                logger.info(f"已处理作者数量: {len(processed_authors)}")
            except Exception as e:
                logger.error(f"读取临时文件失败: {str(e)}")
                processed_authors = set()
        
        # 按作者名分组处理
        unique_authors = df["Author"].unique()
        # 过滤掉已处理的作者
        remaining_authors = [author for author in unique_authors if author not in processed_authors]
        total_authors = len(remaining_authors)
        
        if not remaining_authors:
            logger.info("所有作者已处理完成，无需继续处理")
            # 如果没有剩余作者，直接使用临时文件作为最终结果
            if os.path.exists(temp_output_path):
                os.rename(temp_output_path, output_path)
                logger.info(f"已将临时文件重命名为最终结果: {output_path}")
            return
        
        logger.info(f"开始处理 {total_authors} 个未处理作者名")
        
        for idx, author in enumerate(remaining_authors, 1):
            elapsed_time = time.time() - start_time
            avg_time_per_author = elapsed_time / idx if idx > 0 else 0
            remaining_count = total_authors - idx
            estimated_remaining = avg_time_per_author * remaining_count
            
            logger.info("="*50)
            logger.info(f"进度：{idx}/{total_authors} | 当前处理作者：{author}")
            logger.info(f"已用时间：{format_time(elapsed_time)} | 预计剩余时间：{format_time(estimated_remaining)}")
            logger.info("="*50)
            
            author_subset = df[df["Author"] == author].reset_index(drop=True)
            logger.info(f"作者 {author}：提取到 {len(author_subset)} 条记录")
            processed_subset = process_author_group(author_subset, OLLAMA_ENDPOINT)
            results.append(processed_subset)
            
            # 每处理5个作者保存一次中间结果
            if idx % 5 == 0:
                temp_df = pd.concat(results, ignore_index=True)
                temp_df.to_csv(temp_output_path, index=False, encoding='utf-8')
                logger.info(f"已保存中间结果（处理{idx}个作者），路径：{temp_output_path}")
        
        # 生成最终结果
        final_df = pd.concat(results, ignore_index=True)
        final_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # 输出统计摘要
        total_time = time.time() - start_time
        unique_author_ids = final_df[["Author", "AuthorID"]].drop_duplicates()
        author_id_count = unique_author_ids.groupby("Author").size()
        
        logger.info("\n" + "="*60)
        logger.info("处理完成！最终统计摘要：")
        logger.info(f"- 原始不同作者名数量：{len(unique_authors)}")
        logger.info(f"- 实际区分的作者数量：{len(unique_author_ids)}")
        logger.info(f"- 平均每个作者名对应实际作者数：{author_id_count.mean():.2f}")
        logger.info(f"- 最终输出记录数：{len(final_df)}")
        logger.info(f"- 总耗时：{format_time(total_time)}")
        logger.info(f"- 最终结果路径：{output_path}")
        logger.info(f"- 临时文件路径：{temp_output_path}（保留供参考）")
        logger.info(f"- 模型思考过程已记录在日志文件中")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"程序执行过程中发生致命错误：{str(e)}", exc_info=True)
        
        # 保存当前进度到临时文件
        if 'results' in locals() and results:
            temp_df = pd.concat(results, ignore_index=True)
            temp_df.to_csv(temp_output_path, index=False, encoding='utf-8')
            logger.info(f"已保存错误时的临时文件: {temp_output_path}")
        return

if __name__ == "__main__":
    main()