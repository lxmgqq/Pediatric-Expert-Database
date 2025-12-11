import pandas as pd
import requests
import time
import os
from ast import literal_eval
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 线程安全锁
print_lock = threading.Lock()
save_lock = threading.Lock()

def merge_author_records(df):
    """合并相同Author和AuthorID的记录"""
    # 定义合并函数
    def merge_pmids(pmid_list):
        """合并PMID列表，去重并保持列表格式"""
        merged = []
        for pmids in pmid_list:
            # 尝试将字符串转换为列表，如果已经是列表则直接使用
            try:
                if isinstance(pmids, str):
                    pmid_list = literal_eval(pmids)
                else:
                    pmid_list = pmids
                merged.extend(pmid_list)
            except:
                # 如果解析失败，直接添加原始值
                merged.append(str(pmids))
        
        # 去重并保持顺序
        seen = set()
        result = []
        for pmid in merged:
            if pmid not in seen:
                seen.add(pmid)
                result.append(pmid)
        return result
    
    def get_longest_affiliation(affiliations):
        """选择最长的单位信息"""
        if affiliations.empty:
            return ""
        # 从Series中获取值并找到最长的
        return max(affiliations.values, key=len)
    
    # 按Author和AuthorID分组并合并
    grouped = df.groupby(['Author', 'AuthorID']).agg(
        Affiliation=('Affiliation', get_longest_affiliation),
        PMID=('PMID', merge_pmids),
        PMID_Count=('PMID', lambda x: len(merge_pmids(x)))
    ).reset_index()
    
    return grouped

def extract_affiliation_info(affiliation, model="deepseek-r1:70b", index=None, max_retries=3):
    """调用ollama的大语言模型提取单位信息，增加重试机制和输出过滤"""
    # 只取第一条单位信息（分号前的部分）
    first_affiliation = affiliation.split(';')[0].strip()
    
    # 构建提示词
    prompt = f"""请从以下单位信息中提取主要单位、城市和国家。
单位信息: {first_affiliation}
提取要求:
1. 主要单位：提取最主要的机构或大学名称（通常是层级较高的机构）
2. 城市：提取所在城市
3. 国家：提取所在国家，必须使用标准全称，不得使用缩写或简称。
   - 例如：USA应转换为United States of America
   - 例如：UK应转换为United Kingdom
   - 例如：PRC应转换为People's Republic of China
   - 例如：France保持France（已为全称）

示例:
输入单位信息: Institute of Clinical and Molecular Medicine, Norwegian University of Science and Technology, Trondheim, Norway.
输出结果:
Norwegian University of Science and Technology
Trondheim
Norway

另一示例:
输入单位信息: Department of Pediatrics, Harvard Medical School, Boston, USA
输出结果:
Harvard Medical School
Boston
United States of America

请严格按照以上示例的格式输出，不要添加任何额外内容，每条信息占一行，不要包含```等格式符号。
"""
    
    # 调用ollama API，带重试机制
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1}
                },
                timeout=300
            )
            
            if response.status_code == 200:
                result = response.json()
                output = result.get("response", "").strip()
                
                with print_lock:
                    print(f"\n处理单位信息 (索引 {index})：{first_affiliation}")
                    print(f"模型输出：\n{output}\n{'-'*50}")
                
                # 解析输出结果 - 过滤掉包含```的行和空行
                lines = []
                for line in output.split('\n'):
                    stripped_line = line.strip()
                    if stripped_line and '```' not in stripped_line:
                        lines.append(stripped_line)
                
                # 确保我们有足够的有效行
                line_count = len(lines)
                organization = lines[-3].strip() if line_count >= 3 else "未知"
                city = lines[-2].strip() if line_count >= 2 else "未知"
                country = lines[-1].strip() if line_count >= 1 else "未知"
                
                return (index, organization, city, country)
            else:
                with print_lock:
                    print(f"API调用失败 (索引 {index})，状态码：{response.status_code}，重试次数：{retry_count+1}")
                retry_count += 1
                time.sleep(2)  # 重试前短暂等待
                
        except Exception as e:
            with print_lock:
                print(f"调用模型时发生错误 (索引 {index})：{str(e)}，重试次数：{retry_count+1}")
            retry_count += 1
            time.sleep(2)  # 重试前短暂等待
    
    # 多次重试失败后返回未知
    return (index, "未知", "未知", "未知")

def save_cache(df, cache_file):
    """保存缓存文件，覆盖之前的版本"""
    try:
        # 先保存到临时文件，成功后再替换目标文件，避免文件损坏
        temp_file = f"{cache_file}.tmp"
        df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        os.replace(temp_file, cache_file)  # 原子操作替换文件
        
        with print_lock:
            processed_count = sum(df['MainAffiliation'] != "")  # 统计已处理的记录数
            print(f"\n已更新缓存文件（已处理 {processed_count} 条记录）：{cache_file}")
    except Exception as e:
        with print_lock:
            print(f"保存缓存文件失败：{str(e)}")

def main():
    # 配置文件路径
    input_file = "output/author_info_with_ids.csv"
    output_file = "output/author_info_processed.csv"
    updated_output_file = "output/author_info_processed_updated.csv"  # 更新后的输出文件
    cache_file = "output/author_info_cache.csv"  # 固定的缓存文件名
    
    # 确保output目录存在
    os.makedirs("output", exist_ok=True)
    
    # 检查是否存在已处理的结果文件
    if os.path.exists(output_file):
        print(f"发现已存在的结果文件：{output_file}")
        try:
            # 读取已存在的结果文件
            merged_df = pd.read_csv(output_file, encoding='utf-8-sig')
            print(f"成功加载已有结果，共 {len(merged_df)} 条记录")
            
            # 检查是否包含必要的列
            required_columns = ['MainAffiliation', 'City', 'Country']
            if not all(col in merged_df.columns for col in required_columns):
                print("已有结果文件格式不正确，将重新处理所有记录")
                # 重新读取原始文件并合并
                df = pd.read_csv(input_file, encoding='utf-8-sig')
                merged_df = merge_author_records(df)
                # 初始化新列
                merged_df['MainAffiliation'] = ""
                merged_df['City'] = ""
                merged_df['Country'] = ""
            else:
                # 筛选需要重新处理的记录：Country为空或为```
                unprocessed_mask = (merged_df['Country'].isna()) | (merged_df['Country'] == '```') | (merged_df['Country'] == "")
                unprocessed_indices = merged_df[unprocessed_mask].index.tolist()
                print(f"发现 {len(unprocessed_indices)} 条需要重新处理的记录（Country为空或包含特殊字符）")
        except Exception as e:
            print(f"读取已有结果文件失败，将重新处理：{str(e)}")
            # 重新读取原始文件并合并
            df = pd.read_csv(input_file, encoding='utf-8-sig')
            merged_df = merge_author_records(df)
            # 初始化新列
            merged_df['MainAffiliation'] = ""
            merged_df['City'] = ""
            merged_df['Country'] = ""
    else:
        # 不存在结果文件，从头开始处理
        print(f"未发现已存在的结果文件，将从头开始处理")
        # 读取CSV文件
        try:
            df = pd.read_csv(input_file, encoding='utf-8')
            print(f"成功读取文件：{input_file}，共 {len(df)} 条记录")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(input_file, encoding='gbk')
                print(f"使用gbk编码读取文件：{input_file}，共 {len(df)} 条记录")
            except Exception as e:
                print(f"读取文件失败：{str(e)}")
                return
        except Exception as e:
            print(f"读取文件失败：{str(e)}")
            return
        
        # 合并相同作者ID的记录
        merged_df = merge_author_records(df)
        # 初始化新列（英文列名）
        merged_df['MainAffiliation'] = ""
        merged_df['City'] = ""
        merged_df['Country'] = ""
        print(f"合并后共有 {len(merged_df)} 条记录")
    
    # 检查是否存在缓存文件，若存在则加载已处理结果
    if os.path.exists(cache_file):
        try:
            cache_df = pd.read_csv(cache_file, encoding='utf-8-sig')
            # 仅恢复已处理的字段
            for idx in range(len(merged_df)):
                if idx < len(cache_df) and cache_df.at[idx, 'MainAffiliation'] != "":
                    merged_df.at[idx, 'MainAffiliation'] = cache_df.at[idx, 'MainAffiliation']
                    merged_df.at[idx, 'City'] = cache_df.at[idx, 'City']
                    merged_df.at[idx, 'Country'] = cache_df.at[idx, 'Country']
            processed_count = sum(merged_df['MainAffiliation'] != "")
            print(f"已加载缓存文件，恢复 {processed_count} 条已处理记录")
        except Exception as e:
            print(f"加载缓存文件失败，将继续处理：{str(e)}")
    
    # 筛选未处理的记录
    unprocessed_indices = [idx for idx, row in merged_df.iterrows() 
                          if row['MainAffiliation'] == "" or 
                          row['Country'] in ("", "```") or 
                          pd.isna(row['Country'])]
    
    if not unprocessed_indices:
        print("所有记录已处理完毕，无需继续")
        return
    
    print(f"\n开始处理剩余 {len(unprocessed_indices)} 条记录...")
    
    # 记录开始时间
    start_time = time.time()
    save_interval = 5  # 每处理5条记录更新一次缓存
    processed_count = sum(merged_df['MainAffiliation'] != "")  # 初始已处理数量
    
    # 调整线程数
    max_workers = 4
    
    # 准备未处理的任务
    tasks = [(merged_df.at[idx, 'Affiliation'], idx) for idx in unprocessed_indices]
    
    # 并行处理
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extract_affiliation_info, aff, "deepseek-r1:70b", idx) 
                  for aff, idx in tasks]
        
        # 处理结果并更新缓存
        for future in tqdm(as_completed(futures), total=len(unprocessed_indices), desc="处理进度"):
            with save_lock:
                idx, org, city, country = future.result()
                merged_df.at[idx, 'MainAffiliation'] = org
                merged_df.at[idx, 'City'] = city
                merged_df.at[idx, 'Country'] = country
                
                processed_count += 1
                
                # 每处理指定数量的记录，更新一次缓存
                if processed_count % save_interval == 0:
                    save_cache(merged_df, cache_file)
    
    # 处理完成后，保存最终结果
    total_time = time.time() - start_time
    total_records = len(merged_df)
    print(f"\n信息提取完成，总耗时：{total_time:.2f}秒，平均每条记录耗时：{total_time/total_records:.2f}秒")
    
    try:
        # 根据是否存在原始输出文件，决定保存的文件名
        final_output = updated_output_file if os.path.exists(output_file) else output_file
        merged_df.to_csv(final_output, index=False, encoding='utf-8-sig')
        print(f"最终结果已保存至：{final_output}")
        
        # 处理完成后删除缓存文件
        if os.path.exists(cache_file):
            os.remove(cache_file)
            print("已删除缓存文件")
    except Exception as e:
        print(f"保存最终结果失败：{str(e)}")

if __name__ == "__main__":
    main()
