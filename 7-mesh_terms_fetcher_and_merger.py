import pandas as pd
import os
import time
from Bio import Entrez
from tqdm import tqdm

def fetch_mesh_in_batches(pmid_list):
    """
    使用 Biopython (E-utilities) 批量获取 MeSH 词条
    """
    
    # !!! 关键：NCBI 要求提供一个邮箱地址
    Entrez.email = "your.email@example.com"  # 请替换为您自己的邮箱
    
    pmid_to_mesh = {} # 用于存储 {PMID: [MeSH list]}
    
    try:
        # 1. 将PMID列表（纯文本）发送给服务器
        # epost 会在服务器上缓存这个列表，并返回一个查询密钥
        print(f"正在向 Entrez epost {len(pmid_list)} 个 PMIDs...")
        post_handle = Entrez.epost(db="pubmed", id=",".join(map(str, pmid_list)))
        post_results = Entrez.read(post_handle)
        post_handle.close()
        
        webenv = post_results["WebEnv"]
        query_key = post_results["QueryKey"]

        # 2. 使用缓存的密钥 (webenv 和 query_key) 批量获取 (efetch) 数据
        # rettype='medline', retmode='xml' 可以获取包含MeSH的XML数据
        print("正在从 Entrez efetch 批量获取数据...")
        fetch_handle = Entrez.efetch(
            db="pubmed",
            webenv=webenv,
            query_key=query_key,
            rettype="medline",
            retmode="xml"
        )
        
        # 3. 解析XML数据
        # Entrez.read 会智能地处理整个XML流
        print("正在解析XML数据...")
        records = Entrez.read(fetch_handle)
        fetch_handle.close()

        # 4. 提取 MeSH 词条
        # 遍历返回的每篇论文
        if 'PubmedArticle' in records:
            for article in records['PubmedArticle']:
                try:
                    pmid = article['MedlineCitation']['PMID']
                    mesh_list = []
                    
                    # 检查 'MeshHeadingList' 是否存在
                    if 'MeshHeadingList' in article['MedlineCitation']:
                        for mesh_heading in article['MedlineCitation']['MeshHeadingList']:
                            # MeSH 词条在 'DescriptorName' 字段中
                            # 我们也可以提取 'QualifierName' (子主题词)，但这里只取主词
                            # 示例：'Antigen-Presenting Cells'
                            mesh_term = mesh_heading['DescriptorName'].strip()
                            
                            # 有些词条后面会带星号（*）表示主要主题
                            if mesh_heading.attributes.get('MajorTopicYN', 'N') == 'Y':
                                mesh_term += "*"
                                
                            mesh_list.append(mesh_term)
                    
                    pmid_to_mesh[str(pmid)] = mesh_list
                
                except KeyError as e:
                    # 如果某篇论文缺少 PMCID 或 MeSH 字段
                    print(f"\n[Warning] 解析某篇论文时出错 (PMID: {pmid if 'pmid' in locals() else 'Unknown'}): {e}")
                    if 'pmid' in locals():
                        pmid_to_mesh[str(pmid)] = [] # 至少给它一个空列表
                except Exception as e:
                    print(f"\n[Error] 意外错误: {e}")

        return pmid_to_mesh

    except Exception as e:
        print(f"\n[Fatal Error] E-utilities 请求失败: {e}")
        # 如果API失败，返回一个空字典，以便脚本可以安全退出
        return {}

def main():
    # --- 1. 定义文件路径 ---
    file_path = os.path.join('output', 'pubmed_results_with_keywords.csv')
    
    if not os.path.exists(file_path):
        print(f"错误：文件未找到于: {file_path}")
        return

    print(f"正在读取文件: {file_path}")
    df = pd.read_csv(file_path)
    
    # 确保PMID是字符串类型，以便后续匹配
    df['PMID'] = df['PMID'].astype(str)
    
    print(f"总共找到 {len(df)} 篇论文。")

    # --- 2. 批量获取所有 MeSH ---
    # 将所有PMID转换为列表
    all_pmids = df['PMID'].tolist()
    
    # *** 注意 ***
    # 虽然 E-utilities 很强，但一次性提交 6093 个也可能超时
    # 我们将其拆分为更小的批次 (例如每批 500 个)
    
    BATCH_SIZE = 500 
    final_mesh_map = {} # 存储所有结果
    
    # 使用tqdm显示批次处理进度
    for i in tqdm(range(0, len(all_pmids), BATCH_SIZE), desc="Processing Batches"):
        batch_pmids = all_pmids[i : i + BATCH_SIZE]
        tqdm.write(f"\n--- 正在处理批次 {i//BATCH_SIZE + 1}/{len(all_pmids)//BATCH_SIZE + 1} (大小: {len(batch_pmids)}) ---")
        
        # 调用API获取这批PMID的MeSH
        batch_results = fetch_mesh_in_batches(batch_pmids)
        final_mesh_map.update(batch_results)
        
        tqdm.write(f"--- 批次处理完成 ---")
        
        # E-utilities 允许每秒最多10个请求（如果有API Key）
        # 我们在每批之间暂停1秒，保持友好
        time.sleep(1)

    print("\n所有批次处理完毕。")

    # --- 3. 将结果映射回 DataFrame ---
    # df['PMID'] 已经是字符串了
    # 我们使用 .map() 函数，根据 final_mesh_map 字典来填充新列
    print("正在将 MeSH 词条映射回 DataFrame...")
    df['MeSH_API'] = df['PMID'].map(final_mesh_map)
    
    # 替换那些可能在API中没有返回（或失败）的条目为 '[]'
    df['MeSH_API'] = df['MeSH_API'].apply(lambda x: x if isinstance(x, list) else [])

    # --- 4. 保存回原文件 ---
    try:
        # 我们将新列命名为 'MeSH_API' 以区别于旧的HTML抓取尝试
        df.to_csv(file_path, index=False)
        print(f"\n成功！已将 MeSH 词条 (MeSH_API) 添加并保存回: {file_path}")
        
        # 打印前5行以供预览
        print("\n更新后的文件预览 (前5行):")
        print(df.head())
        
    except Exception as e:
        print(f"\n错误：保存文件 {file_path} 失败: {e}")

if __name__ == "__main__":
    main()