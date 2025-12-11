import pandas as pd
import requests
import json
import os
import chardet  # 用于检测文件编码
from tqdm import tqdm

def detect_file_encoding(file_path):
    """检测文件编码格式"""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # 读取前10KB数据用于检测
    result = chardet.detect(raw_data)
    return result['encoding']

def extract_keywords(abstract):
    """调用ollama部署的模型提取关键词，修复编码问题"""
    # 确保摘要文本是字符串格式，避免编码问题
    if not isinstance(abstract, str):
        abstract = str(abstract)
    
    # 构建提示词，明确要求英文关键词
    prompt = f"""请从以下英文论文摘要中提取5个最能代表论文核心内容的英文关键词。
摘要: {abstract}
提取要求:
1. 关键词必须为英文，准确反映论文的核心主题和研究内容
2. 避免使用过于宽泛或通用的词汇
3. 每个关键词单独占一行，共输出5个关键词，每行的关键词前不需要数字序号
4. 不要使用任何中文词汇或特殊符号

请严格按照上述要求输出，不要添加任何额外内容，不要包含```等格式符号。

示例:
摘要: Perianal abscess (PA) and fistula-in-ano (FIA) are common entities in infancy. Although several hypotheses have been suggested, 
the pathogenesis of PA/FIA remains elusive. The natural course of these diseases in infancy is self-limiting in the majority of 
cases whereas older children show similarities to PA/FIA in adults. It is important to rule out rare differential diagnoses of 
PA/FIA such as inflammatory bowel disease (IBD), surgical complications after colorectal surgery, and immunodeficiencies. 
Treatment remains empiric, comprises conservative, as well as surgical approaches, and is dependent on the age of the patient. 
This review summarizes anatomical aspects, current evidence on disease pathogenesis, clinical presentation, and management of 
pediatric patients with PA and FIA.
输出结果:
Perianal abscess
Fistula-in-ano
Pathogenesis
Pediatric Patients
Diagnosis and Treatment
"""
    
    # 调用ollama的API
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "deepseek-r1:70b",
                "prompt": prompt,
                "stream": False
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=180  # 设置超时时间
        )
        
        if response.status_code == 200:
            try:
                result_text = response.text
            except UnicodeDecodeError:
                result_text = response.content.decode('latin-1')
                
            result = json.loads(result_text)
            output = result["response"].strip()
            
            lines = []
            for line in output.split('\n'):
                stripped_line = line.strip()
                if stripped_line and '```' not in stripped_line:
                    lines.append(stripped_line)
            
            if lines and lines[-1] == '```':
                keywords = lines[-6:-1]
            else:
                keywords = lines[-5:]
            
            while len(keywords) < 5:
                keywords.append("")
                
            return keywords[:5]
        else:
            print(f"API请求失败，状态码: {response.status_code}")
            return ["", "", "", "", ""]
    except Exception as e:
        print(f"调用模型时发生错误: {str(e)}")
        return ["", "", "", "", ""]

def is_abstract_valid(abstract):
    """检查摘要是否有效（非空）"""
    if pd.isna(abstract):
        return False
    if not isinstance(abstract, str):
        abstract = str(abstract)
    return abstract.strip() != ""

def is_keywords_empty(keywords):
    """检查关键词是否为空"""
    if pd.isna(keywords):
        return True
    if not isinstance(keywords, str):
        keywords = str(keywords)
    return keywords.strip() == "" or keywords.strip() == "[]"

def process_pubmed_data(input_file, output_file):
    """处理PubMed数据，提取关键词并保存，支持断点续处理"""
    # 检查输出文件是否存在
    if os.path.exists(output_file):
        print(f"发现已存在的输出文件 {output_file}，将继续处理未完成的记录")
        try:
            # 读取已处理的文件继续处理
            df = pd.read_csv(output_file, encoding='utf-8-sig')
            original_count = len(df)
            print(f"从现有文件加载了 {original_count} 条记录")
        except Exception as e:
            print(f"读取已有输出文件失败: {str(e)}，将尝试从原始文件重新开始")
            df = None
    else:
        print(f"未发现输出文件 {output_file}，将从头开始处理")
        df = None
    
    # 如果无法从输出文件加载数据，则从原始输入文件加载
    if df is None:
        try:
            file_encoding = detect_file_encoding(input_file)
            print(f"检测到文件编码: {file_encoding}")
        except Exception as e:
            print(f"检测文件编码失败: {str(e)}，将使用默认编码utf-8")
            file_encoding = 'utf-8'
        
        try:
            df = pd.read_csv(input_file, encoding=file_encoding)
        except UnicodeDecodeError:
            print(f"使用{file_encoding}编码读取失败，尝试使用utf-8-sig编码")
            df = pd.read_csv(input_file, encoding='utf-8-sig')
        except Exception as e:
            print(f"读取CSV文件失败: {str(e)}")
            return
        print(f"从原始文件加载了 {len(df)} 条记录")
    
    # 检查是否有Keywords列，如果没有则添加
    if 'Keywords' not in df.columns:
        df['Keywords'] = ""
    
    # 统计各类记录数量
    total = len(df)
    processed = 0  # 有摘要且有关键词
    no_abstract = 0  # 无摘要
    to_process = 0  # 有摘要但无关键词
    
    for _, row in df.iterrows():
        has_abstract = is_abstract_valid(row['Abstract'])
        has_keywords = not is_keywords_empty(row['Keywords'])
        
        if has_abstract and has_keywords:
            processed += 1
        elif not has_abstract:
            no_abstract += 1
        else:
            to_process += 1
    
    print(f"记录状态统计:")
    print(f"  总计: {total} 条")
    print(f"  已处理 (有摘要且有关键词): {processed} 条")
    print(f"  无需处理 (无摘要): {no_abstract} 条")
    print(f"  待处理 (有摘要但无关键词): {to_process} 条")
    
    # 如果没有待处理的记录，直接返回
    if to_process == 0:
        print("所有可处理的记录都已完成，无需继续处理")
        return
    
    # 处理每条记录
    processed_count = 0
    for index, row in tqdm(df.iterrows(), total=len(df), desc="处理进度"):
        # 检查摘要是否有效
        has_abstract = is_abstract_valid(row['Abstract'])
        # 检查关键词是否为空
        has_no_keywords = is_keywords_empty(row['Keywords'])
        
        # 根据规则决定是否处理
        if has_abstract and has_no_keywords:
            # 有摘要但无关键词，需要处理
            keywords = extract_keywords(row['Abstract'])
            df.at[index, 'Keywords'] = str(keywords)
            processed_count += 1
            
            # 每处理10条记录保存一次，防止意外丢失
            if processed_count % 10 == 0:
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"\n已处理 {processed_count} 条记录，已保存到文件")
    
    # 最后保存一次
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n处理完成，共处理了 {processed_count} 条记录，结果已保存到{output_file}")
    print(f"当前状态: 已处理 {processed + processed_count} 条，待处理 {to_process - processed_count} 条")

if __name__ == "__main__":
    # 定义文件路径
    input_csv = os.path.join("output", "pubmed_results_ver.20250826.csv")
    output_csv = os.path.join("output", "pubmed_results_with_keywords.csv")
    
    # 确保输出目录存在
    os.makedirs("output", exist_ok=True)
    
    # 处理数据
    process_pubmed_data(input_csv, output_csv)
    