import pandas as pd
import ast
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import os
import re

def normalize_keywords(keywords_list):
    """
    关键词清洗函数：
    1. 去除括号及其内容
    2. 转换为标题格式 (Title Case)
    """
    normalized_list = []
    
    for keyword in keywords_list:
        if not isinstance(keyword, str) or not keyword.strip():
            continue
            
        # 1. 去除括号及括号内的内容
        clean_keyword = re.sub(r'\s*\(.*?\)', '', keyword).strip()
        
        if not clean_keyword:
            continue
            
        # 2. 首字母大写 (Title Case)
        final_keyword = clean_keyword.title()
        
        normalized_list.append(final_keyword)
    
    return normalized_list

def create_keyword_wordcloud_spaced():
    # 1. 自动创建保存目录
    output_dirs = ["svg", "eps", "output"]
    for directory in output_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ 已创建目录: ./{directory}")
            
    # 2. 读取CSV文件
    csv_path = 'output/pubmed_results_with_keywords_processed.csv'
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ 成功读取 {len(df)} 条论文数据")
    except FileNotFoundError:
        print(f"❌ 错误：找不到 {csv_path} 文件")
        return
    except Exception as e:
        print(f"❌ 读取文件时出错: {e}")
        return
    
    # 3. 提取关键词
    all_raw_keywords = []
    for index, row in df.iterrows():
        keywords_str = row.get('Keywords_and_MeSH_terms')
        if isinstance(keywords_str, str):
            try:
                keywords_list = ast.literal_eval(keywords_str)
                if isinstance(keywords_list, list):
                    valid_keywords = [kw for kw in keywords_list 
                                    if isinstance(kw, str) and kw.strip()]
                    all_raw_keywords.extend(valid_keywords)
            except (ValueError, SyntaxError):
                continue
    
    if not all_raw_keywords:
        print("❌ 错误：没有找到有效关键词")
        return
    
    # 4. 归一化处理
    normalized_keywords = normalize_keywords(all_raw_keywords)
    keyword_freq = Counter(normalized_keywords)
    
    print(f"统计完毕：共 {len(normalized_keywords)} 个关键词，{len(keyword_freq)} 个唯一词汇")
    
    # 5. 创建优化间距的高清词云
    wordcloud = WordCloud(
        width=1600,  # 稍微加大画布宽度
        height=1000, # 稍微加大画布高度
        background_color='white',
        
        max_words=300,
        
        colormap='viridis',
        
        # [关键修改] 调整相对缩放，0.5 -> 0.4，让词大小过渡更平滑
        relative_scaling=0.4,
        
        # [关键修改] 增加文字间距 (默认为2 -> 改为10)
        margin=10,
        
        random_state=42,
        collocations=False,
        scale=4  # 保持高清
    ).generate_from_frequencies(keyword_freq)
    
    # 6. 绘制并保存
    plt.figure(figsize=(16, 10))
    
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title('Word Cloud of PubMed Paper Keywords (TOP 300)', fontsize=20, pad=30)
    
    # 保存 SVG
    svg_path = 'svg/keyword_wordcloud.svg'
    plt.savefig(svg_path, format='svg', bbox_inches='tight', dpi=300)
    print(f"\n✅ 优化间距后的高清SVG已保存至: {svg_path}")

    # 保存 EPS
    eps_path = 'eps/keyword_wordcloud.eps'
    plt.savefig(eps_path, format='eps', bbox_inches='tight', dpi=300)
    print(f"✅ 优化间距后的高清EPS已保存至: {eps_path}")

    plt.close()

if __name__ == "__main__":
    create_keyword_wordcloud_spaced()