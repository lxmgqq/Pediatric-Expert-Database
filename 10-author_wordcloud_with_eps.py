import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
import os

# --------------------------
# 1. 目录配置：读取自output，保存到svg和eps
# --------------------------
# 数据读取目录（不变，仍为output）
input_dir = "./output"

# 词云保存目录（自动创建）
svg_save_dir = "./svg"
eps_save_dir = "./eps"  # 新增 eps 目录

os.makedirs(svg_save_dir, exist_ok=True)  # 确保svg目录存在
os.makedirs(eps_save_dir, exist_ok=True)  # 确保eps目录存在

# --------------------------
# 2. 数据读取与处理（逻辑不变）
# --------------------------
# 读取作者数据
input_path = os.path.join(input_dir, "author_info_processed_updated.csv")
try:
    df = pd.read_csv(input_path)
except FileNotFoundError:
    print(f"错误：未找到数据文件，路径：{os.path.abspath(input_path)}")
    print("请确保author_info_processed_updated.csv在output文件夹中")
    exit()

# 处理论文数量数据（排除无效值）
df['PMID_Count'] = pd.to_numeric(df['PMID_Count'], errors='coerce')
df = df.dropna(subset=['PMID_Count'])

# 按作者分组计算总论文数（作为词云权重）
author_papers = df.groupby('Author')['PMID_Count'].sum().reset_index()
author_weights = dict(zip(author_papers['Author'], author_papers['PMID_Count']))

# --------------------------
# 3. 词云配置（保留圆形掩码和视觉效果）
# --------------------------
# 词云尺寸设置（适配SVG/EPS矢量格式）
width, height = 4000, 3000

# 创建圆形掩码（词云显示为圆形）
x, y = np.ogrid[:height, :width]
center_x, center_y = width // 2, height // 2
radius = min(center_x, center_y) * 0.8  # 圆形半径为画布的80%
center_mask = (x - center_y) ** 2 + (y - center_x) ** 2 <= radius ** 2
mask = np.ones((height, width), dtype=bool)
mask[center_mask] = False  # 中心区域为可显示区域

# 配置词云参数
wordcloud = WordCloud(
    width=width,
    height=height,
    background_color='white',
    max_words=500,
    mask=~mask,  # 应用圆形掩码
    contour_width=1,
    contour_color='steelblue',  # 圆形边框颜色
    prefer_horizontal=0.9,  # 90%的词水平显示
    collocations=False,  # 不显示重复词组
    stopwords=STOPWORDS,  # 排除通用停用词
    relative_scaling=0.6,  # 词频与字体大小的关联度
    min_font_size=8,
    max_font_size=200,
    font_step=1,
    regexp=r"\w[\w\s]*"  # 支持带空格的作者名
)

# 生成词云
wordcloud.generate_from_frequencies(author_weights)

# --------------------------
# 4. 绘制并保存图像（SVG 和 EPS）
# --------------------------
# 创建画布
fig, ax = plt.subplots(1, 1, figsize=(width/100, height/100))

# 显示词云
ax.imshow(wordcloud, interpolation='bilinear')
ax.axis('off')  # 隐藏坐标轴

# 添加标题
fig.suptitle(
    'Word Cloud of Publications by Global Pediatric Surgery Experts(Top 500)',
    fontsize=30,
    y=0.98,
    fontweight='bold'
)

# --- 保存 SVG ---
svg_output_path = os.path.join(svg_save_dir, "author_publication_wordcloud.svg")
plt.savefig(
    svg_output_path,
    format='svg',
    bbox_inches='tight',
    facecolor='white',
    edgecolor='none'
)
print(f"✅ 词云SVG矢量图已保存至：")
print(f"   {os.path.abspath(svg_output_path)}")

# --- [新增] 保存 EPS ---
eps_output_path = os.path.join(eps_save_dir, "author_publication_wordcloud.eps")
plt.savefig(
    eps_output_path,
    format='eps',
    bbox_inches='tight',
    facecolor='white',
    edgecolor='none'
)
print(f"✅ 词云EPS矢量图已保存至：")
print(f"   {os.path.abspath(eps_output_path)}")

# 可选：显示预览
plt.show()