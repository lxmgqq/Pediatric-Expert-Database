import csv
import re
import os
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

# ----------------------
# 全局配置
# ----------------------
# 1. 设置字体 [修改点：已改回 Arial]
plt.rcParams['font.sans-serif'] = ['Arial', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

# 2. 目录准备
if not os.path.exists('svg'):
    os.makedirs('svg')
if not os.path.exists('eps'):
    os.makedirs('eps')

# 期刊影响因子
journal_impact_factors = {
    "N Engl J Med": 78.5, 
    "Lancet": 88.5, 
    "JAMA": 55, 
    "Br Med J": 42.7,
    "Nature": 48.5, 
    "Science": 45.8, 
    "Cell": 42.5, 
    "J Pediatr Surg": 2.5,
    "Pediatr Surg Int": 1.6, 
    "Eur J Pediatr Surg": 1.4, 
    "World J Pediatr Surg": 1.3,
    "Ann Pediatr Surg": 0.3, 
    "J Pediatr Surg Open": 0.333, 
    "J Pediatr Surg Case Rep": 0.2,
    "JAMA Pediatr": 18, 
    "Lancet Child Adolesc Health": 15.5, 
    "Pediatr Res": 3.1,
    "Arch Dis Child Educ Pract Ed": 3.2, 
    "J Pediatr": 3.5, 
    "Pediatrics": 6.4,
    "World J Pediatr": 4.5, 
    "Ann Surg": 6.4, 
    "Br J Surg": 8.8,
    "Surgery": 2.7, 
    "World J Surg": 2.5
}

# ----------------------
# 1. 读取pubmed论文数据
# ----------------------
pubmed_file = 'output/pubmed_results_with_keywords_processed.csv'
pmid_col = 'PMID'
journal_col = 'Journal'

pmid_to_journal = {} 
journal_paper_count = {} 

try:
    with open(pubmed_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pmid = str(row[pmid_col].strip())
            journal_abbr = row[journal_col].strip()
            
            if journal_abbr in journal_impact_factors:
                pmid_to_journal[pmid] = journal_abbr
                journal_paper_count[journal_abbr] = journal_paper_count.get(journal_abbr, 0) + 1
except FileNotFoundError:
    print(f"❌ 未找到文件: {pubmed_file}")
    exit()

# ----------------------
# 2. 处理作者数据，计算期刊合作关系
# ----------------------
author_file = 'output/author_info_processed_updated.csv'
author_pmid_col = 'PMID'

journal_cooperation = {}

try:
    with open(author_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pmid_count = int(row['PMID_Count'])
                if pmid_count < 2:
                    continue
                pmid_str = row.get(author_pmid_col, '').strip()
                pmids = re.findall(r'\d+', pmid_str)
                pmids = [str(pmid) for pmid in pmids]
                
                journals = {pmid_to_journal[pmid] for pmid in pmids if pmid in pmid_to_journal}
                
                if len(journals) >= 2:
                    journal_list = sorted(list(journals))
                    for i in range(len(journal_list)):
                        for j in range(i + 1, len(journal_list)):
                            j1, j2 = journal_list[i], journal_list[j]
                            journal_cooperation[(j1, j2)] = journal_cooperation.get((j1, j2), 0) + 1
            except (KeyError, ValueError):
                continue
except FileNotFoundError:
    print(f"❌ 未找到文件: {author_file}")
    exit()

# ----------------------
# 3. 绘制网络图
# ----------------------
G = nx.Graph()

# 添加节点
for journal in journal_paper_count:
    size = journal_paper_count[journal] * 15  
    G.add_node(journal, size=size)

# 添加边
for (j1, j2), weight in journal_cooperation.items():
    G.add_edge(j1, j2, weight=weight)

# 移除孤立节点
isolates = list(nx.isolates(G))
if isolates:
    print(f"ℹ️ 已移除以下无连线的孤立期刊: {isolates}")
    G.remove_nodes_from(isolates)

# 可视化参数准备
all_ifs = list(journal_impact_factors.values())
min_if, max_if = min(all_ifs), max(all_ifs)
colors = [(0.7, 0.8, 1), (0, 0, 0.8)]
cmap = LinearSegmentedColormap.from_list('if_cmap', colors, N=100)

# 获取节点颜色
node_colors = []
for node in G.nodes:
    if_val = journal_impact_factors.get(node, min_if) 
    norm_val = (if_val - min_if) / (max_if - min_if) if max_if > min_if else 0
    node_colors.append(cmap(norm_val))

node_sizes = [G.nodes[node]['size'] for node in G.nodes]

if G.edges():
    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    edge_widths = [2 * np.log(w) + 1 for w in edge_weights]  
else:
    edge_widths = []

# ----------------------------------------------------
# 布局与坐标微调
# ----------------------------------------------------
print("⏳ 正在计算布局...")
# 1. 使用 kamada_kawai_layout (比较舒展)
pos = nx.kamada_kawai_layout(G)

# 2. 手动调整特定节点位置
target_node = 'J Pediatr Surg'
if target_node in pos:
    x, y = pos[target_node]
    # 将坐标数值乘以 0.5 (即向中心点 0,0 移动 50%)
    pos[target_node] = np.array([x * 0.25, y * 0.5])
    print(f"🔧 已手动拉近节点: {target_node}")

# 创建大尺寸画布
fig, ax = plt.subplots(figsize=(32, 27))  

# 绘制节点
nx.draw_networkx_nodes(
    G, pos, 
    node_size=node_sizes, 
    node_color=node_colors, 
    alpha=0.8, 
    ax=ax
)

# 绘制边
if G.edges():
    nx.draw_networkx_edges(
        G, pos, 
        width=edge_widths, 
        alpha=0.7, 
        edge_color='#555555', 
        ax=ax
    )

# 绘制标签
nx.draw_networkx_labels(
    G, pos, 
    font_size=18,  # 保持小字号
    font_weight='bold', 
    ax=ax
)

# 增加画布留白
ax.margins(0.1) 

# 颜色条
sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=min_if, vmax=max_if))
sm.set_array([])
cbar = fig.colorbar(sm, ax=ax)
cbar.set_label('Impact Factor', rotation=270, labelpad=40, fontsize=32)
cbar.ax.tick_params(labelsize=24)

# 标题
ax.set_title(
    'Journal Collaboration Network',
    fontsize=42,
    pad=40 
)

ax.axis('off')

# ----------------------
# 4. 保存图片
# ----------------------

# 保存 SVG
svg_path = 'svg/journal_collaboration_network_large_font.svg'
plt.savefig(svg_path, format='svg', bbox_inches='tight', dpi=300)
print(f"✅ 网络图(SVG) 已保存至 {svg_path}")

# 保存 EPS
eps_path = 'eps/journal_collaboration_network_large_font.eps'
plt.savefig(eps_path, format='eps', bbox_inches='tight', dpi=300)
print(f"✅ 网络图(EPS) 已保存至 {eps_path}")

plt.close()