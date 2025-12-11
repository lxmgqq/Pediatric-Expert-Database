import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

def plot_journals_with_pie_and_bar_solve_overlap():
    # 基础配置
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["font.size"] = 10
    # 设置 Linux 支持的中文字体
    plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'Microsoft YaHei', 'SimHei', 'Arial', 'sans-serif']

    # --------------------------
    # 0. 期刊名称映射表 (缩写 -> 全称)
    # --------------------------
    journal_mapping = {
        "N Engl J Med": "The New England Journal of Medicine",
        "Lancet": "The Lancet",
        "JAMA": "Journal of the American Medical Association",
        "Br Med J": "British Medical Journal",
        "Nature": "Nature",
        "Science": "Science",
        "Cell": "Cell",
        "J Pediatr Surg": "Journal of Pediatric Surgery",
        "Pediatr Surg Int": "Pediatric Surgery International",
        "Eur J Pediatr Surg": "European Journal of Pediatric Surgery",
        "World J Pediatr Surg": "World Journal of Pediatric Surgery",
        "Ann Pediatr Surg": "Annals of Pediatric Surgery",
        "J Pediatr Surg Open": "Journal of Pediatric Surgery Open",
        "J Pediatr Surg Case Rep": "Journal of Pediatric Surgery Case Reports",
        "JAMA Pediatr": "JAMA Pediatrics",
        "Lancet Child Adolesc Health": "The Lancet Child & Adolescent Health",
        "Pediatr Res": "Pediatric Research",
        "Arch Dis Child Educ Pract Ed": "Archives of Disease in Childhood",
        "J Pediatr": "The Journal of Pediatrics",
        "Pediatrics": "Pediatrics",
        "World J Pediatr": "World Journal of Pediatrics",
        "Ann Surg": "Annals of Surgery",
        "Br J Surg": "British Journal of Surgery",
        "Surgery": "Surgery",
        "World J Surg": "World Journal of Surgery"
    }

    # --------------------------
    # 1. 自动创建保存目录
    # --------------------------
    output_dirs = ["svg", "eps", "output"]
    for directory in output_dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ 已创建目录: ./{directory}")

    # CSV 文件路径
    csv_file_path = "./output/pubmed_results_with_keywords.csv"

    try:
        # --------------------------
        # 2. 读取数据
        # --------------------------
        try:
            df = pd.read_csv(csv_file_path, encoding='utf-8')
        except UnicodeDecodeError:
            print("⚠️ UTF-8 读取失败，尝试 GBK 编码...")
            try:
                df = pd.read_csv(csv_file_path, encoding='gbk')
            except UnicodeDecodeError:
                print("⚠️ GBK 读取失败，尝试 Latin-1 编码...")
                df = pd.read_csv(csv_file_path, encoding='latin1')
        
        print(f"✅ 成功读取文件，共 {len(df)} 行数据")

        if "Journal" not in df.columns:
            print(f"❌ 错误：CSV缺少'Journal'列，当前列：{list(df.columns)}")
            return

        # --------------------------
        # 3. 数据统计与处理
        # --------------------------
        df["Journal_Clean"] = df["Journal"].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
        df["Journal_Full"] = df["Journal_Clean"].map(journal_mapping).fillna(df["Journal_Clean"])

        journal_counts = df["Journal_Full"].value_counts().reset_index()
        journal_counts.columns = ["Journal", "Paper_Count"]
        total_papers = journal_counts["Paper_Count"].sum()

        min_percent = 2
        journal_counts["Percent"] = (journal_counts["Paper_Count"] / total_papers) * 100
        
        main_journals = journal_counts[journal_counts["Percent"] >= min_percent]
        small_journals = journal_counts[journal_counts["Percent"] < min_percent]
        others_count = small_journals["Paper_Count"].sum()
        others_percent = (others_count / total_papers) * 100

        final_data = []
        for _, row in main_journals.iterrows():
            final_data.append(row.to_dict())
        
        if others_count > 0:
            final_data.append({
                "Journal": "Others",
                "Paper_Count": others_count,
                "Percent": others_percent
            })

        final_df = pd.DataFrame(final_data)
        final_df = final_df.sort_values(by="Paper_Count", ascending=False)

        # --------------------------
        # 4. 绘图配置
        # --------------------------
        labels = final_df["Journal"].tolist()
        paper_counts = final_df["Paper_Count"].tolist()
        percents = final_df["Percent"].tolist()

        # 颜色方案
        cmap = plt.get_cmap("tab20")
        colors = [cmap(i) for i in np.linspace(0, 1, len(labels))]
        for i, label in enumerate(labels):
            if label == "Others":
                colors[i] = "#BDC3C7"

        # 创建双图布局
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 10)) # 加宽画布以容纳图例

        # --- 左侧：饼图 (修改：不显示标签，只显示图例) ---
        explode = [0] * len(labels)
        
        # [关键修改] labels=None, 避免饼图周围文字重叠
        wedges, texts, autotexts = ax1.pie(
            paper_counts, 
            explode=explode, 
            labels=None,  
            colors=colors,
            autopct=lambda pct: f"{pct:.1f}%" if pct > 2 else "", # 太小的百分比也不显示，避免重叠
            startangle=90, 
            pctdistance=0.85, 
            textprops={'fontsize': 9, 'color': 'black'}
        )

        ax1.set_title("Journal Distribution (Pie Chart)", fontsize=14, fontweight='bold', pad=20)
        
        # [关键修改] 添加图例
        # bbox_to_anchor=(1.0, 0.5) 将图例放在饼图的右侧
        ax1.legend(
            wedges, 
            labels, 
            title="Journals", 
            loc="center left", 
            bbox_to_anchor=(0.9, 0.5), 
            fontsize=9,
            frameon=False # 去掉图例边框
        )

        # --- 右侧：条形图 ---
        y_pos = range(len(labels))
        bars = ax2.barh(y_pos, paper_counts, color=colors, edgecolor='white', linewidth=0.8)

        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(labels, fontsize=10)
        ax2.set_xlabel('Number of Papers', fontsize=12, fontweight='bold')
        ax2.set_title("Journal Paper Count (Bar Chart)", fontsize=14, fontweight='bold', pad=20)

        for i, (bar, count, pct) in enumerate(zip(bars, paper_counts, percents)):
            width = bar.get_width()
            ax2.text(
                width + max(paper_counts) * 0.01,
                bar.get_y() + bar.get_height()/2,
                f"{count} ({pct:.1f}%)",
                ha='left', va='center', fontsize=9,
                fontweight='normal'
            )

        ax2.grid(axis='x', alpha=0.3, linestyle='--', linewidth=0.8)
        ax2.set_xlim(0, max(paper_counts) * 1.2)

        plt.tight_layout()

        # --------------------------
        # 5. 保存图片
        # --------------------------
        file_base_name = "Journal_Distribution_Final_Fixed"

        svg_path = f"./svg/{file_base_name}.svg"
        plt.savefig(svg_path, format='svg', bbox_inches='tight', facecolor='white')
        print(f"✅ SVG 已保存: {svg_path}")

        eps_path = f"./eps/{file_base_name}.eps"
        plt.savefig(eps_path, format='eps', bbox_inches='tight', facecolor='white')
        print(f"✅ EPS 已保存: {eps_path}")

    except FileNotFoundError:
        print(f"❌ 未找到文件：{csv_file_path}")
    except Exception as e:
        print(f"❌ 发生未知错误：{str(e)}")

if __name__ == "__main__":
    plot_journals_with_pie_and_bar_solve_overlap()