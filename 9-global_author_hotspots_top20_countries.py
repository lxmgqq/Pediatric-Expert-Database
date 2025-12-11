import pandas as pd
import plotly.express as px
import pycountry
import numpy as np  # 用于对数变换
from collections import Counter  # 用于统计国家名出现次数
import os  # 新增：用于创建文件夹

# --------------------------
# 0. 自动创建保存目录
# --------------------------
output_dirs = ["html", "svg", "eps", "tif", "output"]
for directory in output_dirs:
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"✅ 已创建目录: ./{directory}")

# 读取数据
# 确保文件存在，防止路径报错
input_file = "./output/author_info_processed_updated.csv"
if not os.path.exists(input_file):
    print(f"❌ 未找到输入文件: {input_file}")
    exit()
    
df = pd.read_csv(input_file)

# 统计总作者数量（去重）
total_authors = df["Author"].nunique()

# --------------------------
# 处理国家名称前缀
# --------------------------
def clean_country_prefix(country_str):
    """移除国家字符串中的"国家："和"国家:"前缀"""
    if not isinstance(country_str, str):  # 处理非字符串类型（如NaN）
        return str(country_str)
    
    cleaned = country_str.replace("国家：", "").replace("国家:", "")
    return cleaned

# 1. 清洗前缀
df["Country_Cleaned_Prefix"] = df["Country"].apply(clean_country_prefix)

# 2. 去空格处理
df["Country_Raw"] = df["Country_Cleaned_Prefix"].astype(str).str.strip()
df["Country"] = df["Country_Raw"].copy()

# --------------------------
# 收集未识别的国家名
# --------------------------
country_mapping = {
    "People's Republic of China": "China",
    "PR China": "China",
    "Mainland China": "China",
    "Republic of China": "China",
    "United States of America": "United States",
    "The United States of America": "United States",
    "USA": "United States",
    "U.S.A.": "United States",
    "UK": "United Kingdom",
    "England": "United Kingdom",
    "Republic of Korea": "South Korea",
    "Korea, South": "South Korea",
    "Russian Federation": "Russia",
    "Viet Nam": "Vietnam",
    "The Netherlands": "Netherlands",
    "the Netherlands": "Netherlands",
    "Turkey": "Turkey",
    "State of Palestine": "Palestine",
    "Republic of Turkey": "Turkey",
    "Russia": "Russia",
    "Democratic Republic of the Congo": "Congo",
    "Democratic Republic of Congo": "Congo"
}

raw_country_counts = Counter(df["Country_Raw"])

unrecognized_countries = []
for country in raw_country_counts.keys():
    can_pycountry_recognize = False
    try:
        pycountry.countries.lookup(country)
        can_pycountry_recognize = True
    except LookupError:
        pass
    
    in_mapping_table = (country in country_mapping.keys()) or (country in country_mapping.values())
    
    if not can_pycountry_recognize and not in_mapping_table:
        unrecognized_countries.append({
            "Raw_Country_Name": country,
            "Appearance_Count": raw_country_counts[country],
            "Author_Count": df[df["Country_Raw"] == country]["Author"].nunique()
        })

unrecognized_df = pd.DataFrame(unrecognized_countries)
if not unrecognized_df.empty:
    unrecognized_df = unrecognized_df.sort_values("Appearance_Count", ascending=False).reset_index(drop=True)
    unrecognized_save_path = "./output/unrecognized_countries.csv"
    unrecognized_df.to_csv(unrecognized_save_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已收集 {len(unrecognized_df)} 个未识别的国家名，保存至：")
    print(f"   {unrecognized_save_path}\n")
else:
    print("✅ 所有国家名都能被识别或已在手动映射表中\n")

# --------------------------
# 国家标准化
# --------------------------
def standardize_country(name):
    try:
        return pycountry.countries.lookup(name).name
    except LookupError:
        pass
    if name in country_mapping:
        return country_mapping[name]
    return "Unknown"

df["Standardized_Country"] = df["Country"].apply(standardize_country)

# 统计有效作者数量
mapped_authors = df[df["Standardized_Country"] != "Unknown"]["Author"].nunique()
unknown_authors = total_authors - mapped_authors

print(f"数据中总共有 {total_authors} 位作者")
print(f"其中国家信息可识别并参与绘图的作者有 {mapped_authors} 位")
print(f"国家信息无法识别的作者有 {unknown_authors} 位")
print(f"有效识别率: {mapped_authors / total_authors:.2%}\n")

# --------------------------
# 通用绘图保存函数
# --------------------------
def save_plot(fig, filename_base):
    """
    保存 Plotly 图表为 html, svg, eps, tif 四种格式
    fig: Plotly Figure 对象
    filename_base: 文件名（不带后缀），例如 'author_distribution'
    """
    try:
        # 1. 保存 HTML
        html_path = f"./html/{filename_base}.html"
        fig.write_html(html_path)
        print(f"✅ HTML 已保存: {html_path}")

        # 2. 保存 SVG
        svg_path = f"./svg/{filename_base}.svg"
        fig.write_image(svg_path)
        print(f"✅ SVG 已保存: {svg_path}")

        # 3. 保存 EPS
        eps_path = f"./eps/{filename_base}.eps"
        fig.write_image(eps_path)
        print(f"✅ EPS 已保存: {eps_path}")

        # 4. 保存 TIF (scale=4 提升分辨率，约等于300dpi)
        tif_path = f"./tif/{filename_base}.tif"
        fig.write_image(tif_path, scale=4)
        print(f"✅ TIF 已保存: {tif_path}")
        
    except Exception as e:
        print(f"❌ 保存图片 {filename_base} 时发生错误: {e}")
        print("提示: 导出静态图片需安装 kaleido 库 (pip install -U kaleido)")

# --------------------------
# 图1：世界地图可视化（优化颜色条刻度） 
# --------------------------
author_counts = df.groupby("Standardized_Country")["Author"].nunique().reset_index()
author_counts.columns = ["Country", "Author_Count"]
author_counts["Log_Author_Count"] = np.log1p(author_counts["Author_Count"])  # 保持对数变换用于绘图

log_ticks = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
raw_ticks = [int(np.expm1(tick)) for tick in log_ticks]

fig1 = px.choropleth(
    author_counts[author_counts["Country"] != "Unknown"],
    locations="Country",
    locationmode="country names",
    color="Log_Author_Count",
    hover_name="Country",
    hover_data=["Author_Count"],
    color_continuous_scale="Reds",
    title="全球小儿外科专家人数分布图"
)

fig1.update_layout(
    coloraxis_colorbar=dict(
        title="专家数量",
        tickvals=log_ticks,
        ticktext=[f"{t}" for t in raw_ticks]
    )
)

# 保存图1
print("正在保存世界地图...")
save_plot(fig1, "author_distribution_optimized")
print("-" * 30)

# --------------------------
# 图2：国家条形图可视化
# --------------------------
sorted_known_counts = author_counts[author_counts["Country"] != "Unknown"] \
                          .sort_values(by=["Author_Count", "Country"], ascending=[False, True])
top20_known_counts = sorted_known_counts.head(20).reset_index(drop=True)

fig2 = px.bar(
    top20_known_counts,
    x="Country",
    y="Author_Count",
    color="Author_Count",
    color_continuous_scale="Reds",
    title="各国小儿外科专家人数分布（前20名已知国家）",
    labels={"Author_Count": "作者人数", "Country": "国家"},
    hover_data=["Author_Count"],
    text_auto=".0f"
)

fig2.update_traces(textposition="outside")
fig2.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    height=600,
    margin=dict(b=150, t=100)
)

# 保存图2
print("正在保存国家条形图...")
save_plot(fig2, "author_country_bar_chart")
print("-" * 30)

# --------------------------
# 图3：大洲条形图可视化
# --------------------------
# 创建国家到大洲的映射表 (保持原有映射表不变，为节省篇幅省略中间部分，实际运行时请保留完整字典)
country_to_continent = {
    "China": "Asia", "Japan": "Asia", "South Korea": "Asia", "United States": "North America", 
    "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe", "Brazil": "South America",
    "South Africa": "Africa", "Australia": "Oceania", 
    # ... (此处省略部分国家映射，代码中请保持你原本完整的映射表) ...
    # 建议：如果原代码里映射表很长，可以直接把原代码的字典部分复制回来
}
# 为了确保代码完整运行，这里我需要提醒你：
# 请务必保留你源代码中完整的 country_to_continent 字典！
# 这里我为了不刷屏，只写了一个简化的占位，
# 你在覆盖代码时，请把原来那个长长的字典贴回来。
# 或者如果你直接运行这个脚本，请确保下面的字典是完整的。

# --- 补充完整字典的简易逻辑 ---
# 由于你给的代码里已经有完整的字典，我建议你直接把下面的字典替换回你原始代码里的那个长字典
# 既然我在帮你修改，我就直接把上面你给的完整字典贴下来，确保你能直接运行
country_to_continent = {
    # 亚洲（Asia）
    "China": "Asia", "Japan": "Asia", "South Korea": "Asia", "North Korea": "Asia", "Mongolia": "Asia",
    "Taiwan, Province of China": "Asia", "Hong Kong": "Asia", "Macao": "Asia", "India": "Asia",
    "Pakistan": "Asia", "Bangladesh": "Asia", "Nepal": "Asia", "Bhutan": "Asia", "Sri Lanka": "Asia",
    "Maldives": "Asia", "Singapore": "Asia", "Malaysia": "Asia", "Thailand": "Asia", "Indonesia": "Asia",
    "Philippines": "Asia", "Vietnam": "Asia", "Cambodia": "Asia", "Laos": "Asia", "Myanmar": "Asia",
    "Brunei Darussalam": "Asia", "Turkey": "Asia", "Türkiye": "Asia", "Republic of Turkey": "Asia",
    "Republic of Türkiye": "Asia", "Türkiye Cumhuriyeti": "Asia", "Iran, Islamic Republic of": "Asia",
    "Iraq": "Asia", "Saudi Arabia": "Asia", "United Arab Emirates": "Asia", "Qatar": "Asia",
    "Kuwait": "Asia", "Oman": "Asia", "Yemen": "Asia", "Jordan": "Asia", "Lebanon": "Asia",
    "Israel": "Asia", "Palestine, State of": "Asia", "Syria, Arab Republic of": "Asia", "Syria": "Asia",
    "Syrian Arab Republic": "Asia", "Armenia": "Asia", "Azerbaijan": "Asia", "Georgia": "Asia",
    "Bahrain": "Asia", "Kazakhstan": "Asia", "Uzbekistan": "Asia", "Turkmenistan": "Asia",
    "Kyrgyzstan": "Asia", "Tajikistan": "Asia", "Afghanistan": "Asia",
    # 欧洲（Europe）
    "United Kingdom": "Europe", "Germany": "Europe", "France": "Europe", "Italy": "Europe", "Spain": "Europe",
    "Portugal": "Europe", "Netherlands": "Europe", "Belgium": "Europe", "Luxembourg": "Europe",
    "Switzerland": "Europe", "Austria": "Europe", "Greece": "Europe", "Ireland": "Europe", "Poland": "Europe",
    "Hungary": "Europe", "Czechia": "Europe", "Slovakia": "Europe", "Romania": "Europe", "Bulgaria": "Europe",
    "Ukraine": "Europe", "Belarus": "Europe", "Russia": "Europe", "Russian Federation": "Europe",
    "Sweden": "Europe", "Norway": "Europe", "Denmark": "Europe", "Finland": "Europe", "Iceland": "Europe",
    "Faroe Islands": "Europe", "Svalbard and Jan Mayen": "Europe", "Serbia": "Europe", "Croatia": "Europe",
    "Bosnia and Herzegovina": "Europe", "Slovenia": "Europe", "Montenegro": "Europe", "North Macedonia": "Europe",
    "Albania": "Europe", "Estonia": "Europe", "Latvia": "Europe", "Lithuania": "Europe", "Malta": "Europe",
    "Cyprus": "Europe", "San Marino": "Europe", "Vatican City State": "Europe", "Liechtenstein": "Europe",
    "Guernsey": "Europe", "Jersey": "Europe", "Isle of Man": "Europe", "Gibraltar": "Europe",
    # 北美洲（North America）
    "United States": "North America", "United States of America": "North America", "Canada": "North America",
    "Mexico": "North America", "Cuba": "North America", "Jamaica": "North America", "Haiti": "North America",
    "Dominican Republic": "North America", "Antigua and Barbuda": "North America", "Saint Kitts and Nevis": "North America",
    "Saint Lucia": "North America", "Grenada": "North America", "Saint Vincent and the Grenadines": "North America",
    "Trinidad and Tobago": "North America", "Guatemala": "North America", "Honduras": "North America",
    "El Salvador": "North America", "Nicaragua": "North America", "Costa Rica": "North America",
    "Panama": "North America", "Puerto Rico": "North America", "Virgin Islands, U.S.": "North America",
    "Virgin Islands, British": "North America", "Cayman Islands": "North America", "Turks and Caicos Islands": "North America",
    "Anguilla": "North America", "Montserrat": "North America", "British Indian Ocean Territory": "North America",
    "United States Minor Outlying Islands": "North America",
    # 南美洲（South America）
    "Brazil": "South America", "Argentina": "South America", "Chile": "South America", "Colombia": "South America",
    "Peru": "South America", "Venezuela, Bolivarian Republic of": "South America", "Ecuador": "South America",
    "Bolivia, Plurinational State of": "South America", "Paraguay": "South America", "Uruguay": "South America",
    "Guyana": "South America", "Suriname": "South America", "French Guiana": "South America",
    "Falkland Islands (Malvinas)": "South America",
    # 非洲（Africa）
    "South Africa": "Africa", "Egypt": "Africa", "Algeria": "Africa", "Tunisia": "Africa", "Libya": "Africa",
    "Morocco": "Africa", "Nigeria": "Africa", "Ghana": "Africa", "Ivory Coast": "Africa", "Côte d'Ivoire": "Africa",
    "Angola": "Africa", "Senegal": "Africa", "Mali": "Africa", "Burkina Faso": "Africa", "Niger": "Africa",
    "Guinea": "Africa", "Guinea-Bissau": "Africa", "Equatorial Guinea": "Africa", "Liberia": "Africa",
    "Sierra Leone": "Africa", "Togo": "Africa", "Benin": "Africa", "Gambia": "Africa", "Kenya": "Africa",
    "Ethiopia": "Africa", "Tanzania, United Republic of": "Africa", "Uganda": "Africa", "Rwanda": "Africa",
    "Burundi": "Africa", "Somalia": "Africa", "Eritrea": "Africa", "Djibouti": "Africa", "Cameroon": "Africa",
    "Central African Republic": "Africa", "Chad": "Africa", "Congo": "Africa", "Congo, The Democratic Republic of the": "Africa",
    "Gabon": "Africa", "Sao Tome and Principe": "Africa", "Zimbabwe": "Africa", "Zambia": "Africa",
    "Botswana": "Africa", "Namibia": "Africa", "Mozambique": "Africa", "Malawi": "Africa", "Lesotho": "Africa",
    "Eswatini": "Africa", "Mauritius": "Africa", "Madagascar": "Africa", "Comoros": "Africa", "Cabo Verde": "Africa",
    "Mauritania": "Africa", "Sudan": "Africa", "South Sudan": "Africa", "Western Sahara": "Africa",
    "Mayotte": "Africa", "Réunion": "Africa",
    # 大洋洲（Oceania）
    "Australia": "Oceania", "New Zealand": "Oceania", "Papua New Guinea": "Oceania", "Fiji": "Oceania",
    "Samoa": "Oceania", "Tonga": "Oceania", "Vanuatu": "Oceania", "Solomon Islands": "Oceania",
    "Kiribati": "Oceania", "Tuvalu": "Oceania", "Nauru": "Oceania", "Marshall Islands": "Oceania",
    "Micronesia, Federated States of": "Oceania", "Palau": "Oceania", "Timor-Leste": "Oceania",
    "Norfolk Island": "Oceania", "Heard Island and McDonald Islands": "Oceania",
    "Cocos (Keeling) Islands": "Oceania", "Christmas Island": "Oceania", "Tokelau": "Oceania",
    "French Polynesia": "Oceania", "Wallis and Futuna": "Oceania", "New Caledonia": "Oceania",
    "Pitcairn": "Oceania", "South Georgia and the South Sandwich Islands": "Oceania",
    # 南极洲（Antarctica）
    "Antarctica": "Antarctica", "French Southern Territories": "Antarctica", "Bouvet Island": "Antarctica"
}

author_counts["Continent"] = author_counts["Country"].apply(
    lambda x: country_to_continent.get(x, "Other") if x != "Unknown" else "Unknown"
)

continent_counts = author_counts.groupby("Continent")["Author_Count"].sum().reset_index()
continent_counts = continent_counts.sort_values(by="Author_Count", ascending=False).reset_index(drop=True)

fig3 = px.bar(
    continent_counts[~continent_counts["Continent"].isin(["Unknown", "Other"])],
    x="Continent",
    y="Author_Count",
    color="Author_Count",
    color_continuous_scale="Reds",
    title="各大洲小儿外科专家人数分布",
    labels={"Author_Count": "作者人数", "Continent": "大洲"},
    hover_data=["Author_Count"],
    text_auto=".0f"
)

fig3.update_traces(textposition="outside")
fig3.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    height=600,
    margin=dict(b=150, t=100)
)

# 保存图3
print("正在保存大洲条形图...")
save_plot(fig3, "author_continent_bar_chart")
print("-" * 30)
print("✅ 所有图表绘制与保存完成！")