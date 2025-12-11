import urllib.parse

def build_pubmed_url(pediatric_terms, surgical_terms, journals, size=200):
    # 处理儿科相关关键词（使用+OR+连接）
    pediatric_query = "+OR+".join(
        [f'%22{urllib.parse.quote_plus(term.strip())}%22%5BTitle%2FAbstract%5D'
         for term in pediatric_terms]
    )

    # 处理外科相关关键词（使用+OR+连接）
    surgical_query = "+OR+".join(
        [f'%22{urllib.parse.quote_plus(term.strip())}%22%5BTitle%2FAbstract%5D'
         for term in surgical_terms]
    )

    # 组合儿科和外科关键词，用AND连接
    combined_keywords = f"%28{pediatric_query}%29+AND+%28{surgical_query}%29"

    # 处理期刊部分（使用+OR+连接）
    journal_query = "+OR+".join(
        [f'%22{urllib.parse.quote_plus(j.strip())}%22%5BJournal%5D'
         for j in journals]
    )

    # 最终查询：(儿科关键词) AND (外科关键词) AND (期刊)
    full_query = f"{combined_keywords}+AND+%28{journal_query}%29"

    # 基础URL
    base_url = "https://pubmed.ncbi.nlm.nih.gov/"

    # 参数构建
    params = {
        "term": full_query,
        "sort": "",
        "size": size
    }

    # 处理参数
    param_str = []
    for k, v in params.items():
        if isinstance(v, list):
            for item in v:
                param_str.append(f"{k}={item}")
        else:
            param_str.append(f"{k}={v}")

    return f"{base_url}?{'&'.join(param_str)}"


# 儿科相关关键词组
pediatric_terms = [
    "pediatric*",
    "paediatric*",
    "child*",
    "children",
    "infant*",
    "neonat*",
    "newborn*",
    "toddler*",
    "adolescent*",
    "teenager*",
    "youth",
    "juvenile*"
]

# 外科相关关键词组
surgical_terms = [
    "surgery",
    "surgical",
    "surgical intervention*",
    "surgical treatment*",
    "operative procedure*",
    "laparoscopy",
    "laparoscopic",
    "thoracoscopy",
    "thoracoscopic",
    "minimally invasive surgery",
    "robotic surgery",
    "robot-assisted surgery"
]

# 期刊列表保持不变
journals = [
    "N Engl J Med",
    "Lancet",
    "JAMA",
    "Br Med J",
    "Nature",
    "Science",
    "Cell",
    "J Pediatr Surg",
    "Pediatr Surg Int",
    "Eur J Pediatr Surg",
    "World J Pediatr Surg",
    "Ann Pediatr Surg",
    "J Pediatr Surg Open",
    "J Pediatr Surg Case Rep",
    "JAMA Pediatr",
    "Lancet Child Adolesc Health",
    "Pediatr Res",
    "Arch Dis Child Educ Pract Ed",
    "J Pediatr",
    "Pediatrics",
    "World J Pediatr",
    "Ann Surg",
    "Br J Surg",
    "Surgery",
    "World J Surg"
]

# 生成URL
url = build_pubmed_url(pediatric_terms, surgical_terms, journals)

# 保存URL到文件
with open('./output/pubmed_urls.txt', 'w', encoding='utf-8') as f:
    f.write(url)

print("生成的PubMed检索URL已保存到文件")
print("URL:", url)
