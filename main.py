import datetime
import json
import os
import re
import sqlite3
import time
import pandas as pd
from urllib.parse import quote, urlencode
from DrissionPage import Chromium, ChromiumOptions
from func_timeout import func_set_timeout
from loguru import logger
from bs4 import BeautifulSoup
import requests
import urllib3
urllib3.disable_warnings()
import argparse

logger.add("qikan_cqvip_com.log",level="INFO")

class qikan_cqvip_com:


    def __init__(self):

        self.data_dir = "data"
        self.json_dir = os.path.join(self.data_dir, "json")
        self.html_dir = os.path.join(self.data_dir, "html")
        self.csv_dir = os.path.join(self.data_dir, "csv")
        

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        # 是否切换代理
        self.ip_TF = True
        self.proxy_me = None


        # 请求等待时间单位 s
        self.wait_time = 20

        # 获取浏览器
        self.browser = self.get_browser_cookies()


    # 使用自动化获取cookies
    def get_browser_cookies(self,try_max = 3):
        
        for _ in range(try_max):
            try:
                return self.get_browser_cookies_()
            except:
                continue
        logger.error(f"获取cookies失败，已重试{try_max}次")
        return None
    
    def get_browser_cookies_(self):

        # 启动浏览器
        co = ChromiumOptions().auto_port()
        # 设置浏览器路径
        # co.headless(True)  # 设置无头加载  无头模式是一种在浏览器没有界面的情况下运行的模式，它可以提高浏览器的性能和加载速度
        # co.incognito(True)  # 设置无痕模式
        # 设置ua
        co.set_user_agent(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')  # 设置ua

        # 启动浏览器W
        browser = Chromium(co).latest_tab
        return browser


    # 解析一个期刊得文章
    def analysis_get_RightArticle(self,response,cluster_item):
        soup = BeautifulSoup(response, "html.parser")

        def _clean_text(text):
            if text is None:
                return ""
            text = re.sub(r"\s+", " ", str(text))
            return text.strip()

        result = {}

        # 章节标题有时为 h5，有时为 h6，这里同时兼容
        headers = soup.find_all(["h5", "h6"])  # 如：总体技术、推进技术等
        for header in headers:
            section_name = _clean_text(header.get_text())
            if not section_name:
                continue

            # 找到该章节后紧邻的列表
            ul = header.find_next_sibling("ul")
            if not ul:
                continue

            items = []
            for li in ul.find_all("li"):
                # 标题与链接
                a_tag = None
                title_span = li.find("span", class_="title")
                if title_span:
                    a_tag = title_span.find("a")
                if a_tag is None:
                    a_tag = li.find("a")

                title = _clean_text(a_tag.get_text()) if a_tag else ""
                href = _clean_text(a_tag.get("href", "")) if a_tag else ""
                if href.startswith("/"):
                    title_url = f"https://qikan.cqvip.com{href}"
                else:
                    title_url = href

                # 作者
                writer_span = li.find("span", class_="writer")
                writer = _clean_text(writer_span.get_text()) if writer_span else ""

                # 页码：示例 (1-14) 或 (-F0002)
                pages_span = li.find("span", class_="pages")
                raw_pages = _clean_text(pages_span.get_text()) if pages_span else ""
                pages = ""
                if raw_pages:
                    m = re.search(r"\(\s*([^)]+)\s*\)", raw_pages)
                    pages = m.group(1) if m else raw_pages

                # 只要有标题或链接等信息就收集
                if any([title, title_url, writer, pages]):
                    items.append({
                        "title": title,
                        "title_url": title_url,
                        "writer": writer,
                        "pages": pages
                    })

            if items:
                result[section_name] = items

        with open(os.path.join(self.json_dir, f'期刊文章_{cluster_item["gch"]}_{cluster_item["year"]}_{cluster_item["num"]}.json'), 'w',encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        # 汇总并持久化到 CSV（幂等追加 + 去重）—— 所有期次写入同一个表格
        csv_path = self.csv_all_path
        csv_columns = [
            "cn_terms","en_terms","gch","year","num","journal_catalog","title","writer","pages","article_url"
        ]
        rows_to_append = []

        for key,value in result.items():
            for article_item in value:
                article_url = article_item["title_url"]
                title = article_item["title"]
                writer = article_item["writer"]
                pages = article_item["pages"]
                logger.info(f"获取文章详细 {article_url}")
                subject = self.get_Article_Detail(article_url)
                # 如果没有任何关键词，也写入一行空关键词，避免丢文献条目
                if not subject:
                    rows_to_append.append({
                        "gch": cluster_item["gch"],
                        "year": cluster_item["year"],
                        "num": cluster_item["num"],
                        "journal_catalog": key,
                        "title": title,
                        "writer": writer,
                        "pages": pages,
                        "article_url": article_url,
                        "cn_terms": "",
                        "en_terms": "",
                    })
                for cn_terms,en_terms in subject.items():
                    rows_to_append.append({
                        "gch": cluster_item["gch"],
                        "year": cluster_item["year"],
                        "num": cluster_item["num"],
                        "journal_catalog": key,
                        "title": title,
                        "writer": writer,
                        "pages": pages,
                        "article_url": article_url,
                        "cn_terms": cn_terms,
                        "en_terms": en_terms,
                    })

        # 安全写入 CSV：
        # - 首次创建写入表头
        # - 追加前做去重（基于文章URL+中文关键词）
        try:
            if os.path.exists(csv_path):
                try:
                    df_exist = pd.read_csv(csv_path, dtype=str)
                except Exception:
                    df_exist = pd.DataFrame(columns=csv_columns)
            else:
                df_exist = pd.DataFrame(columns=csv_columns)

            df_new = pd.DataFrame(rows_to_append, columns=csv_columns)
            # 统一为字符串，避免 NaN 影响比较
            for col in csv_columns:
                if col in df_new.columns:
                    df_new[col] = df_new[col].astype(str)
                if col in df_exist.columns:
                    df_exist[col] = df_exist[col].astype(str)

            # 去重键：同一文章的同一中文关键词只保留一条
            def _make_key(df):
                a = df.get("article_url", "")
                b = df.get("cn_terms", "")
                return (a.fillna("") + "||" + b.fillna("")) if hasattr(a, "fillna") else a + "||" + b

            if not df_exist.empty:
                exist_keys = set(_make_key(df_exist).tolist())
            else:
                exist_keys = set()

            if not df_new.empty:
                df_new_keys = _make_key(df_new).tolist()
                mask = [k not in exist_keys for k in df_new_keys]
                df_new = df_new[mask]

            # 合并并写回
            if df_exist.empty and df_new.empty:
                pass
            elif df_exist.empty:
                df_new.to_csv(csv_path, index=False, encoding="utf-8-sig")
            elif df_new.empty:
                # 无新增，跳过
                pass
            else:
                df_all = pd.concat([df_exist, df_new], ignore_index=True)
                df_all.to_csv(csv_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            logger.error(f"写入CSV失败 {csv_path} {e}")

        return result


    # 解析文章详细
    def analysis_get_Article_Detail(self,response,article_url):
        soup = BeautifulSoup(response, "html.parser")

        def _clean_text(text):
            if text is None:
                return ""
            text = re.sub(r"\s+", " ", str(text))
            return text.strip()

        # 仅提取“关键词”，并进行中英对照
        result = {}
        subject_divs = soup.find_all("div", class_="subject")
        for div in subject_divs:
            label_span = div.find("span", class_="label")
            label_text = _clean_text(label_span.get_text()) if label_span else ""
            if label_text != "关键词":
                continue

            # 中文关键词：在 <em> 之前的 <span><a>中文</a></span>
            cn_terms = []
            for child in div.children:
                name = getattr(child, "name", None)
                if name == "em":
                    break
                if name == "span":
                    a = child.find("a")
                    if a:
                        term = _clean_text(a.get_text())
                        if term:
                            cn_terms.append(term)

            # 英文关键词：在 <em> 内部的 <span>英文</span>
            en_terms = []
            em = div.find("em")
            if em:
                for sp in em.find_all("span"):
                    term = _clean_text(sp.get_text())
                    if term:
                        en_terms.append(term)

            # 对照映射（按顺序一一对应）；若只有中文没有英文，也要保留，英文置为空串
            for i, cn in enumerate(cn_terms):
                en = en_terms[i] if i < len(en_terms) else ""
                if cn:
                    result[cn] = en

            break

        return result

    # 获取文章详细
    def get_Article_Detail(self,article_url):
        self.browser.get(article_url)

        html_browser = self.browser.html

        logger.info(f"获取文章详细 {article_url}")
        article_url_id = article_url.split("?id=")[-1]
        with open(os.path.join(self.html_dir, f'文章详细_{article_url_id}.html'), 'w',encoding='utf-8') as f:
            f.write(html_browser)

        # 解析 详细 
        return self.analysis_get_Article_Detail(html_browser,article_url)

    

    # 获取一个期刊得文章
    def get_RightArticle(self,cluster_item):
        self.browser.listen.start(["qikan.cqvip.com/Journal/RightArticle"],method=["POST"])
        js_code = """
            fetch("https://qikan.cqvip.com/Journal/RightArticle", {
            "headers": {
                "accept": "text/html, */*; q=0.01",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "sec-ch-ua-mobile": "?0",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "x-requested-with": "XMLHttpRequest"
            },
            "body": "journalArticalInfoModel=___journalArticalInfoModel___",
            "method": "POST",
            "mode": "cors",
            "credentials": "include"
            });
        """

        ___journalArticalInfoModel___ = quote(json.dumps(cluster_item, ensure_ascii=False), safe="")
        js_code = js_code.replace("___journalArticalInfoModel___",___journalArticalInfoModel___)
        logger.info(___journalArticalInfoModel___)

        try:
            self.browser.run_js(js_code)
        except Exception as e:
            logger.error(f"获取一个期刊得文章 {cluster_item} {e}")
            self.browser = self.get_browser_cookies()
            return self.get_RightArticle(cluster_item)
        
        for package in self.browser.listen.steps(timeout=self.wait_time):
            try:
                if package.response.status != 200:
                    logger.info(f"获取一个期刊得文章 {cluster_item} {package.response.status}")
                    # 重新获取cookies
                    self.browser = self.get_browser_cookies()
                    return self.get_RightArticle(cluster_item)

                logger.info(f"获取一个期刊得文章 {cluster_item} {package.response.status}")
                with open(os.path.join(self.html_dir, f'期刊文章_{cluster_item["gch"]}_{cluster_item["year"]}_{cluster_item["num"]}.html'), 'w',encoding='utf-8') as f:
                    f.write(package.response.body)

                # 解析 列表 
                return self.analysis_get_RightArticle(package.response.body,cluster_item)
            except Exception as e:
                logger.error(f"获取一个期刊得文章失败: {e}")
                continue
        # 重新获取cookies
        self.browser = self.get_browser_cookies()
        return self.get_RightArticle(cluster_item) 


    # 获得所有期刊
    def get_cluster_items(self,url):

        self.browser.get(url)

        cluster_items = self.browser.eles("css=li.cluster-item > a.rightPageShow")
        cluster_items_list = []

        for cluster_item in cluster_items:
            gch = cluster_item.attr("gch")
            num = cluster_item.attr("num")
            year = cluster_item.attr("year")
            logger.info(f"期刊：{gch}，{num}，{year}")
            cluster_items_list.append({"gch":gch,"num":num,"year":year})
        return cluster_items_list

    
    def main(self):
        self.csv_all_path = os.path.join(self.csv_dir, "期刊文章.csv")

        url = 'https://qikan.cqvip.com/Qikan/Journal/Summary?kind=1&gch=61458X&from=Qikan_Search_Index'

        gch = url.split("&gch=")[-1].split("&")[0]
        cluster_items_list = self.get_cluster_items(url)

        with open(os.path.join(self.json_dir, f"cluster_items_list_{gch}.json"), "w") as f:
            json.dump(cluster_items_list, f, ensure_ascii=False, indent=4)

        for cluster_item in cluster_items_list:
            logger.info(f"获取期刊 {cluster_item}")
            self.get_RightArticle(cluster_item)

        logger.info(f"获取期刊文章完成")
        # subject = self.get_Article_Detail("https://qikan.cqvip.com/Qikan/Article/Detail?id=7200526451")
        # logger.info(subject)
if __name__ == "__main__":
    qikan = qikan_cqvip_com()
    qikan.main()
