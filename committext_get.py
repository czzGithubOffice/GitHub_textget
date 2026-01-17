import requests
import pandas as pd
import time
import itertools
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# =========================
# 1️⃣ GitHub Tokens
# =========================
GITHUB_TOKENS = [
XXXXX
]
token_cycle = itertools.cycle(GITHUB_TOKENS)

def get_headers():
    return {
        "Authorization": f"Bearer {next(token_cycle)}",
        "Accept": "application/vnd.github+json"
    }

# 写文件锁
write_lock = Lock()

# =========================
# 2️⃣ GraphQL 请求抓 commit 元数据（含 headline 和 body）
# =========================
def run_graphql(query, variables=None, max_retries=5):
    url = "https://api.github.com/graphql"
    for _ in range(max_retries):
        try:
            r = requests.post(url, json={"query": query, "variables": variables},
                              headers=get_headers(), timeout=30)
            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    print(f"❗ GraphQL 错误: {data['errors']}")
                    return None
                return data
            elif r.status_code in [401, 403]:
                time.sleep(30)
            else:
                time.sleep(5)
        except requests.exceptions.RequestException:
            time.sleep(5)
    return None

# =========================
# 3️⃣ 边抓边写 commit
# =========================
def stream_commit_history(owner, repo, output_csv):
    cursor = None
    first_commit_printed = False

    query = """
    query($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        defaultBranchRef {
          name
          target {
            ... on Commit {
              history(first: 50, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                edges {
                  node {
                    oid
                    committedDate
                    messageHeadline
                    messageBody
                    author { name user { login } }
                    additions deletions changedFiles
                    parents { totalCount }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    while True:
        result = run_graphql(query, {"owner": owner, "name": repo, "cursor": cursor})
        if result is None:
            print(f"❌ {owner}/{repo} 无数据或查询失败")
            break

        repo_data = result["data"]["repository"]
        if repo_data is None:
            print(f"❌ {owner}/{repo} 仓库不存在或无权限")
            break

        ref = repo_data["defaultBranchRef"]
        if ref is None:
            print(f"⚠️ {owner}/{repo} 无默认分支")
            break

        history = ref["target"]["history"]
        rows = []

        for edge in history["edges"]:
            n = edge["node"]
            row = {
                "owner": owner,
                "repo": repo,
                "sha": n["oid"],
                "committed_date": n["committedDate"],
                "author_login": n["author"]["user"]["login"] if n["author"] and n["author"]["user"] else "",
                "author_name": n["author"]["name"] if n["author"] else "",
                "message_headline": n["messageHeadline"],
                "message_body": n["messageBody"] if n["messageBody"] else "",
                "lines_added": n["additions"],
                "lines_deleted": n["deletions"],
                "files_changed": n["changedFiles"],
                "parent_count": n["parents"]["totalCount"]
            }
            rows.append(row)

            # 打印首条 commit
            #if not first_commit_printed:
                #print(f"📝 {owner}/{repo} 第一条 commit:")
               # print(row)
                #first_commit_printed = True

        if rows:
            with write_lock:
                pd.DataFrame(rows).to_csv(output_csv, mode="a", header=False, index=False)

        if history["pageInfo"]["hasNextPage"]:
            cursor = history["pageInfo"]["endCursor"]
            time.sleep(1)
        else:
            break

# =========================
# 4️⃣ 项目列表
# =========================
INPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/组织项目-历史数据/ORG_projects_basic_summary_correct.csv"
OUTPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/大语言模型文本数据/ORG_projects_commit_finaltext_history_full.csv"

df_projects = pd.read_csv(INPUT_CSV)
projects = list(zip(df_projects["Organization Name"], df_projects["Repository Name"]))

# 初始化输出
if not os.path.exists(OUTPUT_CSV):
    pd.DataFrame(columns=[
        "owner","repo","sha","committed_date","author_login","author_name",
        "message_headline","message_body","lines_added","lines_deleted","files_changed","parent_count"
    ]).to_csv(OUTPUT_CSV, index=False)

# =========================
# 5️⃣ 多线程抓取
# =========================
def process_project(owner, repo):
    print(f"🚀 开始抓取 {owner}/{repo}")
    stream_commit_history(owner, repo, OUTPUT_CSV)

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(process_project, o, r) for o, r in projects]
    for f in as_completed(futures):
        f.result()

print("✅ 所有项目 commit 标题 + 内容抓取完成")
