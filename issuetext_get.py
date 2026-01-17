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
   XXXXXX
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
# 2️⃣ GraphQL 请求
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
# 3️⃣ 抓 Issue（每个 Issue 一行）+ 评论作者列表
# =========================
def stream_issues(owner, repo, output_csv):
    cursor = None
    first_issue_printed = False

    query = """
    query($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        issues(first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              number
              title
              body
              state
              createdAt
              closedAt
              assignees(first: 50) { nodes { login } }
              labels(first: 50) { nodes { name } }
              comments(first: 50) { nodes { author { login } } }
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

        issues = repo_data["issues"]["edges"]
        rows = []

        for edge in issues:
            issue = edge["node"]
            assignees = [a["login"] for a in issue["assignees"]["nodes"]]
            labels = [l["name"] for l in issue["labels"]["nodes"]]
            comment_authors = [c["author"]["login"] for c in issue["comments"]["nodes"] if c["author"]]

            row = {
                "owner": owner,
                "repo": repo,
                "issue_number": issue["number"],
                "title": issue["title"],
                "body": issue["body"] or "",
                "state": issue["state"],
                "created_at": issue["createdAt"],
                "closed_at": issue["closedAt"],
                "assignees": ",".join(assignees),
                "labels": ",".join(labels),
                "comment_authors": ",".join(comment_authors)
            }
            rows.append(row)

            #if not first_issue_printed and rows:
               # print(f"📝 {owner}/{repo} 第一条 Issue 示例:")
                #print(row)
              #  first_issue_printed = True

        if rows:
            with write_lock:
                pd.DataFrame(rows).to_csv(output_csv, mode="a", header=False, index=False)

        page_info = repo_data["issues"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
            time.sleep(1)
        else:
            break

# =========================
# 4️⃣ 项目列表
# =========================
INPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/组织项目-历史数据/ORG_projects_basic_summary_correct.csv"
OUTPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/大语言模型文本数据/ORG_projects_issue_metadata_with_assignees.csv"
INPUT_CSV = "C:\\Users\\Administrator\\Desktop\\python项目数据cla\\ORG_python_projects_20220112_all.csv"
OUTPUT_CSV = "C:\\Users\\Administrator\\Desktop\\python项目数据cla\\ORG_projects_PRs_text_with_assignees.csv"

df_projects = pd.read_csv(INPUT_CSV)
projects = list(zip(df_projects["Organization Name"], df_projects["Repository Name"]))

# 初始化输出
if not os.path.exists(OUTPUT_CSV):
    pd.DataFrame(columns=[
        "owner","repo","issue_number","title","body","state","created_at","closed_at",
        "assignees","labels","comment_authors"
    ]).to_csv(OUTPUT_CSV, index=False)

# =========================
# 5️⃣ 多线程抓取 Issue
# =========================
def process_project(owner, repo):
    print(f"🚀 开始抓取 {owner}/{repo}")
    stream_issues(owner, repo, OUTPUT_CSV)

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(process_project, o, r) for o, r in projects]
    for f in as_completed(futures):
        f.result()

print("✅ 所有项目 Issue 抓取完成（评论作者列表已包含）")
