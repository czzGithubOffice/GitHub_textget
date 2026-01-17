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
XXXX
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
# 2️⃣ GraphQL 查询 PR
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
# 3️⃣ 抓取 PR 信息（含 Assignee）
# =========================
def stream_pull_requests(owner, repo, output_csv):
    cursor = None
    first_pr_printed = False

    query = """
    query($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequests(first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              number
              title
              body
              createdAt
              mergedAt
              closedAt
              additions
              deletions
              changedFiles
              commits { totalCount }
              reviews(first: 50) { nodes { author { login } state } }
              assignees(first: 50) { nodes { login } }
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

        prs = repo_data["pullRequests"]["edges"]
        rows = []

        for edge in prs:
            pr = edge["node"]
            reviewers = [r["author"]["login"] for r in pr["reviews"]["nodes"] if r["author"]]
            assignees = [a["login"] for a in pr["assignees"]["nodes"]]
            row = {
                "owner": owner,
                "repo": repo,
                "pr_number": pr["number"],
                "title": pr["title"],
                "body": pr["body"] if pr["body"] else "",
                "created_at": pr["createdAt"],
                "merged_at": pr["mergedAt"],
                "closed_at": pr["closedAt"],
                "additions": pr["additions"],
                "deletions": pr["deletions"],
                "changed_files": pr["changedFiles"],
                "commits_count": pr["commits"]["totalCount"],
                "reviewers": ",".join(reviewers),
                "assignees": ",".join(assignees)
            }
            rows.append(row)

            #if not first_pr_printed:
               # print(f"📝 {owner}/{repo} 第一条 PR:")
                #print(row)
                #first_pr_printed = True

        if rows:
            with write_lock:
                pd.DataFrame(rows).to_csv(output_csv, mode="a", header=False, index=False)

        page_info = repo_data["pullRequests"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
            time.sleep(1)
        else:
            break

# =========================
# 4️⃣ 项目列表
# =========================

INPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/组织项目-历史数据/ORG_projects_basic_summary_correct.csv"
OUTPUT_CSV = "/Users/chenzhenzhen/Desktop/4.开源研究想法/202501开源项目CLA布局/项目层面研究/组织项目/大语言模型文本数据/ORG_projects_PRs_metadata_with_assignees.csv"

df_projects = pd.read_csv(INPUT_CSV)
projects = list(zip(df_projects["Organization Name"], df_projects["Repository Name"]))

# 初始化输出
if not os.path.exists(OUTPUT_CSV):
    pd.DataFrame(columns=[
        "owner","repo","pr_number","title","body",
        "created_at","merged_at","closed_at",
        "additions","deletions","changed_files","commits_count",
        "reviewers","assignees"
    ]).to_csv(OUTPUT_CSV, index=False)

# =========================
# 5️⃣ 多线程抓取 PR
# =========================
def process_project(owner, repo):
    print(f"🚀 开始抓取 {owner}/{repo}")
    stream_pull_requests(owner, repo, OUTPUT_CSV)

with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(process_project, o, r) for o, r in projects]
    for f in as_completed(futures):
        f.result()

print("✅ 所有项目 PR 元数据 + 标题 + 内容 + reviewer + assignee 抓取完成")
