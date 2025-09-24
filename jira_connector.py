import os
from dotenv import load_dotenv, dotenv_values
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from jira import JIRA
import pandas as pd
import warnings

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# Define Jira Server URL and authentication details
load_dotenv()
jira_url = os.getenv("JIRA_URL")
email = os.getenv("USER_ID")
token = os.getenv("API_KEY")

# Establish connection
jira_options = {"verify": False}  # Disable SSL verification
jira = JIRA(
    options=jira_options,
    server=jira_url,
    basic_auth=(email, token),
)
print("Connected to Jira successfully!")


# Fetch all tickets from the last 12 months
relevant_project_keys = [
    "PS2",
    "ITE",
    "PLE",
    "ITP",
    "ORT",
    "CUS",
    "PER",
    "MOB",
    "ORC",
    "OSI",
    "PRD",
]  # List of teams needed to be pulled
base_date = date.today() - relativedelta(years=1, months=6)  # 18 months ago
print(f"Fetching tickets updated since {base_date}...")
jql_query = f'project in ({",".join(relevant_project_keys)}) AND updated >= "{base_date}" order by updated DESC'

historical_field_list = {
    "ID": lambda issue: issue["key"],
    "Link": lambda issue: f"https://abcsupply.atlassian.net/browse/{issue["key"]}",
    "Title": lambda issue: issue["fields"]["summary"],
    "Backlog": lambda issue: datetime.strptime(
        issue["fields"]["created"].split("T")[0], "%Y-%m-%d"
    ).date(),
    "Current_Status_Category": lambda issue: issue["fields"]["status"]["name"],
    "Item_Rank": lambda issue: issue["fields"]["customfield_10000"],
    "Updated": lambda issue: datetime.strptime(
        issue["fields"]["updated"].split("T")[0], "%Y-%m-%d"
    ).date(),
    "Issue_Type": lambda issue: issue["fields"]["issuetype"]["name"],
    "Priority": lambda issue: issue["fields"]["priority"]["name"],
    "Fix_versions": lambda issue: ", ".join(
        [v["name"] for v in issue["fields"]["fixVersions"]]
    ),
    "Components": lambda issue: ", ".join(
        [c["name"] for c in issue["fields"]["components"]]
    ),
    "Assignee": lambda issue: issue["fields"]["assignee"]["displayName"],
    "Reporter": lambda issue: issue["fields"]["reporter"]["displayName"],
    "Project": lambda issue: issue["fields"]["project"]["key"],
    "Resolution": lambda issue: issue["fields"]["resolution"]["name"],
    "Labels": lambda issue: (
        f"[{"|".join(issue["fields"]["labels"])}]" if issue["fields"]["labels"] else ""
    ),
    "Blocked_Days": lambda issue: "",  # TODO: Ask about this field
    "Blocked": lambda issue: "FALSE",  # TODO: Ask about this field
    "Parent": lambda issue: issue["fields"]["parent"]["key"],
    "done_datetime": lambda issue: issue["fields"]["resolutiondate"],
    "Status Change Date": lambda: "",
    "Status Change From": lambda: "",
    "Status Change To": lambda: "",
}

# flattened_field_list = historical_field_list
# flattened_field_list.update(
#     {
#         "Backlog": lambda: "",
#         "In_Refinement": lambda: "",
#         "Ready": lambda: "",
#         "In_Progress": lambda: "",
#         "In_Review": lambda: "",
#         "Ready_for_QA": lambda: "",
#         "In_QA": lambda: "",
#         "Done": lambda: "",
#     }
# )
# Print field names and IDs
data = [list(historical_field_list.keys())]
chunk_size = 100
all_issues = []
total_fetched = 0
i = 1
nextPageToken = None
while True:
    issues = jira.enhanced_search_issues(
        jql_query,
        fields="*all",
        expand="changelog",
        nextPageToken=nextPageToken,
        maxResults=chunk_size,
        json_result=True,
    )
    this_batch_issue = issues["issues"]
    all_issues.extend(this_batch_issue)
    total_fetched += len(this_batch_issue)
    print(
        f"Retrieved batch {i}: {len(this_batch_issue)} issues. Total fetched: {total_fetched}"
    )
    if issues["isLast"]:
        print("Reached end of results for this date range.")
        break
    nextPageToken = issues["nextPageToken"]
    i += 1

for issue in all_issues:
    for status_change in issue["changelog"]["histories"]:
        for status_item in status_change["items"]:
            row = []
            if status_item["field"] != "status" or status_item["toString"] not in [
                "In Refinement",
                "Ready",
                "In Progress",
                "In Review",
                "Ready for QA",
                "In QA",
                "Done",
            ]:
                break
            for field in historical_field_list:
                try:
                    if field == "Status Change Date":
                        value = datetime.strptime(
                            status_change["created"].split("T")[0], "%Y-%m-%d"
                        ).date()
                    elif field == "Status Change From":
                        value = status_item["fromString"]
                    elif field == "Status Change To":
                        value = status_item["toString"]
                    else:
                        value = historical_field_list[field](issue)
                except Exception as e:
                    value = ""
                row.append(value)
            data.append(row)
df = pd.DataFrame(data[1:], columns=data[0])
historical_df = df
print(historical_df)
historical_df.to_csv("jira_issues_historical.csv", index=False)

status_map = {
    "In Refinement": "In_Refinement",
    "Ready": "Ready",
    "In Progress": "In_Progress",
    "In Review": "In_Review",
    "Ready for QA": "Ready_for_QA",
    "In QA": "In_QA",
    "Done": "Done",
}

pivoted_df = (
    pd.pivot_table(
        df,
        index="ID",
        columns="Status Change To",
        values="Status Change Date",
        aggfunc="first",
        fill_value="",
    )
    .reset_index()
    .rename_axis(None, axis=1)
)
pivoted_df = pivoted_df.rename(columns=status_map)
pivoted_df = pd.merge(
    historical_df.drop(
        ["Status Change Date", "Status Change From", "Status Change To"], axis=1
    ).drop_duplicates(),
    pivoted_df,
    on="ID",
)

print(pivoted_df)
pivoted_df.to_csv("jira_issues_pivoted.csv", index=False)
