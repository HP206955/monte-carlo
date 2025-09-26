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

def get():
    # Fetch all tickets from the last 18 months
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
        ).date(),  # TODO: Confirm if this is the correct field for Backlog date
        "Current_Status_Category": lambda issue: issue["fields"]["status"]["name"]
        .upper()
        .replace(" ", "_"),  # TODO: "Development In Progress"?
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
    }

    # Print field names and IDs
    MAX_FETCH = None  # Limit the total number of issues fetched for testing
    data = [list(historical_field_list.keys())]
    chunk_size = 100
    all_issues = []
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
        print(
            f"Retrieved batch {i}: {len(this_batch_issue)} issues. Total fetched: {len(all_issues)}"
        )
        if MAX_FETCH and len(all_issues) >= MAX_FETCH:
            print(f"Reached max fetch limit of {MAX_FETCH}. Stopping.")
            break
        if issues["isLast"]:
            print("Reached end of results for this date range.")
            break
        nextPageToken = issues["nextPageToken"]
        i += 1

    for issue in all_issues:
        row = []
        for field in historical_field_list:
            try:
                value = historical_field_list[field](issue)
            except Exception as e:
                value = ""
            row.append(value)
        data.append(row)

    status_change_data = [
        [
            "ID",
            "Status Change Date",
            "Status Change From",
            "Status Change To",
        ]
    ]

    for issue in all_issues:
        for status_change in issue["changelog"]["histories"]:
            for status_item in status_change["items"]:
                row = [issue["key"]]
                if status_item["field"] != "status" or status_item["toString"] not in [
                    "In Refinement",
                    "Ready",
                    "In Progress",
                    "In Review",
                    "Ready for QA",
                    "In QA",
                    "Done",
                ]:
                    continue
                for field in status_change_data[0]:
                    if field == "ID":
                        continue
                    try:
                        if field == "Status Change Date":
                            value = datetime.strptime(
                                status_change["created"].split("T")[0], "%Y-%m-%d"
                            ).date()
                        elif field == "Status Change From":
                            value = status_item["fromString"]
                        elif field == "Status Change To":
                            value = status_item["toString"]
                    except Exception as e:
                        value = ""
                    row.append(value)
                status_change_data.append(row)
    df = pd.DataFrame(data[1:], columns=data[0])
    status_change_df = pd.DataFrame(status_change_data[1:], columns=status_change_data[0])
    df = df.merge(status_change_df, on="ID", how="left")
    print(df)
    print(f"Total unique issues: {len(df["ID"].unique())}")
    df.to_csv("data/jira_issues_historical.csv", index=False)
