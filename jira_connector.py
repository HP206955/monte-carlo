import os
from dotenv import load_dotenv, dotenv_values
from datetime import date, timedelta
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
jql_query = (
    f'project in ({",".join(relevant_project_keys)}) AND updated >= "{base_date}"'
)

field_list = {
    "ID": lambda issue: issue.key,
    "Link": lambda issue: f"{jira_url}/browse/{issue.key}",
    "Title": lambda issue: issue.fields.summary,
    # "Backlog": lambda : '',
    # "In_Refinement": lambda : '',
    # "Ready": lambda : '',
    # "In_Progress": lambda : '',
    # "In_Review": lambda : '',
    # "Ready_for_QA": lambda : '',
    # "In_QA": lambda : '',
    # "Done": lambda : '',
    "Current_Status_Category": lambda issue: issue.fields.status.name,
    "Item_Rank": lambda issue : issue.fields.customfield_11029,
    "Updated": lambda issue : issue.fields.updated,
    "Issue_Type": lambda issue: issue.fields.issuetype.name,
    "Priority": lambda issue: issue.fields.priority.name,
    "Fix_versions": lambda issue: ', '.join([v.name for v in issue.fields.fixVersions]),
    "Components": lambda issue: ', '.join([c.name for c in issue.fields.components]),
    "Assignee": lambda issue: issue.fields.assignee.displayName,
    "Reporter": lambda issue: issue.fields.reporter.displayName,
    "Project": lambda issue: issue.fields.project.key,
    "Resolution": lambda issue: issue.fields.resolution.name,
    "Labels": lambda issue: ', '.join(issue.fields.labels),
    "Blocked_Days": lambda issue: '',
    "Blocked": lambda issue: 'True' if 'Blocked' in issue.fields.labels else 'False',
    "Parent": lambda : '',
    "done_datetime": lambda : '',
}

# Print field names and IDs
data = [list(field_list.keys())]

issues = jira.search_issues(jql_query, maxResults=1000)
for issue in issues:
    row = []
    for field in field_list:
        try:
            value = field_list[field](issue)
        except Exception as e:
            value = ''
        row.append(value)
    data.append(row)
df = pd.DataFrame(data[1:], columns=data[0])

print(df)
df.to_csv("jira_issues.csv", index=False)
