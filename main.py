import csv
import os
from jira import JIRA
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import warnings

# Optimize pandas settings and suppress warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Jira connection details
JIRA_URL = os.getenv('JIRA_URL') 
USER_ID = os.getenv('USER_ID')
API_KEY = os.getenv('API_KEY')

# Connect to Jira
print("Connecting to Jira...")
try:
    jira = JIRA(server=JIRA_URL, basic_auth=(USER_ID, API_KEY), options={'verify': False})
    myself = jira.myself()
    print(f"Successfully connected as: {myself.get('displayName', 'unknown')}")
except Exception as e:
    print(f"Error connecting to Jira: {str(e)}")
    raise

# Fetches all custom fields from Jira and saves them to a CSV file for reference
fields = jira.fields()
custom_fields = [field for field in fields if field.get('custom')]

with open('custom_fields.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['ID', 'Name', 'Type'])
    for field in custom_fields:
        writer.writerow([field['id'], field['name'], field['schema']['type']])

print("Custom fields saved to custom_fields.csv")

# Define Kanban teams and JQL filter
kanban_teams = [
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
]

# Format teams for JQL query
kanban_teams_jql = ', '.join(f'"{team}"' for team in kanban_teams)
project_filter = f"project = {kanban_teams_jql}"

# Workaround for pagination using date ranges

def generate_date_ranges(start_date, end_date, delta_days):
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + datetime.timedelta(days=delta_days)
        yield current_date, min(next_date, end_date)
        current_date = next_date

# Define the date range for the query
today = datetime.datetime.now()
eighteen_months_ago = today - relativedelta(years=1, months=6)
date_ranges = generate_date_ranges(eighteen_months_ago, today, 30)  # Split into 30-day chunks

all_issues = []
print("\nFetching issues from Jira for specified teams...")
print(f"Teams included: {', '.join(kanban_teams)}")
# for start_date, end_date in date_ranges:
for start_date, end_date in [date_ranges[5]]:
    print(f"\nProcessing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    # Construct JQL with proper filtering
    jql_query = f"project in ({kanban_teams_jql}) AND updated >= {start_date.strftime('%Y-%m-%d')} AND updated < {end_date.strftime('%Y-%m-%d')}"
    try:
        filtered_query = f"{jql_query} ORDER BY updated DESC"
        print(f"Using JQL query: {filtered_query}")  # Debug line to see the actual query
        
        # First get total count
        initial_results = jira.enhanced_search_issues(filtered_query, 
                                                    fields='key',
                                                    maxResults=1)
        total_expected = len(initial_results)
        print(f"Found {total_expected} issues in current date range...")
        
        # Fetch in batches of 100 (Jira Cloud maximum)
        start_at = 0
        # while True:
        for i in range(5):
            batch = jira.enhanced_search_issues(filtered_query,
                                              fields='*all',
                                              expand='changelog',
                                              maxResults=100)
            
            if not batch:
                break
                
            all_issues.extend(batch)
            print(f"Retrieved {len(all_issues)} issues so far...")
            
            if len(batch) < 100:  # Last batch
                break
    except Exception as e:
        print(f"Error fetching issues: {str(e)}")
        print(f"JQL Query used: {jql_query}")
        raise

print(f"\nCompleted fetching all date ranges")
print(f"Total issues collected: {len(all_issues)}")
print(f"Starting to process collected issues...")

# Process the collected issues
issues = all_issues
if not issues:
    raise ValueError("No issues found in Jira query results")

# Prepare data structures
issue_data = []
historical_data = [['Issue Key', 'Summary', 'Assignee', 'Status', 'Date Entered Status']]

# Iterate through each issue to extract details and historical data
total_issues = len(issues)
print(f"\nProcessing {total_issues} issues...")
for index, issue in enumerate(issues, 1):
    fields = issue.fields
    try:
        project_key = fields.project.key if hasattr(fields.project, 'key') else 'Unknown'
    except Exception:
        project_key = 'Unknown'

    issue_data.append({
        'ID': issue.key,
        'Link': f"{JIRA_URL}/browse/{issue.key}",
        'Title': fields.summary,
        'Project': project_key,  # Use the safely extracted project_key
        'Backlog': '',
        'In_Refinement': '',
        'Ready': '',
        'In_Progress': '',
        'In_Review': '',
        'Ready_for_QA': '',
        'In_QA': '',
        'Done': '',
        'Current_Status_Category': fields.status.name if fields.status else '',
        'Item_Rank': getattr(fields, 'customfield_10016', ''),
        'Updated': fields.updated,
        'Issue_Type': fields.issuetype.name,
        'Priority': fields.priority.name if fields.priority else '',
        'Fix_versions': ', '.join([fv.name for fv in fields.fixVersions]) if fields.fixVersions else '',
        'Components': ', '.join([comp.name for comp in fields.components]) if fields.components else '',
        'Assignee': getattr(fields.assignee, 'displayName', 'Unassigned') if hasattr(fields, 'assignee') else 'Unassigned',
        'Reporter': fields.reporter.displayName if fields.reporter else '',
        'Resolution': fields.resolution.name if fields.resolution else '',
        'Labels': ', '.join(fields.labels) if fields.labels else '',
        'Blocked_Days': '',
        'Blocked': 'True' if 'Blocked' in fields.labels else 'False',
        'Parent': '',
        'done_datetime': ''
    })

    # Extract historical status changes from the changelog
    summary = issue.fields.summary
    assignee = getattr(issue.fields.assignee, 'displayName', 'Unassigned') if hasattr(issue.fields, 'assignee') else 'Unassigned'
    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == 'status':
                full_timestamp = history.created
                formatted_date = pd.to_datetime(full_timestamp).strftime('%Y-%m-%d')
                formatted_time = pd.to_datetime(full_timestamp).strftime('%H:%M:%S')
                historical_data.append([issue.key, summary, assignee, item.toString, formatted_date, formatted_time])



# Converts the issue data into a Pandas DataFrame
print("\nCreating DataFrame from issue_data...")
if not issue_data:
    print("ERROR: issue_data is empty!")
    raise ValueError("No data collected from Jira")

# Create DataFrame
df = pd.DataFrame(issue_data)

# Verify Project column exists
if 'Project' not in df.columns:
    raise KeyError("Project column not found in DataFrame")

# Add Project Name column (teams are already filtered in JQL)
df['Project_Name'] = df.apply(lambda x: next((issue.fields.project.name 
                                            for issue in issues 
                                            if issue.key == x['ID']), None), axis=1)

# Calculate additional metrics
df['stage'] = df['Current_Status_Category'].apply(lambda x: 'In Progress' if x in ['IN_QA', 'READY_FOR_QA', 'IN_REVIEW', 'IN_PROGRESS'] else ('To Do' if x in ['READY', 'IN_REFINEMENT', 'BACKLOG'] else ('Done' if x in ['DONE', 'REJECTED', "WON'T_DO"] else 'Unknown')))
workflow_stage_mapping = {
    'IN_QA': 'In QA',
    'READY_FOR_QA': 'Ready for QA',
    'IN_REVIEW': 'In Review',
    'IN_PROGRESS': 'In Progress',
    'READY': 'Ready',
    'IN_REFINEMENT': 'In Refinement',
    'BACKLOG': 'To Do',
    'DONE': 'Done',
    'REJECTED': 'Done',
    "WON'T_DO": 'Done'
}
df['workflow_stage'] = df['Current_Status_Category'].map(workflow_stage_mapping).fillna('Unknown')

# Standardize datetime objects 
for col in ['Backlog', 'In_Progress', 'Done', 'Updated']:
    if col in df.columns:
        # Convert to datetime with UTC timezone
        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
        # Convert to local timezone
        df[col] = df[col].dt.tz_convert(None)


# Calculates cycle time, lead time, work item age, stage age, and staleness metrics  #This follows the SQL logic of master.sql provided 
df['cycle_time'] = df.apply(lambda row: (row['Done'] - row['In_Progress']).days + 1 if row['Current_Status_Category'] == 'DONE' else 0, axis=1)
df['lead_time'] = df.apply(lambda row: (row['Done'] - row['Backlog']).days + 1 if pd.notnull(row['Done']) and pd.notnull(row['Backlog']) and row['Current_Status_Category'] == 'DONE' else 0, axis=1)
df['work_item_age'] = df.apply(lambda row: (pd.Timestamp.now() - row['In_Progress']).days + 1 if pd.notnull(row['In_Progress']) and row['Current_Status_Category'] in ['IN_PROGRESS', 'IN_REVIEW', 'READY_FOR_QA', 'IN_QA'] else 0, axis=1)
def calculate_stage_age(row):
    try:
        stage_column = row['workflow_stage'].replace(' ', '_')
        if stage_column in row.index and pd.notnull(row[stage_column]) and row['workflow_stage'] in ['In QA', 'Ready for QA', 'In Review', 'In Progress', 'Ready', 'In Refinement', 'To Do']:
            return (pd.Timestamp.now() - row[stage_column]).days + 1
        return 0
    except:
        return 0

df['stage_age'] = df.apply(calculate_stage_age, axis=1)
df['staleness'] = df['Updated'].apply(lambda x: (pd.Timestamp.now() - x).days if pd.notnull(x) else 0)
df['planned'] = df['Issue_Type'].apply(lambda x: 'Planned' if x == 'Story' else 'Unplanned')

# Save to Jira_data directory
user_directory = r'.'
if not os.path.exists(user_directory):
    os.makedirs(user_directory)

file_path = os.path.join(user_directory, 'jira_issues.csv')
df.to_csv(file_path, index=False)

print(f"Jira issues with derived metrics saved to {file_path}")

# Save historical data to specified 
user_directory = r'.'  #change based on your device
if not os.path.exists(user_directory):
    os.makedirs(user_directory)

historical_file_path = os.path.join(user_directory, 'historical_data.csv')
with open(historical_file_path, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerows(historical_data)

print(f"Historical data with formatted date and time saved to {historical_file_path}")

# Populates status timestamps for each issue based on historical data.
status_columns = ['Backlog', 'In_Refinement', 'Ready', 'In_Progress', 'In_Review', 'Ready_for_QA', 'In_QA', 'Done']
processed_data = []

for issue in issues:
    fields = issue.fields
    status_timestamps = {status: '' for status in status_columns}

    # Populate status timestamps from historical data
    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == 'status':
                status = item.toString.replace(' ', '_')
                if status in status_columns:
                    status_timestamps[status] = history.created

    # Add processed data for the issue
    # Get project key safely
    try:
        project_key = fields.project.key if hasattr(fields.project, 'key') else 'Unknown'
    except Exception as e:
        print(f"Error getting project for issue {issue.key}:", str(e))
        project_key = 'Error'

    processed_data.append({
        'ID': issue.key,
        'Link': f"{JIRA_URL}/browse/{issue.key}",
        'Title': fields.summary,
        'Project': project_key,
        **status_timestamps,
        'Current_Status_Category': fields.status.name if fields.status else '',
        'Item_Rank': getattr(fields, 'customfield_10016', ''),
        'Updated': fields.updated,
        'Issue_Type': fields.issuetype.name,
        'Priority': fields.priority.name if fields.priority else '',
        'Fix_versions': ', '.join([fv.name for fv in fields.fixVersions]) if fields.fixVersions else '',
        'Components': ', '.join([comp.name for comp in fields.components]) if fields.components else '',
        'Assignee': getattr(fields.assignee, 'displayName', 'Unassigned') if hasattr(fields, 'assignee') else 'Unassigned',
        'Reporter': fields.reporter.displayName if fields.reporter else '',
        'Resolution': fields.resolution.name if fields.resolution else '',
        'Labels': ', '.join(fields.labels) if fields.labels else '',
        'Blocked_Days': '',
        'Blocked': 'True' if 'Blocked' in fields.labels else 'False',
        'Parent': '',
        'done_datetime': ''
    })

# Save processed data
df_processed = pd.DataFrame(processed_data)
processed_file_path = os.path.join(user_directory, 'processed_data.csv')
df_processed.to_csv(processed_file_path, index=False)
print(f"Processed data saved to {processed_file_path}")

# Ensure datetime-like values for relevant columns
for col in ['Done', 'In_Progress', 'Backlog', 'Updated']:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Update calculations to avoid using .dt on non-datetime columns
df['cycle_time'] = df.apply(
    lambda row: (row['Done'] - row['In_Progress']).days + 1 if pd.notnull(row['Done']) and pd.notnull(row['In_Progress']) and row['Current_Status_Category'] == 'DONE' else 0,
    axis=1
)