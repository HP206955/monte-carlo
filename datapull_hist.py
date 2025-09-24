import csv
import os
from jira import JIRA
import pandas as pd
import datetime
from datetime import timedelta
import warnings

# Optimize pandas settings and suppress warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Jira connection details
JIRA_URL = 'https://abcsupply.atlassian.net'
USER_ID = 'sristi.halder@abcsupply.com'  # change based on your user ID
API_KEY = '' #change based on your API key
# Output directory
OUTPUT_DIR = r'C:\Users\SH207048\OneDrive - abcsupply.com\Desktop\PythonCodes'

def connect_to_jira():
    """Connect to JIRA and return the connection object"""
    print("Connecting to Jira...")
    try:
        jira = JIRA(server=JIRA_URL, basic_auth=(USER_ID, API_KEY), options={'verify': False})
        myself = jira.myself()
        print(f"Successfully connected as: {myself.get('displayName', 'unknown')}")
        return jira
    except Exception as e:
        print(f"Error connecting to Jira: {str(e)}")
        raise

def generate_date_ranges(start_date, end_date, delta_days):
    """Generate date ranges for pagination to handle large datasets"""
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=delta_days)
        yield current_date, min(next_date, end_date)
        current_date = next_date

def fetch_all_jira_issues(jira):
    """Fetch all JIRA issues from the past 18 months with pagination"""
    # Define the date range for the query (past 18 months)
    today = datetime.datetime.now()
    eighteen_months_ago = today - datetime.timedelta(days=18 * 30)
    
    print(f"\nFetching ALL JIRA issues from {eighteen_months_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
    
    # Split into smaller date ranges to handle pagination
    date_ranges = generate_date_ranges(eighteen_months_ago, today, 30)  # 30-day chunks
    
    all_issues = []
    total_fetched = 0
    
    for start_date, end_date in date_ranges:
        print(f"\nProcessing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Construct JQL query for all issues updated in this date range
        jql_query = f"updated >= {start_date.strftime('%Y-%m-%d')} AND updated < {end_date.strftime('%Y-%m-%d')}"
        
        try:
            filtered_query = f"{jql_query} ORDER BY updated DESC"
            print(f"Using JQL query: {filtered_query}")
            
            # First get total count for this date range
            initial_results = jira.search_issues(filtered_query, 
                                                fields='key',
                                                maxResults=1)
            total_expected = initial_results.total
            print(f"Found {total_expected} issues in current date range...")
            
            if total_expected == 0:
                print("No issues found in this date range, skipping...")
                continue
            
            # Fetch in batches of 100 (Jira Cloud maximum)
            start_at = 0
            batch_count = 0
            
            while start_at < total_expected:
                try:
                    batch = jira.search_issues(filtered_query,
                                              fields='*all',
                                              expand='changelog',
                                              startAt=start_at,
                                              maxResults=100)
                    
                    if not batch:
                        print("Received empty batch, stopping retrieval for this date range")
                        break
                    
                    all_issues.extend(batch)
                    total_fetched += len(batch)
                    batch_count += 1
                    
                    print(f"Retrieved batch {batch_count}: {len(batch)} issues. Total fetched: {total_fetched}")
                    
                    start_at += len(batch)
                    
                    # Break if we got fewer than requested (last batch)
                    if len(batch) < 100:
                        break
                        
                except Exception as e:
                    print(f"Error fetching batch starting at {start_at}: {str(e)}")
                    if "429" in str(e):  # Rate limit
                        print("Rate limit hit. Waiting 30 seconds before retrying...")
                        import time
                        time.sleep(30)
                        continue
                    else:
                        print(f"Skipping batch due to error: {str(e)}")
                        break
                        
        except Exception as e:
            print(f"Error fetching issues for date range: {str(e)}")
            print(f"JQL Query used: {jql_query}")
            continue
    
    print(f"\nCompleted fetching all date ranges")
    print(f"Total issues collected: {len(all_issues)}")
    return all_issues

def save_historical_data(issues):
    """Save raw historical data to CSV - NON-FLATTENED (multiple entries per issue)"""
    print(f"\nProcessing {len(issues)} issues for historical data...")
    
    # Prepare historical data structure - each status change gets its own row
    historical_data = [['Issue Key', 'Summary', 'Project', 'Project Key', 'Issue Type', 'Status', 'Assignee', 
                       'Reporter', 'Priority', 'Created', 'Updated', 'Resolution', 
                       'Fix Versions', 'Components', 'Labels', 'Status Change Date', 
                       'Status Changed To', 'Status Changed From', 'Change Author']]
    
    processed_count = 0
    
    for issue in issues:
        try:
            fields = issue.fields
            
            # Basic issue information
            issue_key = issue.key
            summary = getattr(fields, 'summary', '')
            project_name = getattr(fields.project, 'name', '') if hasattr(fields, 'project') else ''
            project_key = getattr(fields.project, 'key', '') if hasattr(fields, 'project') else ''
            issue_type = getattr(fields.issuetype, 'name', '') if hasattr(fields, 'issuetype') else ''
            current_status = getattr(fields.status, 'name', '') if hasattr(fields, 'status') else ''
            assignee = getattr(fields.assignee, 'displayName', 'Unassigned') if hasattr(fields, 'assignee') and fields.assignee else 'Unassigned'
            reporter = getattr(fields.reporter, 'displayName', '') if hasattr(fields, 'reporter') and fields.reporter else ''
            priority = getattr(fields.priority, 'name', '') if hasattr(fields, 'priority') and fields.priority else ''
            created = getattr(fields, 'created', '')
            updated = getattr(fields, 'updated', '')
            resolution = getattr(fields.resolution, 'name', '') if hasattr(fields, 'resolution') and fields.resolution else ''
            
            # Fix versions
            fix_versions = ', '.join([fv.name for fv in fields.fixVersions]) if hasattr(fields, 'fixVersions') and fields.fixVersions else ''
            
            # Components
            components = ', '.join([comp.name for comp in fields.components]) if hasattr(fields, 'components') and fields.components else ''
            
            # Labels
            labels = ', '.join(fields.labels) if hasattr(fields, 'labels') and fields.labels else ''
            
            # Process ALL historical status changes from changelog - each gets its own row
            if hasattr(issue, 'changelog') and issue.changelog:
                for history in issue.changelog.histories:
                    change_author = getattr(history.author, 'displayName', '') if hasattr(history, 'author') else ''
                    for item in history.items:
                        if item.field == 'status':
                            change_date = history.created
                            status_to = item.toString
                            status_from = item.fromString
                            
                            # Each status change gets its own row - NON-FLATTENED
                            historical_data.append([
                                issue_key, summary, project_name, project_key, issue_type, status_to, assignee,
                                reporter, priority, created, updated, resolution, fix_versions,
                                components, labels, change_date, status_to, status_from, change_author
                            ])
            else:
                # If no changelog, add at least the current state
                historical_data.append([
                    issue_key, summary, project_name, project_key, issue_type, current_status, assignee, 
                    reporter, priority, created, updated, resolution, fix_versions, 
                    components, labels, updated, current_status, '', ''
                ])
            
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} issues...")
                
        except Exception as e:
            print(f"Error processing issue {issue.key}: {str(e)}")
            continue
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Save to CSV
    historical_file_path = os.path.join(OUTPUT_DIR, 'historical_data.csv')
    with open(historical_file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(historical_data)
    
    print(f"\nHistorical data saved to: {historical_file_path}")
    print(f"Total records (including all status changes): {len(historical_data) - 1}")  # Subtract header
    print(f"Total issues processed: {processed_count}")
    print("Note: This is non-flattened data - each issue will have multiple entries (one per status change)")

def main():
    """Main function to orchestrate the data pull"""
    print("Starting JIRA data pull for past 18 months...")
    print(f"Output directory: {OUTPUT_DIR}")
    
    # Connect to JIRA
    jira = connect_to_jira()
    
    # Fetch all issues
    all_issues = fetch_all_jira_issues(jira)
    
    if not all_issues:
        print("No issues found! Exiting...")
        return
    
    # Save historical data
    save_historical_data(all_issues)
    
    print("\nData pull completed successfully!")

if __name__ == "__main__":
    main()
