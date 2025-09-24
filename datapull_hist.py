import csv
import os
from dotenv import load_dotenv
from jira import JIRA
import pandas as pd
import datetime
from datetime import timedelta
import warnings
import time

# Optimize pandas settings and suppress warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# Jira connection details
load_dotenv()
JIRA_URL = os.getenv("JIRA_URL")
USER_ID = os.getenv("USER_ID")
API_KEY = os.getenv("API_KEY")
# Output directory
RELEVANT_PROJECT_KEYS = [
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


def connect_to_jira():
    """Connect to JIRA and return the connection object"""
    print("Connecting to Jira...")
    try:
        jira = JIRA(
            server=JIRA_URL, basic_auth=(USER_ID, API_KEY), options={"verify": False}
        )
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


def build_project_filter_jql():
    """Build JQL filter for relevant projects"""
    project_filter = " OR ".join([f"project = {key}" for key in RELEVANT_PROJECT_KEYS])
    return f"({project_filter})"


def fetch_filtered_jira_issues(jira):
    """Fetch JIRA issues from relevant projects only from the past 18 months with pagination"""
    # Define the date range for the query (past 18 months)
    today = datetime.datetime.now()
    eighteen_months_ago = today - datetime.timedelta(days=18 * 30)

    # Build project filter
    project_filter = build_project_filter_jql()

    print(f"\nFetching JIRA issues from RELEVANT PROJECTS ONLY:")
    print(f"Projects: {', '.join(RELEVANT_PROJECT_KEYS)}")
    print(
        f"Date range: {eighteen_months_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}"
    )

    # Split into smaller date ranges to handle pagination
    date_ranges = generate_date_ranges(eighteen_months_ago, today, 30)  # 30-day chunks

    all_issues = []
    total_fetched = 0

    for start_date, end_date in date_ranges:
        print(
            f"\nProcessing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        # Construct JQL query with PROJECT FILTER FIRST, then date filter
        date_filter = f"updated >= {start_date.strftime('%Y-%m-%d')} AND updated < {end_date.strftime('%Y-%m-%d')}"
        jql_query = f"{project_filter} AND {date_filter}"

        try:
            filtered_query = f"{jql_query} ORDER BY updated DESC"
            print(f"Using JQL query: {filtered_query}")

            # First get total count for this date range
            # initial_results = jira.search_issues(filtered_query,
            #                                     fields='key',
            #                                     maxResults=1)
            # total_expected = initial_results.total
            # print(f"Found {total_expected} issues in relevant projects for current date range...")

            # if total_expected == 0:
            #     print("No relevant issues found in this date range, skipping...")
            #     continue

            # Fetch in batches of 100 (this is the Jira Cloud maximum)
            nextPageToken = None
            chunk_size = 100
            i = 1
            while True:
                try:
                    batch = jira.enhanced_search_issues(
                        filtered_query,
                        fields="*all",
                        expand="changelog",
                        nextPageToken=nextPageToken,
                        maxResults=chunk_size,
                        json_result=True,
                    )
                    this_batch_issue = batch["issues"]
                    print(this_batch_issue[0].keys())
                    break
                    # all_issues.extend(this_batch_issue)
                    # total_fetched += len(this_batch_issue)
                    # print(
                    #     f"Retrieved batch {i}: {len(this_batch_issue)} issues. Total fetched: {total_fetched}"
                    # )
                    # if batch["isLast"]:
                    #     print("Reached end of results for this date range.")
                    #     break
                    # nextPageToken = batch["nextPageToken"]
                    # i += 1
                    # start_at += batch.

                    # all_issues.extend(batch)
                    # total_fetched += len(batch)
                    # print(
                    #     f"Retrieved batch {start_at}: {len(batch)} issues. Total fetched: {total_fetched}"
                    # )
                    # if start_at > total_fetched:
                    #     break

                except Exception as e:
                    print(f"Error fetching batch {i}: {str(e)}")
                    if "429" in str(e):  # Rate limit
                        print(
                            "Rate limit hit. Waiting 30 seconds before retrying..."
                        )  # debugging step
                        time.sleep(30)
                        continue
                    else:
                        print(
                            f"Skipping batch due to error: {str(e)}"
                        )  # debugging step
                        break

        except Exception as e:
            print(f"Error fetching issues for date range: {str(e)}")  # debugging step
            print(f"JQL Query used: {jql_query}")
            continue

    print(
        f"\nCompleted fetching all date ranges for relevant projects"
    )  # debugging step
    print(
        f"Total issues collected from relevant projects: {len(all_issues)}"
    )  # debugging step

    # Verify project filtering worked
    if all_issues:
        project_keys_found = set()
        for issue in all_issues:
            if hasattr(issue.fields, "project"):
                project_keys_found.add(issue.fields.project.key)
        print(f"Project keys found in results: {sorted(project_keys_found)}")

    return all_issues


def save_historical_data(issues):
    """Save raw historical data to CSV - NON-FLATTENED (multiple entries per issue)"""
    print(f"\nProcessing {len(issues)} issues for historical data...")

    # Prepare historical data structure - each status change gets its own row
    historical_data = [
        [
            "Issue Key",
            "Summary",
            "Project",
            "Project Key",
            "Issue Type",
            "Status",
            "Assignee",
            "Reporter",
            "Priority",
            "Created",
            "Updated",
            "Resolution",
            "Fix Versions",
            "Components",
            "Labels",
            "Status Change Date",
            "Status Changed To",
            "Status Changed From",
            "Change Author",
        ]
    ]

    processed_count = 0

    for issue in issues:
        try:
            fields = issue.fields

            # Basic issue information
            issue_key = issue.key
            summary = getattr(fields, "summary", "")
            project_name = (
                getattr(fields.project, "name", "")
                if hasattr(fields, "project")
                else ""
            )
            project_key = (
                getattr(fields.project, "key", "") if hasattr(fields, "project") else ""
            )
            issue_type = (
                getattr(fields.issuetype, "name", "")
                if hasattr(fields, "issuetype")
                else ""
            )
            current_status = (
                getattr(fields.status, "name", "") if hasattr(fields, "status") else ""
            )
            assignee = (
                getattr(fields.assignee, "displayName", "Unassigned")
                if hasattr(fields, "assignee") and fields.assignee
                else "Unassigned"
            )
            reporter = (
                getattr(fields.reporter, "displayName", "")
                if hasattr(fields, "reporter") and fields.reporter
                else ""
            )
            priority = (
                getattr(fields.priority, "name", "")
                if hasattr(fields, "priority") and fields.priority
                else ""
            )
            created = getattr(fields, "created", "")
            updated = getattr(fields, "updated", "")
            resolution = (
                getattr(fields.resolution, "name", "")
                if hasattr(fields, "resolution") and fields.resolution
                else ""
            )

            # Fix versions
            fix_versions = (
                ", ".join([fv.name for fv in fields.fixVersions])
                if hasattr(fields, "fixVersions") and fields.fixVersions
                else ""
            )

            # Components
            components = (
                ", ".join([comp.name for comp in fields.components])
                if hasattr(fields, "components") and fields.components
                else ""
            )

            # Labels
            labels = (
                ", ".join(fields.labels)
                if hasattr(fields, "labels") and fields.labels
                else ""
            )

            # Process ALL historical status changes from changelog - each gets its own row
            if hasattr(issue, "changelog") and issue.changelog:
                for history in issue.changelog.histories:
                    change_author = (
                        getattr(history.author, "displayName", "")
                        if hasattr(history, "author")
                        else ""
                    )
                    for item in history.items:
                        if item.field == "status":
                            change_date = history.created
                            status_to = item.toString
                            status_from = item.fromString

                            # Each status change gets its own row - NON-FLATTENED
                            historical_data.append(
                                [
                                    issue_key,
                                    summary,
                                    project_name,
                                    project_key,
                                    issue_type,
                                    status_to,
                                    assignee,
                                    reporter,
                                    priority,
                                    created,
                                    updated,
                                    resolution,
                                    fix_versions,
                                    components,
                                    labels,
                                    change_date,
                                    status_to,
                                    status_from,
                                    change_author,
                                ]
                            )
            else:
                # If no changelog, add at least the current state
                historical_data.append(
                    [
                        issue_key,
                        summary,
                        project_name,
                        project_key,
                        issue_type,
                        current_status,
                        assignee,
                        reporter,
                        priority,
                        created,
                        updated,
                        resolution,
                        fix_versions,
                        components,
                        labels,
                        updated,
                        current_status,
                        "",
                        "",
                    ]
                )

            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} issues...")

        except Exception as e:
            print(f"Error processing issue {issue.key}: {str(e)}")
            continue

    # Ensure output directory exists
    df = pd.DataFrame(
        historical_data[1:], columns=historical_data[0]
    )  # Skip header row for DataFrame
    output_file = "jira_historical_data_non_flattened.csv"
    df.to_csv(output_file, index=False)
    print(
        f"Total records (including all status changes): {len(historical_data) - 1}"
    )  # Subtract header
    print(f"Total issues processed: {processed_count}")
    print(
        "Note: This is non-flattened data - each issue will have multiple entries (one per status change)"
    )


def main():
    """Main function to orchestrate the data pull"""
    print("Starting JIRA data pull for past 18 months...")

    # Connect to JIRA
    jira = connect_to_jira()

    # Fetch all issues
    all_issues = fetch_filtered_jira_issues(jira)

    if not all_issues:
        print("No issues found! Exiting...")
        return

    # Save historical data
    save_historical_data(all_issues)

    print("\nData pull completed successfully!")


if __name__ == "__main__":
    main()
