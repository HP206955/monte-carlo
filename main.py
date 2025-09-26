import pandas as pd
import datetime
import os
import warnings

# Optimize pandas settings and suppress warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')

# File paths
INPUT_DIR = r'C:\Users\SH207048\OneDrive - abcsupply.com\Desktop\PythonCodes'
OUTPUT_DIR = r'C:\Users\SH207048\OneDrive - abcsupply.com\Desktop\PythonCodes'
HISTORICAL_FILE = os.path.join(INPUT_DIR, 'jira_historical.csv')

def load_historical_data():
    """Load the historical data CSV (already pre-filtered by DataPull_hist.py)"""
    print("Loading historical data...")
    try:
        df = pd.read_csv(HISTORICAL_FILE)
        print(f"Loaded {len(df)} historical records (pre-filtered for relevant projects)")
        return df
    except FileNotFoundError:
        print(f"Error: Historical data file not found at {HISTORICAL_FILE}")
        raise
    except Exception as e:
        print(f"Error loading historical data: {str(e)}")
        raise

def flatten_historical_data(df):
    """Flatten the historical data - convert multiple status entries per issue to single row with timestamps"""
    print("Flattening historical data...")
    
    issue_data = []
    
    for issue_key, group in df.groupby('Issue Key'):
        latest_record = group.sort_values('Status Change Date', ascending=False).iloc[0]
        
        # Get Fix_versions and Components, handle empty/null values
        fix_versions = latest_record.get('Fix Versions')
        if pd.isna(fix_versions) or fix_versions == '' or str(fix_versions).strip() == '':
            fix_versions = 'N/A'
            
        components = latest_record.get('Components')
        if pd.isna(components) or components == '' or str(components).strip() == '':
            components = 'N/A'
        
        # Initialize base row with exact column names as specified
        row = {
            'ID': issue_key,
            'Link': f"https://abcsupply.atlassian.net/browse/{issue_key}",
            'Title': latest_record.get('Summary', 'N/A'),
            'Backlog': latest_record.get('Backlog', 'N/A'),
            'In_Refinement': latest_record.get('In Refinement', 'N/A'),
            'Ready': latest_record.get('Ready', 'N/A'),
            'In_Progress': latest_record.get('In Progress', 'N/A'),
            'In_Review': latest_record.get('In Review', 'N/A'),
            'Ready_for_QA': latest_record.get('Ready for QA', 'N/A'),
            'In_QA': latest_record.get('In QA', 'N/A'),
            'Done': latest_record.get('Done', 'N/A'),
            'Current_Status_Category': latest_record.get('Status', 'N/A'),
            'Item_Rank': 'N/A',  # Not available in current data
            'Updated': latest_record.get('Updated', 'N/A'),
            'Issue_Type': latest_record.get('Issue Type', 'N/A'),
            'Priority': latest_record.get('Priority', 'N/A'),
            'Fix_versions': fix_versions,
            'Components': components,
            'Assignee': latest_record.get('Assignee', 'N/A'),
            'Reporter': latest_record.get('Reporter', 'N/A'),
            'Project': latest_record.get('Project Key', 'N/A'),
            'Resolution': latest_record.get('Resolution', 'N/A'),
            'Labels': latest_record.get('Labels', 'N/A'),
            'Blocked_Days': 0,  # Will be calculated
            'Blocked': 'No',    # Will be determined
            'Parent': 'N/A',    # Not available in current data
            'done_datetime': 'N/A'  # Will be set if Done status exists
        }
        
        # Process status changes to populate timestamp columns
        for _, record in group.iterrows():
            status = record.get('Status Changed To', '')
            change_date = record.get('Status Change Date', '')
            
            # Map JIRA statuses to exact column names
            status_mapping = {
                'Backlog': 'Backlog',
                'In Refinement': 'In_Refinement', 
                'Ready': 'Ready',
                'In Progress': 'In_Progress',
                'In Review': 'In_Review',
                'Ready for QA': 'Ready_for_QA',
                'In QA': 'In_QA',
                'Done': 'Done'
            }
            
            if status in status_mapping and change_date:
                row[status_mapping[status]] = change_date
                
                # Set done_datetime if status is Done
                if status == 'Done':
                    row['done_datetime'] = change_date
        
        issue_data.append(row)
    
    flattened_df = pd.DataFrame(issue_data)
    print(f"Flattened to {len(flattened_df)} unique issues")
    return flattened_df

def compute_metrics(df):
    """Compute additional metrics on the flattened data"""
    print("Computing additional metrics...")
    
    # Add all the previous calculated metrics with appropriate column names
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

    # Convert date columns to datetime (only non-N/A values)
    date_columns = ['Backlog', 'In_Refinement', 'Ready', 'In_Progress', 'In_Review', 'Ready_for_QA', 'In_QA', 'Done', 'Updated', 'done_datetime']
    for col in date_columns:
        if col in df.columns:
            # Replace 'N/A' with NaT for datetime conversion, but keep as 'N/A' string for display
            df[col + '_datetime'] = pd.to_datetime(df[col].replace('N/A', pd.NaT), errors='coerce')
            if (col + '_datetime') in df.columns and df[col + '_datetime'].dtype.name == 'datetime64[ns, UTC]':
                df[col + '_datetime'] = df[col + '_datetime'].dt.tz_localize(None)

    current_time = pd.Timestamp.now()

    def safe_date_diff(date1, date2):
        """Calculate date difference safely"""
        try:
            if pd.notnull(date1) and pd.notnull(date2):
                return (date1 - date2).days
            return 0
        except:
            return 0

    # Calculate all the metrics from previous version using datetime columns
    df['cycle_time'] = df.apply(lambda row: safe_date_diff(row.get('done_datetime_datetime'), row.get('In_Progress_datetime')) + 1 if pd.notnull(row.get('done_datetime_datetime')) and pd.notnull(row.get('In_Progress_datetime')) and row['Current_Status_Category'] == 'Done' else 0, axis=1)
    df['lead_time'] = df.apply(lambda row: safe_date_diff(row.get('done_datetime_datetime'), row.get('Backlog_datetime')) + 1 if pd.notnull(row.get('done_datetime_datetime')) and pd.notnull(row.get('Backlog_datetime')) and row['Current_Status_Category'] == 'Done' else 0, axis=1)
    df['work_item_age'] = df.apply(lambda row: safe_date_diff(current_time, row.get('In_Progress_datetime')) + 1 if pd.notnull(row.get('In_Progress_datetime')) and row['Current_Status_Category'] in ['IN_PROGRESS', 'IN_REVIEW', 'READY_FOR_QA', 'IN_QA'] else 0, axis=1)
    
    def calculate_stage_age(row):
        try:
            stage_column = row['workflow_stage'].replace(' ', '_') + '_datetime'
            if stage_column in row.index and pd.notnull(row[stage_column]) and row['workflow_stage'] in ['In QA', 'Ready for QA', 'In Review', 'In Progress', 'Ready', 'In Refinement', 'To Do']:
                return safe_date_diff(current_time, row[stage_column]) + 1
            return 0
        except:
            return 0

    df['stage_age'] = df.apply(calculate_stage_age, axis=1)
    df['staleness'] = df['Updated_datetime'].apply(lambda x: safe_date_diff(current_time, x) if pd.notnull(x) else 0)
    df['planned'] = df['Issue_Type'].apply(lambda x: 'Planned' if x == 'Story' else 'Unplanned')
    
    # Calculate blocked status and days (enhanced from previous version)
    def calculate_blocked_info(row):
        """Determine if item is blocked and for how long"""
        # Enhanced logic: if item hasn't been updated in 30+ days and not Done, consider blocked
        if pd.notnull(row.get('Updated_datetime')) and row['Current_Status_Category'] != 'Done':
            days_since_update = safe_date_diff(current_time, row['Updated_datetime'])
            if days_since_update > 30:
                return 'Yes', days_since_update
        return 'No', 0

    df[['Blocked', 'Blocked_Days']] = df.apply(
        lambda row: pd.Series(calculate_blocked_info(row)), axis=1
    )
    
    # Properly set done_datetime for items that are Done
    df['done_datetime'] = df.apply(
        lambda row: row['Done'] if row['Current_Status_Category'] == 'Done' and row['Done'] != 'N/A' else 'N/A', 
        axis=1
    )
    
    # Drop the temporary datetime columns
    datetime_cols = [col for col in df.columns if col.endswith('_datetime')]
    df = df.drop(columns=datetime_cols)
    
    print("Metrics computed successfully")
    return df

def save_processed_data(df):
    """Save the processed and flattened data """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Required columns in exact order
    required_columns = [
        'ID', 'Link', 'Title', 'Backlog', 'In_Refinement', 'Ready', 
        'In_Progress', 'In_Review', 'Ready_for_QA', 'In_QA', 'Done', 
        'Current_Status_Category', 'Item_Rank', 'Updated', 'Issue_Type', 
        'Priority', 'Fix_versions', 'Components', 'Assignee', 'Reporter', 
        'Project', 'Resolution', 'Labels', 'Blocked_Days', 'Blocked', 
        'Parent', 'done_datetime'
    ]
    
    # Additional calculated metrics columns (from previous version)
    additional_columns = [
        'stage', 'workflow_stage', 'cycle_time', 'lead_time', 
        'work_item_age', 'stage_age', 'staleness', 'planned'
    ]
    
    # Combine all columns
    all_columns = required_columns + additional_columns
    
    # Reorder columns and ensure all exist
    df_final = df.reindex(columns=all_columns)
    
    # Fill empty cells with 'N/A' for Fix_versions and Components specifically
    df_final['Fix_versions'] = df_final['Fix_versions'].fillna('N/A').replace('', 'N/A')
    df_final['Components'] = df_final['Components'].fillna('N/A').replace('', 'N/A')
    
    # Check for entirely empty columns and fill with 'N/A'
    for column in all_columns:
        if df_final[column].isnull().all():
            df_final[column] = 'N/A'
    
    output_file = os.path.join(OUTPUT_DIR, 'processed_flattened_data.csv')
    df_final.to_csv(output_file, index=False)
    
    print(f"Processed data saved to: {output_file}")
    print(f"Total processed issues: {len(df_final)}")
    print(f"Required columns (27): {required_columns}")
    print(f"Additional calculated metrics (8): {additional_columns}")
    print(f"Total columns: {len(all_columns)}")
    print("Note: Missing columns are represented as 'N/A' strings")

def main():
    """Main function to process historical data"""
    print("Starting historical data processing...")
    print("Note: Data is already pre-filtered by DataPull_hist.py")
    
    # Load historical data (already filtered)
    df = load_historical_data()
    
    if df.empty:
        print("No data found")
        return
    
    # Flatten the data
    df_flattened = flatten_historical_data(df)
    
    # Compute metrics
    df_with_metrics = compute_metrics(df_flattened)
    
    # Save processed data
    save_processed_data(df_with_metrics)
    
    print("Historical data processing completed")
    print("Data transformation (pivot) completed")

if __name__ == "__main__":
    main()