import pandas as pd
import datetime
import os
import warnings

# Optimize pandas settings and suppress warnings
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')

# File paths
INPUT_DIR = r'C:\Users\SH207048\OneDrive - abcsupply.com\Desktop\PythonCodes' #change as needed
OUTPUT_DIR = r'C:\Users\SH207048\OneDrive - abcsupply.com\Desktop\PythonCodes' #change as needed
HISTORICAL_FILE = os.path.join(INPUT_DIR, 'historical_data.csv')

# Define relevant project keys for filtering
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
]

def load_historical_data():
    """Load the historical data CSV"""
    print("Loading historical data...")
    try:
        df = pd.read_csv(HISTORICAL_FILE)
        print(f"Loaded {len(df)} historical records")
        return df
    except FileNotFoundError:
        print(f"Error: Historical data file not found at {HISTORICAL_FILE}")
        raise
    except Exception as e:
        print(f"Error loading historical data: {str(e)}")
        raise

def filter_relevant_projects(df):
    """Filter data to only include relevant project keys"""
    print("Filtering for relevant projects...")
    
    if 'Project Key' in df.columns:
        initial_count = len(df)
        df_filtered = df[df['Project Key'].isin(RELEVANT_PROJECT_KEYS)]
        final_count = len(df_filtered)
        print(f"Filtered from {initial_count} to {final_count} records")
        print(f"Relevant projects found: {sorted(df_filtered['Project Key'].unique())}")
        return df_filtered
    else:
        print("Warning: 'Project Key' column not found. Returning original data.")
        return df

def flatten_historical_data(df):
    """Flatten the historical data - convert multiple status entries per issue to single row with timestamps"""
    print("Flattening historical data...")
    
    issue_data = []
    
    for issue_key, group in df.groupby('Issue Key'):
        latest_record = group.sort_values('Status Change Date', ascending=False).iloc[0]
        
        row = {
            'Issue_Key': issue_key,
            'Summary': latest_record.get('Summary', ''),
            'Project': latest_record.get('Project', ''),
            'Project_Key': latest_record.get('Project Key', ''),
            'Issue_Type': latest_record.get('Issue Type', ''),
            'Assignee': latest_record.get('Assignee', ''),
            'Reporter': latest_record.get('Reporter', ''),
            'Priority': latest_record.get('Priority', ''),
            'Created': latest_record.get('Created', ''),
            'Updated': latest_record.get('Updated', ''),
            'Resolution': latest_record.get('Resolution', ''),
            'Fix_Versions': latest_record.get('Fix Versions', ''),
            'Components': latest_record.get('Components', ''),
            'Labels': latest_record.get('Labels', ''),
            'Current_Status': latest_record.get('Status', ''),
        }
        
        status_columns = ['Backlog', 'In_Refinement', 'Ready', 'In_Progress', 'In_Review', 'Ready_for_QA', 'In_QA', 'Done']
        for status in status_columns:
            row[status] = ''
        
        for _, record in group.iterrows():
            status = record.get('Status Changed To', '')
            change_date = record.get('Status Change Date', '')
            
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
        
        issue_data.append(row)
    
    flattened_df = pd.DataFrame(issue_data)
    print(f"Flattened to {len(flattened_df)} unique issues")
    return flattened_df

def compute_metrics(df):
    """Compute additional metrics on the flattened data"""
    print("Computing additional metrics...")
    
    df['stage'] = df['Current_Status'].apply(lambda x: 'In Progress' if x in ['IN_QA', 'READY_FOR_QA', 'IN_REVIEW', 'IN_PROGRESS'] else ('To Do' if x in ['READY', 'IN_REFINEMENT', 'BACKLOG'] else ('Done' if x in ['DONE', 'REJECTED', "WON'T_DO"] else 'Unknown')))
    
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
    df['workflow_stage'] = df['Current_Status'].map(workflow_stage_mapping).fillna('Unknown')

    for col in ['Backlog', 'In_Progress', 'Done', 'Updated']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].dtype.name == 'datetime64[ns, UTC]':
                df[col] = df[col].dt.tz_localize(None)

    current_time = pd.Timestamp.now()

    def safe_date_diff(date1, date2):
        try:
            if pd.notnull(date1) and pd.notnull(date2):
                return (date1 - date2).days
            return 0
        except:
            return 0

    df['cycle_time'] = df.apply(lambda row: safe_date_diff(row['Done'], row['In_Progress']) + 1 if pd.notnull(row['Done']) and pd.notnull(row['In_Progress']) and row['Current_Status'] == 'DONE' else 0, axis=1)
    df['lead_time'] = df.apply(lambda row: safe_date_diff(row['Done'], row['Backlog']) + 1 if pd.notnull(row['Done']) and pd.notnull(row['Backlog']) and row['Current_Status'] == 'DONE' else 0, axis=1)
    df['work_item_age'] = df.apply(lambda row: safe_date_diff(current_time, row['In_Progress']) + 1 if pd.notnull(row['In_Progress']) and row['Current_Status'] in ['IN_PROGRESS', 'IN_REVIEW', 'READY_FOR_QA', 'IN_QA'] else 0, axis=1)
    
    def calculate_stage_age(row):
        try:
            stage_column = row['workflow_stage'].replace(' ', '_')
            if stage_column in row.index and pd.notnull(row[stage_column]) and row['workflow_stage'] in ['In QA', 'Ready for QA', 'In Review', 'In Progress', 'Ready', 'In Refinement', 'To Do']:
                return safe_date_diff(current_time, row[stage_column]) + 1
            return 0
        except:
            return 0

    df['stage_age'] = df.apply(calculate_stage_age, axis=1)
    df['staleness'] = df['Updated'].apply(lambda x: safe_date_diff(current_time, x) if pd.notnull(x) else 0)
    df['planned'] = df['Issue_Type'].apply(lambda x: 'Planned' if x == 'Story' else 'Unplanned')
    
    print("Metrics computed successfully")
    return df

def save_processed_data(df):
    """Save the processed and flattened data"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    output_file = os.path.join(OUTPUT_DIR, 'processed_flattened_data.csv')
    df.to_csv(output_file, index=False)
    
    print(f"Processed data saved to: {output_file}")
    print(f"Total processed issues: {len(df)}")

def main():
    """Main function to process historical data"""
    print("Starting historical data processing...")
    
    # Load historical data
    df = load_historical_data()
    
    # # Filter for relevant projects
    # df_filtered = filter_relevant_projects(df)
    
    # if df_filtered.empty:
    #     print("No relevant project data found. Exiting.")
    #     return
    
    # Flatten the data
    df_flattened = flatten_historical_data(df)
    
    # Compute metrics
    df_with_metrics = compute_metrics(df_flattened)
    
    # Save processed data
    save_processed_data(df_with_metrics)
    
    print("Historical data processing completed!")

if __name__ == "__main__":
    main()

