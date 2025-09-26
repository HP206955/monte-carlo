import pandas as pd


def get(csv_path="data/jira_issues_historical.csv"):
    df = pd.read_csv(csv_path, parse_dates=["Status Change Date"])
    df = df[df["Issue_Type"].isin(["Story", "Bug", "Defect", "Production Support"])]
    historical_df = df.copy()
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
    print("pivoted_df before merge", pivoted_df)
    pivoted_df = pd.merge(
        historical_df.drop(
            ["Status Change Date", "Status Change From", "Status Change To"], axis=1
        ).drop_duplicates(),
        pivoted_df,
        on="ID",
        how="left",
    )
    print(pivoted_df)
    print(f"Total unique issues after pivot: {len(pivoted_df['ID'].unique())}")
    pivoted_df.to_csv("data/jira_issues_pivoted.csv", index=False)
    return pivoted_df