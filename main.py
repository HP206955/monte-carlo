from monte_carlo import forecasted_throughput
from jira_connector import get_historical_data, get_pivoted_data

if __name__ == "__main__":
    # get_historical_data.get()
    pivoted_df = get_pivoted_data.get()
    pivoted_df.to_csv("data/jira_issues_pivoted.csv", index=False)

    forecasted_df = forecasted_throughput.get_forcasted_throughput(
        throughput_csv="data/throughput.csv",
        release_cadences_csv="data/release_cadences.csv",
    )
    forecasted_df.to_csv("data/raw_format_dual.csv", index=False)
