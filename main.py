from monte_carlo import forecasted_throughput
from jira_connector import get_historical_data, get_pivoted_data

if __name__ == "__main__":
    # get_historical_data.get()
    get_pivoted_data.get()
    forecasted_throughput.get_forcasted_throughput(
        throughput_csv="data/throughput.csv",
        release_cadences_csv="data/release_cadences.csv",
    )
