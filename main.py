from monte_carlo import forecasted_throughput

if __name__ == "__main__":
    forecasted_throughput.get_forcasted_throughput(
        throughput_csv="data/throughput.csv", release_cadences_csv="data/release_cadences.csv"
    )
