import pandas as pd
import numpy as np
import random
import datetime


def monte_carlo_simulation(historical_throughput, forecast_days=14, simulations=1000):
    # Placeholder for Monte Carlo simulation logic
    all_forecasts = []
    for sim in range(simulations):
        daily_forecast = []
        for day in range(forecast_days):
            # Sample directly from all historical data (including zeros)
            daily_throughput = random.choice(historical_throughput)
            daily_forecast.append(daily_throughput)
        # Calculate total for this simulation
        total_throughput = sum(daily_forecast)
        all_forecasts.append(total_throughput)
    # Calculate only the requested percentiles
    results = {
        "_70_pt": np.percentile(
            all_forecasts, 30
        ),  # There’s a 70% chance we’ll exceed this number
        "_85_pt": np.percentile(
            all_forecasts, 15
        ),  # There’s an 85% chance we’ll exceed this number
    }
    return results


throughput = pd.read_csv("throughput.csv")
teams = throughput.groupby("team")
release_cadences = pd.read_csv("release_cadences.csv")
periods = release_cadences.groupby("cadence")
team_cadences = {
    "Connect Partner API": "Weekly",
    "Integrations Platform": "Biweekly",
    "Customer Management": "Biweekly",
    "Integrations API": "Biweekly",
    "Mobile": "Biweekly",
    "Order Create": "Biweekly",
    "Personalization": "Biweekly",
    "Products & Pricing": "Biweekly",
    "Integrations Enabling": "Biweekly",
    "Order Management": "Biweekly",
    "Order Submit & Ingest": "Biweekly",
    "Experience Enhancements": "Biweekly",
    "Platform Engineering": "Biweekly",
}

for team_name, group in teams:
    print(f"Team: {team_name}")
    print(group)
    print("\n")
    historical_throughput = group["throughput"].tolist()
    relevant_range = 60
    relevant_ht = historical_throughput[-relevant_range:]
    print(
        f"Relevant historical throughput (last {relevant_range} entries): {relevant_ht}"
    )
    release_date = datetime.datetime.strptime(
        periods.first().loc[team_cadences[team_name], "release_date"], "%Y-%m-%dT%H:%M"
    ).date()
    print(f"Next release date: {release_date}")
    days_until_release = abs(release_date - datetime.date.today()).days
    print(f"Days until next release: {days_until_release}")
    current_forecast = monte_carlo_simulation(
        relevant_ht, forecast_days=days_until_release, simulations=1000
    )
    print(
        f"Forecasted throughput for next {days_until_release} days: {current_forecast}"
    )
    future_forcast = monte_carlo_simulation(
        relevant_ht,
        forecast_days=14 if team_cadences[team_name] == "Biweekly" else 7,
        simulations=1000,
    )
    print(f"Forecasted throughput for next period: {future_forcast}")
