import pandas as pd
import datetime
from . import monte_carlo_simulation

def get_raw_forecasted_throughput(
    throughput_csv="throughput.csv",
    release_cadences_csv="release_cadences.csv",
    relevant_range=60,
    simulations=1000,
):
    throughput = pd.read_csv(throughput_csv)
    release_cadences = pd.read_csv(release_cadences_csv)
    teams = throughput.groupby("team")
    periods = release_cadences.groupby("cadence")
    is_biweekly_team = {
        "Connect Partner API": 0,
        "Integrations Platform": 1,
        "Customer Management": 1,
        "Integrations API": 1,
        "Mobile": 1,
        "Order Create": 1,
        "Personalization": 1,
        "Products & Pricing": 1,
        "Integrations Enabling": 1,
        "Order Management": 1,
        "Order Submit & Ingest": 1,
        "Experience Enhancements": 1,
        "Platform Engineering": 1,
    }
    forecast = []
    for team_name in is_biweekly_team.keys():
        release_date = datetime.datetime.strptime(
            periods.first().loc[
                "Biweekly" if is_biweekly_team[team_name] else "Weekly", "release_date"
            ],
            "%Y-%m-%dT%H:%M",
        ).date()
        print(f"Next release date: {release_date}")
        days_until_release = abs(release_date - datetime.date.today()).days
        print(f"Days until next release: {days_until_release}")
        if team_name not in teams.groups:
            forecast.append(
                [
                    team_name,
                    0,
                    0,
                    days_until_release,
                    0,
                ]
            )
            continue
        group = teams.get_group(team_name)
        group = group.sort_values(by="date_day", ascending=False)
        print(f"Team: {team_name}")
        print(group)
        print("\n")
        historical_throughput = group["throughput"].tolist()
        relevant_ht = historical_throughput[:relevant_range]
        print(
            f"Relevant historical throughput (last {relevant_range} entries): {relevant_ht}"
        )
        current_forecast = monte_carlo_simulation.simulates(
            relevant_ht, forecast_days=days_until_release, simulations=simulations
        )
        future_forecast = monte_carlo_simulation.simulates(
            relevant_ht,
            forecast_days=7 * (is_biweekly_team[team_name] + 1),
            simulations=simulations,
        )
        forecast.append(
            [
                team_name,
                int(future_forecast["_85_pt"]),
                int(future_forecast["_70_pt"]),
                days_until_release,
                int(current_forecast["_85_pt"]),
            ]
        )
    return forecast


def get_forcasted_throughput(
    relevant_range=60,
    throughput_csv="throughput.csv",
    release_cadences_csv="release_cadences.csv",
):
    future_forecast = get_raw_forecasted_throughput(
        relevant_range=relevant_range,
        throughput_csv=throughput_csv,
        release_cadences_csv=release_cadences_csv,
    )
    print(f"Forecasted throughput for next release: {future_forecast}")

    df = pd.DataFrame(
        future_forecast,
        columns=[
            "team_name",
            "_85_pt",
            "_70_pt",
            "days_until_release",
            "current_period_forecast",
        ],
    )
    df.sort_values(by="_85_pt", inplace=True)
    return df
