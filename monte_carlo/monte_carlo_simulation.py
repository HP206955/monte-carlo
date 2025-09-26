import random
import numpy as np


def monte_carlo_simulation(historical_throughput, forecast_days=14, simulations=1000):
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
