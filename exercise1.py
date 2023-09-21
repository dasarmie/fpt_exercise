import pandas as pd

# Create dataframe
flight = pd.read_csv('src/flight_source.csv'\
                     , delimiter=";"\
                     , parse_dates=["actl_dep_lcl_tms", "actl_arr_lcl_tms", "airborne_lcl_tms", "landing_lcl_tms"])

print(flight.dtypes)

