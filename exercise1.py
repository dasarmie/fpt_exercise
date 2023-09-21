# Import libraries
import pandas as pd

# Create dataframe
flight = pd.read_csv('src/flight_source.csv'\
                     , delimiter=";"\
                     , parse_dates=["actl_dep_lcl_tms", "actl_arr_lcl_tms", "airborne_lcl_tms", "landing_lcl_tms"])

# print(flight.dtypes)

# Create next_flight_id column using lead window function
flight["next_flight_id"] = flight.sort_values(["acft_regs_cde", "actl_dep_lcl_tms"], ascending=[True, True])\
                                                .groupby(["acft_regs_cde"])["id"]\
                                                    .shift(-1)

flight["next_flight_id"] = flight["next_flight_id"].astype(pd.Int32Dtype())

print(flight)