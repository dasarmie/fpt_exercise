# Import libraries
import pandas as pd

# Read file and create dataframe
flight = pd.read_csv("src/flight_source.csv"\
                     , delimiter=";"\
                     , parse_dates=["actl_dep_lcl_tms", "actl_arr_lcl_tms", "airborne_lcl_tms", "landing_lcl_tms"])

# print(flight.dtypes)

# Create next_flight_id column using lead window function
flight["next_flight_id"] = flight.sort_values(["actl_dep_lcl_tms"], ascending=True)\
                                                .groupby(["acft_regs_cde"])["id"]\
                                                    .shift(-1)

flight["next_flight_id"] = flight["next_flight_id"].astype(pd.Int32Dtype())

print(flight)

"""
SQL script solution
SELECT *
, lead(id) OVER (
    PARTITION BY acft_regs_cde
    ORDER BY actl_dep_lcl_tms
) next_flight_id
FROM flight_source_table
"""