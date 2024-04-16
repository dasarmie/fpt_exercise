# Import libraries
import pandas as pd
from datetime import datetime, timedelta

# Exercise 1

# Read file and create dataframe
flight = pd.read_csv("src/flight_source.csv"\
                     , delimiter=";"\
                     , parse_dates=["actl_dep_lcl_tms", "actl_arr_lcl_tms", "airborne_lcl_tms", "landing_lcl_tms"])

# Create next_flight_id column using lead window function
flight["next_flight_id"] = flight.sort_values(["actl_dep_lcl_tms"], ascending=True)\
                                                .groupby(["acft_regs_cde"])["id"]\
                                                    .shift(-1)

flight["next_flight_id"] = flight["next_flight_id"].astype(pd.Int32Dtype())

print(flight.sort_values(["acft_regs_cde"], ascending=True))

# Exercise 2

# Define date start regarding our dataset
min_date = flight["actl_dep_lcl_tms"].min()
start_date = datetime(min_date.year, min_date.month, min_date.day, 0, 0, 0)

# Identify the airports uniques in our dataset
airport_id = flight["orig"].unique().tolist()

# Multiply the hours of the day by the fraction per hour that it require 
num_intervals = 4 * 24 

# Create empty list to collect the result in each iteration
date_list = []
airport_name = []

# Iterate for each 15 minutes interval and per airport to create the dataset to be evaluated
for j in range(num_intervals):
    for i in airport_id:
        airport_name.append(i)
        date_list.append(start_date.strftime("%Y-%m-%d %H:%M:%S"))
    start_date += timedelta(minutes=15)

# Create dataframe with iteration results
flight_out_in = pd.DataFrame({"airport_code": airport_name,
                        "tms": date_list})

# Change tms column type to date
flight_out_in["tms"] = pd.to_datetime(flight_out_in["tms"], format='%Y-%m-%d %H:%M:%S')

# Create empty columns to collect the results for each iteration
flight_out_in["out"] = None
flight_out_in["in"] = None

# Iterate the dataframe to identify the date and airport to be evaluated 
for index, row in flight_out_in.iterrows():
    fecha_hora = row["tms"]
    airport_code = row["airport_code"]

    # Filter flight dataframe where the business rule is true and collect result counting the length 
    df_out = flight[(flight["actl_dep_lcl_tms"] >= fecha_hora - timedelta(hours=2))\
                     & (flight["actl_dep_lcl_tms"] <= fecha_hora)\
                     & (flight["orig"] == airport_code)]
    flight_out_in.loc[index, "out"] = len(df_out)

    df_in = flight[(flight["actl_arr_lcl_tms"] >= fecha_hora - timedelta(hours=2))\
                    & (flight["actl_arr_lcl_tms"] <= fecha_hora)\
                    & (flight["dest"] == airport_code)]
    flight_out_in.loc[index, "in"] = len(df_in)

print(flight_out_in[flight_out_in["airport_code"] == "YVR"])