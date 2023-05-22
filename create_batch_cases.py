from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import os
import subprocess

# inputs
template_path = Path("./case_runner_template")
julia_path = Path("/Applications/Julia-1.8.app/Contents/Resources/julia/bin/julia")
destination_path = Path(".")
ldes_duration_hours = 200
rep_period_lengths = [24,168,8760]
num_rep_periods = [5,30]
ldes_size_mw_base = 1 # size of ldes for the most granular aggregation (will be held constant across aggregations)
pg_output_paths = [
    Path("./pg_output_3z"),
]

# load aggregation data
constituents = pd.read_csv("constituents.csv")

for path in pg_output_paths:
    path_before_z = (str(path)).rpartition('z')[0]
    num_zones = int(path_before_z.split("_")[-1])
    
    # copy case runner template folder into a new folder under destination path with the same name as the PG outputs folder
    destination_case_runner_folder = destination_path / ("case_runner_"+str(num_zones)+"_zone")
    destination = shutil.copytree(template_path, destination_case_runner_folder)

    # copy all input files from PG outputs to destination folder
    for file in (path / "Inputs").glob("*.csv"):
        shutil.copy(file,destination_case_runner_folder / "template")

    # modify Generators_data.csv in place

    generators_data = pd.read_csv(destination_case_runner_folder / "template" / "Generators_data.csv")

    # drop all Hydrogen generators-- we will use the metal air technology as a "generic LDES"
    generators_data.drop(index=generators_data[generators_data.technology.str.contains("Hydrogen")].index,inplace=True)

    metalair_rows = generators_data[generators_data.technology.str.contains("MetalAir")]
    for index in metalair_rows.index:
        zone = metalair_rows.loc[index,"region"]

        # make LDES new build
        generators_data.loc[index,"New_Build"] = 1

        # specify LDES capacity
        for capacity_param in ["Min_Cap_MW","Max_Cap_MW","Min_Charge_Cap_MW","Max_Charge_Cap_MW"]:
            generators_data.loc[index,capacity_param] = "__SPECIAL_"+"LDESCapMW"+zone+"__"

        # change LDES cost to 0
        for cost_param in ["capex_mw","Inv_Cost_per_MWyr","Fixed_OM_Cost_per_MWyr","capex_mwh","Inv_Cost_per_MWhyr"]:
            generators_data.loc[index,cost_param] = 0

        # fix LDES duration
        for duration_param in ["Min_Duration","Max_Duration"]:
            generators_data.loc[index,duration_param] = ldes_duration_hours

    generators_data.to_csv(destination_case_runner_folder / "template" / "Generators_data.csv",index=False)

    # make replacements.csv

    replacements = pd.DataFrame()
    constituents_cur = constituents[constituents.Aggregation==num_zones][["Zone","Constituents"]].set_index("Zone").Constituents

    for length in rep_period_lengths:
        for num_periods in num_rep_periods:
            for incl_ldes in [1,0]:

                if (length != 8760) and (num_periods * length >= 8760):
                    continue

                if length == 8760:
                    replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[0],RepPeriodLengthHours=[0],NumRepPeriods=[0]))
                else:
                    replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[1],RepPeriodLengthHours=[length],NumRepPeriods=[num_periods]))

                for zone in constituents_cur.index:
                    # LDES capacity = number of constituents * size in granular aggregation * boolean for including ldes
                    replacements_cur["LDESCapMW"+zone] = constituents_cur.loc[zone] * ldes_size_mw_base * incl_ldes
                replacements = pd.concat([replacements,replacements_cur],axis=0,ignore_index=True)

    replacements.drop_duplicates(inplace=True)
    replacements["Notes"] = ""
    replacements.index.name="Case"
    replacements.index = replacements.index + 1

    replacements.to_csv(destination_case_runner_folder / "replacements.csv",index=True)

    # run julia code
    os.chdir(destination_case_runner_folder)
    output = subprocess.run([julia_path, "caserunner.jl"])
    os.chdir("..")