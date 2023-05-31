from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import os
import subprocess

# inputs
template_path = Path("/home/gm1710/create_genx_batch_ldes_cases/case_runner_template")
julia_path = Path("/usr/licensed/julia/1.8.2/bin/julia")
destination_path = Path("/scratch/gpfs/gm1710/GenX_cases/LDES_2023")
ldes_duration_hours = 200
rep_period_lengths = [24,168]#[24,72,168,336,8760]
num_rep_periods = [5,30]#[5,15,30,45,60]
ldes_size_mw_total = 1000 #MW of LDES forced in across all zones
ldes_proportions = { # how total LDES is allocated to each meta region (fractions are fraction of total nationwide peak load in load data) 
        1: 0.676,
        2: 0.105,
        3: 0.219,
}
pg_output_paths = [
    Path("/home/gm1710/Real_Conus_Aggs/results_26z/2045/t52nr_2045_52_week,_no_reduction"),
    Path("/home/gm1710/Real_Conus_Aggs/results_22z/2045/t52nr_2045_52_week,_no_reduction"),
    Path("/home/gm1710/Real_Conus_Aggs/results_17z/2045/t52nr_2045_52_week,_no_reduction"),
    Path("/home/gm1710/Real_Conus_Aggs/results_12z/2045/t52nr_2045_52_week,_no_reduction"),
    Path("/home/gm1710/Real_Conus_Aggs/results_7z/2045/t52nr_2045_52_week,_no_reduction"),
    Path("/home/gm1710/Real_Conus_Aggs/results_3z/2045/t52nr_2045_52_week,_no_reduction"),
]
region_to_zone_map = {
        "EIC": 1,
        "TRE": 2,
        "WECC": 3,
}

# load aggregation data
constituents = pd.read_csv("constituents.csv")

# get current path
home_path = Path(".")

for path in pg_output_paths:
    path_before_z = (str(path)).rpartition('z')[0]
    num_zones = int(path_before_z.split("_")[-1])
    
    # copy case runner template folder into a new folder under destination path with the same name as the PG outputs folder
    destination_case_runner_folder = destination_path / ("case_runner_"+str(num_zones)+"_zone")
    destination = shutil.copytree(template_path, destination_case_runner_folder)

    # copy all input files from PG outputs to destination folder
    for file in (path / "Inputs").glob("*.csv"):
        shutil.copy(file,destination_case_runner_folder / "template")

    # modify Load_data.csv
    load_data = pd.read_csv(destination_case_runner_folder / "template" / "Load_data.csv")
    load_data.loc[0,"Timesteps_per_Rep_Period"] = 8760
    load_data.loc[0,"Sub_Weights"] = 8760
    load_data.to_csv(destination_case_runner_folder / "template" / "Load_data.csv",index=False)

    # modify CO2_cap.csv
    CO2_cap = pd.read_csv(destination_case_runner_folder / "template" / "CO2_cap.csv",index_col=0)
    CO2_cap["CO_2_Cap_Zone_1"] = 1
    CO2_cap.to_csv(destination_case_runner_folder / "template" / "CO2_cap.csv",index=True)

    # add min and max capacity requirement csv's
    min_cap_req = pd.DataFrame(data={
        "MinCapReqConstraint":[1,2,3],
        "Constraint_Description":["LDES_1","LDES_2","LDES_3"],
        "Min_MW":["__SPECIAL_LDESCapMW1__","__SPECIAL_LDESCapMW2__","__SPECIAL_LDESCapMW3__"]
    })
    min_cap_req.to_csv(destination_case_runner_folder / "template" / "Minimum_capacity_requirement.csv",index=False)
    max_cap_req = min_cap_req.copy(deep=True)
    max_cap_req = max_cap_req.rename(columns={"MinCapReqConstraint":"MaxCapReqConstraint","Min_MW":"Max_MW"})
    max_cap_req.to_csv(destination_case_runner_folder / "template" / "Maximum_capacity_requirement.csv",index=False)

    # modify Generators_data.csv in place

    generators_data = pd.read_csv(destination_case_runner_folder / "template" / "Generators_data.csv")

    # drop all Hydrogen generators-- we will use the metal air technology as a "generic LDES"
    generators_data.drop(index=generators_data[generators_data.technology.str.contains("Hydrogen")].index,inplace=True)

    # drop retrofit generators-- causing bugs and won't get picked in these cases
    generators_data.drop(index=generators_data[generators_data.RETRO == 1].index,inplace=True)

    # drop existing capacity requirement tags and add LDES capacity requirement tags
    mincap_columns = generators_data.columns[generators_data.columns.str.contains("MinCapTag")]
    generators_data.drop(columns=mincap_columns,inplace=True)
    for col_name in ["MinCapTag_1","MinCapTag_2","MinCapTag_3","MaxCapTag_1","MaxCapTag_2","MaxCapTag_3"]:
        generators_data[col_name]=0

    zone_map_cur = constituents[constituents.Aggregation==num_zones][["Zone","Map_3Zone"]].set_index("Zone").Map_3Zone.to_dict()

    metalair_rows = generators_data[generators_data.technology.str.contains("MetalAir")]
    for index in metalair_rows.index:
        zone = metalair_rows.loc[index,"region"]

        # make LDES new build
        generators_data.loc[index,"New_Build"] = 1

        # set appropriate cap requirement tag to 1
        for col_name in ["MinCapTag_","MaxCapTag_"]:
            generators_data.loc[index,col_name+str(region_to_zone_map[zone_map_cur[zone]])] = 1

        # turn off capacity requirement constraints by resource
        for capacity_param in ["Min_Cap_MW","Max_Cap_MW","Min_Charge_Cap_MW","Max_Charge_Cap_MW"]:
            generators_data.loc[index,capacity_param] = -1

        # change LDES cost to 0
        for cost_param in ["capex_mw","Inv_Cost_per_MWyr","Fixed_OM_Cost_per_MWyr","capex_mwh","Inv_Cost_per_MWhyr"]:
            generators_data.loc[index,cost_param] = 0

        # fix LDES duration
        for duration_param in ["Min_Duration","Max_Duration"]:
            generators_data.loc[index,duration_param] = ldes_duration_hours

    generators_data.to_csv(destination_case_runner_folder / "template" / "Generators_data.csv",index=False)

    # make replacements.csv

    replacements = pd.DataFrame()

    for length in rep_period_lengths:
        for num_periods in num_rep_periods:
            for incl_ldes in [1]:#[1,0]:

                if (length != 8760) and (num_periods * length >= 8760):
                    continue

                if length == 8760:
                    replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[0],RepPeriodLengthHours=[0],NumRepPeriods=[0]))
                else:
                    replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[1],RepPeriodLengthHours=[length],NumRepPeriods=[num_periods]))

                for zone_number in region_to_zone_map.values():
                    # LDES capacity = total LDES * proportion allocated to current zone * LDES boolean
                    replacements_cur["LDESCapMW"+str(zone)] = ldes_size_mw_total * ldes_proportions[zone_number] * incl_ldes
                replacements = pd.concat([replacements,replacements_cur],axis=0,ignore_index=True)

    replacements.drop_duplicates(inplace=True)
    replacements["Notes"] = ""
    replacements.index.name="Case"
    replacements.index = replacements.index + 1

    replacements.to_csv(destination_case_runner_folder / "replacements.csv",index=True)

    # run julia code
    os.chdir(destination_case_runner_folder)
    output = subprocess.run([julia_path, "caserunner.jl"])
    os.chdir(home_path)
