from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import os
import subprocess
import sys
sys.path.append("/home/gm1710/vrestor-preprocessing/")
import create_vrestor_inputs

# inputs
template_path = Path("/home/gm1710/create_genx_batch_ldes_cases/case_runner_template")
julia_path = Path("/usr/licensed/julia/1.8.2/bin/julia")
destination_path = Path("/scratch/gpfs/gm1710/GenX_cases/LDES_2023_with_zerocarbonCT")
rep_period_lengths = [24,72,168,336,8760]
rep_period_default_length = 168 # will be used for zonal aggregations other than default
num_rep_periods = [5,15,30,45,52,75,100]
num_rep_periods_default = 5 # will be used for zonal aggregations other than default
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
advnuclear_cost_base = 450000 # $/MW-yr including FOM-- with regional cost multiplier = 1
run_colocated = False
run_base_case = False
run_zerocarbonCT = True
run_other_sensitivities = False
default_num_zones=12
run_default_num_zones_only=True
zerocarbon_fuel_cost = 20 # $/mmbtu

# load aggregation data
constituents = pd.read_csv("constituents.csv")

def make_replacements_df(replacements,rep_period_lengths,num_rep_periods,region_to_zone_map,ldes_proportions,advnuclear_cost,advnuclear_maxcap,ldes_size_mw,ldes_duration,batteries_as_ldes,use_LDES_constraints,zerocarbonCTMaxCap):
    for length in rep_period_lengths:
        for num_periods in num_rep_periods:
            if (length != 8760) and (num_periods * length > 8760):
                continue
            if length == 8760:
                replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[0],RepPeriodLengthHours=[0],NumRepPeriods=[0]))
            else:
                replacements_cur = pd.DataFrame(data=dict(UseTimeDomainReduction=[1],RepPeriodLengthHours=[length],NumRepPeriods=[num_periods]))
            for zone_number in region_to_zone_map.values():
                replacements_cur["LDESCapMW"+str(zone_number)] = ldes_size_mw * ldes_proportions[zone_number] 
            replacements_cur["AdvNuclearCostPerMWYr"] = advnuclear_cost
            replacements_cur["AdvNuclearMaxCap"] = advnuclear_maxcap
            replacements_cur["LDESDuration"] = ldes_duration
            replacements_cur["BatteriesAsLDES"] = batteries_as_ldes
            replacements_cur["LDESAsLDES"] = use_LDES_constraints
            replacements_cur["zerocarbonCTMaxCap"] = zerocarbonCTMaxCap
            replacements = pd.concat([replacements,replacements_cur],axis=0,ignore_index=True)
    return replacements

# get current path
home_path = Path(".")

for path in pg_output_paths:
    if run_colocated:
        cases = ["no_colocation","colocation"]
    else:
        cases = ["no_colocation"]
    for case in cases:
        path_before_z = (str(path)).rpartition('z')[0]
        num_zones = int(path_before_z.split("_")[-1])

        if num_zones != default_num_zones and run_default_num_zones_only:
            continue

        # copy case runner template folder into a new folder under destination path
        case_runner_name = "case_runner_"+str(num_zones)+"_zone"
        if run_colocated:
            case_runner_name += "_" + case
        destination_case_runner_folder = destination_path / case_runner_name
        destination = shutil.copytree(template_path, destination_case_runner_folder)

        # copy all input files from PG outputs to destination folder
        for file in (path / "Inputs").glob("*.csv"):
            shutil.copy(file,destination_case_runner_folder / "template")

        # modify Load_data.csv
        load_data = pd.read_csv(destination_case_runner_folder / "template" / "Load_data.csv")
        load_data.loc[0,"Timesteps_per_Rep_Period"] = 8760
        load_data.loc[0,"Sub_Weights"] = 8760
        load_data.to_csv(destination_case_runner_folder / "template" / "Load_data.csv",index=False)

        # modify Fuels_data.csv
        fuels_data = pd.read_csv(destination_case_runner_folder / "template" / "Fuels_data.csv")
        fuels_data["zerocarbon_fuel"]=0
        fuels_data.loc[1:,"zerocarbon_fuel"] = zerocarbon_fuel_cost
        fuels_data.to_csv(destination_case_runner_folder / "template" / "Fuels_data.csv",index=False)

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

        # make classification of batteries as LDES a special parameter
        generators_data.loc[generators_data.technology.str.contains("Batter"),"LDS"] = "__SPECIAL_BatteriesAsLDES__"

        # make classification of LDES as LDES a special parameter
        generators_data.loc[generators_data.technology.str.contains("MetalAir"),"LDS"] = "__SPECIAL_LDESAsLDES__"

        zone_map_cur = constituents[constituents.Aggregation==num_zones][["Zone","Map_3Zone"]].set_index("Zone").Map_3Zone.to_dict()

        # drop existing capacity requirement tags (LDES tags will be added later) 
        mincap_columns = generators_data.columns[generators_data.columns.str.contains("MinCapTag")]
        generators_data.drop(columns=mincap_columns,inplace=True)
        for col_name in ["MinCapTag_1","MinCapTag_2","MinCapTag_3","MaxCapTag_1","MaxCapTag_2","MaxCapTag_3"]:
            generators_data[col_name]=0

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
                generators_data.loc[index,duration_param] = "__SPECIAL_LDESDuration__"

        # modify capacity reserves to be by zone
        capres = generators_data.CapRes_1.copy(deep=True)
        for col_name in ["CapRes_1","CapRes_2","CapRes_3"]:
            generators_data[col_name] = 0
            curzone_mask = generators_data.region.map(zone_map_cur).map(region_to_zone_map) == int(col_name.split("_")[-1])
            generators_data.loc[curzone_mask,col_name] = capres[curzone_mask]

        # output generators_data
        generators_data.to_csv(destination_case_runner_folder / "template" / "Generators_data.csv",index=False)

        # modify Capacity_reserve_margin.csv
        capres = pd.read_csv(destination_case_runner_folder / "template" / "Capacity_reserve_margin.csv")
        capres.columns.values[0] = "region"
        capres_original = capres["CapRes_1"].copy(deep=True)
        for col_name in ["CapRes_1","CapRes_2","CapRes_3"]:
            capres[col_name] = 0
            curzone_mask = capres.region.map(zone_map_cur).map(region_to_zone_map) == int(col_name.split("_")[-1])
            capres.loc[curzone_mask,col_name] = capres_original[curzone_mask]
        capres.to_csv(destination_case_runner_folder / "template" / "Capacity_reserve_margin.csv",index=False)

        # modify Network.csv
        network = pd.read_csv(destination_case_runner_folder / "template" / "Network.csv")
        for capres_num in [2,3]:
            for col_name in ["DerateCapRes_","CapRes_Excl_"]:
                network[col_name+str(capres_num)] = network[col_name+"1"]
        network.to_csv(destination_case_runner_folder / "template" / "Network.csv",index=False)

        if run_colocated:
            if case == "colocation":
                colocated_on_param = True
            elif case == "no_colocation":
                colocated_on_param = False
            else:
                raise ValueError("not a valid case")
            create_vrestor_inputs.convert_case_to_vrestor(
                    case_folder=destination_case_runner_folder / "template",
                    storage_type="LDES",
                    colocated_on=colocated_on_param,
                    zero_out_storage_costs=True
            )

        # part of the generators_data.csv modification must be done after the colocated modifications are being run

        generators_data = pd.read_csv(destination_case_runner_folder / "template" / "Generators_data.csv")
        generators_data.loc[generators_data.technology.str.contains("AdvNuclear"),"Fixed_OM_Cost_per_MWyr"] = 0
        generators_data.loc[generators_data.technology.str.contains("AdvNuclear"),"Inv_Cost_per_MWyr"] = "__SPECIAL_AdvNuclearCostPerMWYr__"
        generators_data.loc[generators_data.technology.str.contains("AdvNuclear"),"Max_Cap_MW"] = "__SPECIAL_AdvNuclearMaxCap__"

        # add CTs with zero carbon fuel and make their availability a special parameter
        CT_rows = generators_data[generators_data.technology.str.contains("NaturalGas_CTAvgCF_Moderate")].copy(deep=True)
        CT_rows["Max_Cap_MW"] = "__SPECIAL_zerocarbonCTMaxCap__"
        CT_rows["Fuel"] = "zerocarbon_fuel"
        CT_rows["Resource"] = CT_rows["Resource"] + "_zerocarbon"
        CT_rows["technology"] = CT_rows["technology"] + "_zerocarbon"
        generators_data = pd.concat([generators_data,CT_rows],axis=0,ignore_index=True)
        
        generators_data.to_csv(destination_case_runner_folder / "template" / "Generators_data.csv",index=False)

        # add availability for CTs with zero carbon fuel (note this depends on the code above)
        generators_variability = pd.read_csv(destination_case_runner_folder / "template" / "Generators_variability.csv")
        for generator in CT_rows.Resource.values:
            generators_variability[generator] = 1
        generators_variability.to_csv(destination_case_runner_folder / "template" / "Generators_variability.csv")

        ### make replacements.csv

        replacements = pd.DataFrame()

        if num_zones != default_num_zones:
            rep_period_lengths_to_use = [rep_period_default_length]
            num_rep_periods_to_use = [num_rep_periods_default]
        else:
            rep_period_lengths_to_use = rep_period_lengths
            num_rep_periods_to_use = num_rep_periods

        # base case
        if run_base_case:
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
        # add CT with zero carbon fuel
        if run_zerocarbonCT:
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=-1)

        if run_other_sensitivities:
            # advanced nuclear cost 25% higher
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base*1.25,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
            # advanced nuclear cost 25% lower
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base*0.75,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
            # no advanced nuclear (w/ batteries as LDES)
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=0,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=1,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
            # different amounts of LDES forced in
            for ldes_size_mw in [100,1000,10000,50000]:
                replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=-1,ldes_size_mw=ldes_size_mw,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
            # different LDES durations
            for ldes_duration in [24,100,200,500]:
                replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=ldes_duration,batteries_as_ldes=0,use_LDES_constraints=1,zerocarbonCTMaxCap=0)
        
            # no LDES constraints
            replacements = make_replacements_df(replacements,rep_period_lengths_to_use,num_rep_periods_to_use,region_to_zone_map,ldes_proportions,advnuclear_cost=advnuclear_cost_base,advnuclear_maxcap=-1,ldes_size_mw=1000,ldes_duration=200,batteries_as_ldes=0,use_LDES_constraints=0,zerocarbonCTMaxCap=0)
        
        replacements.drop_duplicates(inplace=True)
        replacements["Notes"] = ""
        replacements.index.name="Case"
        replacements.index = replacements.index + 1

        replacements.to_csv(destination_case_runner_folder / "replacements.csv",index=True)

        # run julia code
        os.chdir(destination_case_runner_folder)
        # output = subprocess.run([julia_path, "caserunner.jl"])
        os.chdir(home_path)


