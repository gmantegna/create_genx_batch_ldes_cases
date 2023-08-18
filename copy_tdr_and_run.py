import pandas as pd
from pathlib import Path
import subprocess
import os
import shutil
p = Path("/scratch/gpfs/gm1710/GenX_cases/LDES_082023")
num_base_cases = 24
last_case = 1140

for case_runner_folder in p.iterdir():
    print("case runner folder is {}".format(case_runner_folder.name))
    replacements = pd.read_csv(case_runner_folder / "replacements.csv",index_col=0)
    replacements_base = replacements.loc[:num_base_cases,:].copy(deep=True)
    for case_number, row in replacements.iterrows():
        if (case_number <= 24):
            continue
        print("processing case {}".format(case_number))
        if (row["UseTimeDomainReduction"] == 1) and (case_number <= 1140):
            # find matching case from base cases
            case_index = replacements_base[
                    (replacements_base.RepPeriodLengthHours==row["RepPeriodLengthHours"])
                    & (replacements_base.NumRepPeriods==row["NumRepPeriods"])
            ].index
            if len(case_index) > 1:
                raise ValueError("too many matching cases found")
            else:
                matching_case = int(case_index[0])

            # copy TDR results from matching case-- deleting destination folder if it exists
            tdr_source = case_runner_folder / "Cases" / ("case_"+str(matching_case)) / "TDR_Results"
            tdr_destination = case_runner_folder / "Cases" / ("case_"+str(case_number)) / "TDR_Results"
            if tdr_destination.exists():
                shutil.rmtree(tdr_destination)
            if not tdr_source.exists():
                raise ValueError("TDR_Results folder in source does not exist.")
            shutil.copytree(tdr_source,tdr_destination)

            # fix fuel cost if it needs to be fixed
            if row["ZeroCarbonFuelCost"] != 20:
                fuels_data = pd.read_csv(tdr_destination / "Fuels_data.csv")
                fuel_cost = row["ZeroCarbonFuelCost"]
                fuels_data.loc[1:,"zerocarbon_fuel"] = fuel_cost
                fuels_data.to_csv(tdr_destination / "Fuels_data.csv",index=False)

        # run case
        owd = os.getcwd()
        os.chdir(case_runner_folder / "Cases" / ("case_"+str(case_number)))
        result = subprocess.run(["sbatch", "jobscript.sh"],capture_output=True,text=True)
        print(result.stdout)
        os.chdir(owd)
    print("Done processing {}.".format(case_runner_folder.name))
print("Done processing all case runner folders.")


