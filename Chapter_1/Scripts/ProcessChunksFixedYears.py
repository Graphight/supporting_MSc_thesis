import re

import numpy as np
import pandas as pd

from glob import  glob
from multiprocessing import Pool
from os import makedirs
from time import time
from tqdm.auto import tqdm


def determine_total_precip_for_each_grid_cell_on_specific_days(
        df: pd.DataFrame,
        region_dict: dict,
        target_col: str,
        model_name: str,
        region_name: str,
        source_name: str
) -> (list, list):
    total_precip_wet_days = []
    total_precip_mid_days = []
    total_precip_dry_days = []

    for grid_cell in df["grid_cell"].unique():
        df_cell = df.loc[df["grid_cell"] == grid_cell]
        df_cell = df_cell.sort_values([target_col], ascending=False).reset_index(drop=True)

        target_wet_days = 5
        target_dry_days = 360 - 300 - 1

        total_precip_wet_days.append(df_cell.iloc[0:target_wet_days][target_col].sum())
        total_precip_mid_days.append(df_cell.iloc[target_wet_days:target_dry_days][target_col].sum())
        total_precip_dry_days.append(df_cell.iloc[target_dry_days:][target_col].sum())

    region_dict["total_precip_wet_days"].append(np.median(total_precip_wet_days))
    region_dict["total_precip_mid_days"].append(np.median(total_precip_mid_days))
    region_dict["total_precip_dry_days"].append(np.median(total_precip_dry_days))
    region_dict["region"].append(region_name)
    region_dict["model"].append(model_name)
    region_dict["source"].append(source_name)


def generate_fixed_days_chunk_wah(file: str) -> None:
    region = re.findall(r"region=(.*?)/", file)[0]
    source = re.findall(r"sim=(.*?)/", file)[0]
    year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

    region_dict = {
        "region": [],
        "model": [],
        "source": [],
        "total_precip_wet_days": [],
        "total_precip_mid_days": [],
        "total_precip_dry_days": []
    }

    target_col = "precip_mm"
    input_cols = [
        "grid_cell",
        target_col
    ]

    for model_dir in sorted(glob(f"{file}model_tag=*/")):
        model_name = re.findall(r"model_tag=(.*?)/", model_dir)[0]

        determine_total_precip_for_each_grid_cell_on_specific_days(
            df=pd.read_parquet(model_dir, engine="pyarrow", columns=input_cols),
            region_dict=region_dict,
            target_col=target_col,
            model_name=model_name,
            region_name=region,
            source_name=source
        )

    parent_dir = f"{DATA_DIR_SIM_OUTPUT}/year={year}"
    makedirs(parent_dir, exist_ok=True)
    file_path = f"{parent_dir}/{region}.parquet"
    df = pd.DataFrame(data=region_dict)
    df.to_parquet(file_path)


def process_files_wah(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} forcing I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()

    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(generate_fixed_days_chunk_wah, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10

DIR_HAPPI = "/Volumes/T7/ExtremeWeather/Data_WeatherAtHome/HAPPI"
DATA_DIR_SIM_INPUT = f"{DIR_HAPPI}/Processed/"
DATA_DIR_SIM_OUTPUT = f"{DIR_HAPPI}/Data_Paper2/Chunks_Fixed_Days_5_300/"

if __name__ == "__main__":
    process_files_wah("ALL", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=ALL/")))
    process_files_wah("HOT", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=HOT/")))
