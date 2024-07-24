import re

import pandas as pd

from time import time
from multiprocessing import Pool
from os import makedirs
from glob import glob
from tqdm.auto import tqdm
from json import load, dumps


def determine_total_precip_for_each_grid_cell_on_rolling_rx_window(
        df: pd.DataFrame,
        grid_cell_dict: dict,
        rolling_window: int,
        target_col: str,
        model_name: str,
        region_name: str
) -> (list, list):
    rolling_target_col = f"{target_col}_Rx{ROLLING_WINDOW}day"

    for grid_cell in df["grid_cell"].unique():
        df_cell = df.loc[df["grid_cell"] == grid_cell].copy()

        df_cell[rolling_target_col] = df_cell[target_col].rolling(rolling_window).sum()
        df_cell.dropna(subset=[rolling_target_col], axis=0, inplace=True)
        df_cell = df_cell.sort_values([rolling_target_col], ascending=False).reset_index(drop=True)

        total_precip_wet_days = df_cell.iloc[0:1][rolling_target_col].sum()
        total_precip_dry_days = df_cell.iloc[-1][rolling_target_col].sum()

        grid_cell_dict["region"].append(region_name)
        grid_cell_dict["grid_cell"].append(grid_cell)
        grid_cell_dict["model"].append(model_name)
        grid_cell_dict["total_precip_wet_days"].append(total_precip_wet_days)
        grid_cell_dict["total_precip_dry_days"].append(total_precip_dry_days)


def generate_fixed_rx_chunk_wah(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]
        year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

        grid_cell_dict = {
            "region": [],
            "grid_cell": [],
            "model": [],
            "total_precip_wet_days": [],
            "total_precip_dry_days": [],
        }

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        for model_dir in sorted(glob(f"{file}model_tag=*/")):
            model_name = re.findall(r"model_tag=(.*?)/", model_dir)[0]

            determine_total_precip_for_each_grid_cell_on_rolling_rx_window(
                df=pd.read_parquet(model_dir, engine="pyarrow", columns=input_cols),
                grid_cell_dict=grid_cell_dict,
                rolling_window=ROLLING_WINDOW,
                target_col=target_col,
                model_name=model_name,
                region_name=region
            )

        parent_dir = f"{DATA_DIR_SIM_OUTPUT}/year={year}/source={source}/window={ROLLING_WINDOW}"
        makedirs(parent_dir, exist_ok=True)
        file_path = f"{parent_dir}/{region}.parquet"
        df = pd.DataFrame(data=grid_cell_dict)
        df.to_parquet(file_path)

    except Exception as e:
        print(f"Encountered an error: {e}")


def process_files_wah(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} forcing I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()

    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(generate_fixed_rx_chunk_wah, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10

DIR_HAPPI = "/Volumes/T7/ExtremeWeather/Data_WeatherAtHome/HAPPI"
DATA_DIR_SIM_INPUT = f"{DIR_HAPPI}/Processed/"
DATA_DIR_SIM_OUTPUT = f"{DIR_HAPPI}/RVI/Chunks_Fixed_RX/"

ROLLING_WINDOW = 7

if __name__ == "__main__":
    process_files_wah("ALL", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=ALL/")))
    process_files_wah("HOT", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=HOT/")))
