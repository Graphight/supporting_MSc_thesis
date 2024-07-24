import re

import pandas as pd

from time import time
from multiprocessing import Pool
from os import makedirs
from glob import glob
from tqdm.auto import tqdm


def determine_n_days_to_reach_yearly_precip_percent(
        df: pd.DataFrame,
        grid_cell_dict: dict,
        target_wet_percent: float,
        target_dry_percent: float,
        target_col: str,
        model_name: str,
        region_name: str
) -> (list, list):
    for grid_cell in df["grid_cell"].unique():
        df_cell = df.loc[df["grid_cell"] == grid_cell]
        df_cell = df_cell.sort_values([target_col], ascending=False).reset_index(drop=True)

        total_target = df_cell[target_col].sum()

        df_cell[f"cumulative_{target_col}"] = df_cell[target_col].cumsum()
        df_cell[f"cumulative_{target_col}_percent"] = df_cell[f"cumulative_{target_col}"] / total_target

        df_cell["n_wet"] = df_cell[f"cumulative_{target_col}_percent"] >= target_wet_percent
        df_cell["n_dry"] = df_cell[f"cumulative_{target_col}_percent"] >= target_dry_percent

        grid_cell_dict["region"].append(region_name)
        grid_cell_dict["grid_cell"].append(grid_cell)
        grid_cell_dict["model"].append(model_name)
        grid_cell_dict["wet"].append(int(df_cell[df_cell["n_wet"]].index.min()))
        grid_cell_dict["dry"].append(int(df_cell.shape[0] - df_cell[df_cell["n_dry"]].index.min()))


def generate_fixed_precip_percent_chunk_wah(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]
        year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

        grid_cell_dict = {
            "region": [],
            "grid_cell": [],
            "model": [],
            "wet": [],
            "dry": []
        }

        target_wet_percent = 0.25
        target_dry_percent = 0.75

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        for model_dir in sorted(glob(f"{file}model_tag=*/")):
            model_name = re.findall(r"model_tag=(.*?)/", model_dir)[0]

            determine_n_days_to_reach_yearly_precip_percent(
                df=pd.read_parquet(model_dir, engine="pyarrow", columns=input_cols),
                grid_cell_dict=grid_cell_dict,
                target_wet_percent=target_wet_percent,
                target_dry_percent=target_dry_percent,
                target_col=target_col,
                model_name=model_name,
                region_name=region
            )

        parent_dir = f"{DATA_DIR_WAH_OUTPUT}/year={year}/source={source}"
        makedirs(parent_dir, exist_ok=True)
        file_path = f"{parent_dir}/{region}.parquet"
        df = pd.DataFrame(data=grid_cell_dict)
        df.to_parquet(file_path)

    except Exception as e:
        print(f"Encountered an error: {e}")


def generate_fixed_precip_percent_chunk_vcsn(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]
        year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

        grid_cell_dict = {
            "region": [],
            "grid_cell": [],
            "model": [],
            "wet": [],
            "dry": []
        }

        target_wet_percent = 0.25
        target_dry_percent = 0.75

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        model_name = "OBSERVED"

        determine_n_days_to_reach_yearly_precip_percent(
            df=pd.read_parquet(file, engine="pyarrow", columns=input_cols),
            grid_cell_dict=grid_cell_dict,
            target_wet_percent=target_wet_percent,
            target_dry_percent=target_dry_percent,
            target_col=target_col,
            model_name=model_name,
            region_name=region
        )

        parent_dir = f"{DATA_DIR_VCSN_OUTPUT}/year={year}/source={source}"
        makedirs(parent_dir, exist_ok=True)
        file_path = f"{parent_dir}/{region}.parquet"
        df = pd.DataFrame(data=grid_cell_dict)
        df.to_parquet(file_path)

    except Exception as e:
        print(f"Encountered an error: {e}")


def process_files(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()

    method_to_run = generate_fixed_precip_percent_chunk_vcsn if sim_type == "VCSN" else generate_fixed_precip_percent_chunk_wah

    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(method_to_run, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10

DIR_HAPPI = "/Volumes/T7/ExtremeWeather/Data_WeatherAtHome/HAPPI"
DIR_VCSN = "/Volumes/T7/ExtremeWeather/Data_VCSN"
DATA_DIR_WAH_INPUT = f"{DIR_HAPPI}/Processed/"
DATA_DIR_VCSN_INPUT = f"{DIR_VCSN}/Processed/"
DATA_DIR_WAH_OUTPUT = f"{DIR_HAPPI}/RVI/Chunks_Fixed_PrecipPercent/"
DATA_DIR_VCSN_OUTPUT = f"{DIR_VCSN}/RVI/Chunks_Fixed_PrecipPercent/"

if __name__ == "__main__":
    # process_files("ALL", sorted(glob(f"{DATA_DIR_WAH_INPUT}/year=*/region=*/sim=ALL/")))
    process_files("VCSN", sorted(glob(f"{DATA_DIR_VCSN_INPUT}/year=*/region=*/sim=VCSN/")))
