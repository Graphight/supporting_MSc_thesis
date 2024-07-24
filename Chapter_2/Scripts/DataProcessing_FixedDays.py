import re

import pandas as pd

from time import time
from multiprocessing import Pool
from os import makedirs
from glob import glob
from tqdm.auto import tqdm
from json import load, dumps


def determine_total_precip_for_each_grid_cell_on_specific_days(
        df: pd.DataFrame,
        grid_cell_dict: dict,
        grid_cell_day_map: dict,
        target_col: str,
        model_name: str,
        region_name: str
) -> (list, list):
    for grid_cell in df["grid_cell"].unique():
        df_cell = df.loc[df["grid_cell"] == grid_cell]
        df_cell = df_cell.sort_values([target_col], ascending=False).reset_index(drop=True)

        target_wet_days = int(grid_cell_day_map[grid_cell]["wet"])
        target_dry_days = df_cell.shape[0] - int(grid_cell_day_map[grid_cell]["dry"] - 1)

        total_precip_wet_days = df_cell.iloc[0:target_wet_days][target_col].sum()
        total_precip_dry_days = df_cell.iloc[target_dry_days:][target_col].sum()

        grid_cell_dict["region"].append(region_name)
        grid_cell_dict["grid_cell"].append(grid_cell)
        grid_cell_dict["model"].append(model_name)
        grid_cell_dict["total_precip_wet_days"].append(total_precip_wet_days)
        grid_cell_dict["total_precip_dry_days"].append(total_precip_dry_days)


def generate_fixed_days_chunk_wah(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]
        year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

        grid_cell_dict = {
            "region": [],
            "grid_cell": [],
            "model": [],
            "total_precip_wet_days": [],
            "total_precip_dry_days": []
        }

        grid_cell_day_map = load(open(f"{DIR_CLIM}/RVI/fixed_precip_grid_cell_map.json"))

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        for model_dir in sorted(glob(f"{file}model_tag=*/")):
            model_name = re.findall(r"model_tag=(.*?)/", model_dir)[0]

            determine_total_precip_for_each_grid_cell_on_specific_days(
                df=pd.read_parquet(model_dir, engine="pyarrow", columns=input_cols),
                grid_cell_dict=grid_cell_dict,
                grid_cell_day_map=grid_cell_day_map,
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


def generate_fixed_days_chunk_vcsn(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]

        grid_cell_dict = {
            "region": [],
            "grid_cell": [],
            "model": [],
            "total_precip_wet_days": [],
            "total_precip_dry_days": []
        }

        grid_cell_day_map = load(open(f"{DIR_VCSN}/RVI/fixed_precip_grid_cell_map.json"))

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        for year_dir in sorted(glob(f"{file.replace('1972', '*')}")):
            year = re.findall(r"year=(.*?)/", year_dir)[0]
            determine_total_precip_for_each_grid_cell_on_specific_days(
                df=pd.read_parquet(year_dir, engine="pyarrow", columns=input_cols),
                grid_cell_dict=grid_cell_dict,
                grid_cell_day_map=grid_cell_day_map,
                target_col=target_col,
                model_name=year,
                region_name=region
            )

        grid_cell_dict["year"] = grid_cell_dict["model"]
        grid_cell_dict["model"] = ["OBSERVED" for _ in range(len(grid_cell_dict["model"]))]

        parent_dir = f"{DATA_DIR_VCSN_OUTPUT}"
        makedirs(parent_dir, exist_ok=True)
        file_path = f"{parent_dir}/{region}.parquet"
        df = pd.DataFrame(data=grid_cell_dict)
        df.to_parquet(file_path)

    except Exception as e:
        print(f"Encountered an error: {e}")


def process_files(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()

    method_to_run = generate_fixed_days_chunk_vcsn if sim_type == "VCSN" else generate_fixed_days_chunk_wah

    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(method_to_run, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10

DIR_CLIM = "/Volumes/T7/ExtremeWeather/Data_WeatherAtHome/climatology_1986-2014"
DIR_VCSN = "/Volumes/T7/ExtremeWeather/Data_VCSN"
DATA_DIR_WAH_INPUT = f"{DIR_CLIM}/Processed/"
DATA_DIR_VCSN_INPUT = f"{DIR_VCSN}/Processed/"
DATA_DIR_WAH_OUTPUT = f"{DIR_CLIM}/RVI/Chunks_Fixed_Days/"
DATA_DIR_VCSN_OUTPUT = f"{DIR_VCSN}/RVI/Chunks_Fixed_Days/"

if __name__ == "__main__":
    process_files("ALL", sorted(glob(f"{DATA_DIR_WAH_INPUT}/year=*/region=*/sim=ALL/")))
    process_files("NAT", sorted(glob(f"{DATA_DIR_WAH_INPUT}/year=*/region=*/sim=NAT/")))
    # process_files("VCSN", sorted(glob(f"{DATA_DIR_VCSN_INPUT}/year=1972/region=*/sim=VCSN/")))
