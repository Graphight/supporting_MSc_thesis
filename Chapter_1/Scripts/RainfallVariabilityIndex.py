import re

import pandas as pd

from time import time
from multiprocessing import Pool
from os import makedirs
from glob import glob
from tqdm.auto import tqdm


def rainfall_variability_index(
        df: pd.DataFrame,
        record_wet: dict,
        record_dry: dict,
        target_wet_percent: float,
        target_dry_percent: float,
        target_col: str
):
    for grid_cell in df["grid_cell"].unique():
        df_cell = df.loc[df["grid_cell"] == grid_cell]
        df_cell = df_cell.sort_values([target_col], ascending=True).reset_index(drop=True)

        total_target = df_cell[target_col].sum()

        df_cell[f"cumulative_{target_col}"] = df_cell[target_col].cumsum()
        df_cell[f"cumulative_{target_col}_percent"] = df_cell[f"cumulative_{target_col}"] / total_target

        df_cell["cutoff_wet"] = df_cell[f"cumulative_{target_col}_percent"] >= target_wet_percent
        df_cell["cutoff_dry"] = df_cell[f"cumulative_{target_col}_percent"] >= target_dry_percent

        if grid_cell not in record_wet.keys():
            record_wet[grid_cell] = []
            record_dry[grid_cell] = []

        record_wet[grid_cell].append(df_cell.shape[0] - df_cell[df_cell["cutoff_wet"]].index.min())
        record_dry[grid_cell].append(df_cell[df_cell["cutoff_dry"]].index.min())


def determine_final_outcome(
        record_dry: list,
        record_wet: list,
        source: str,
        grid_cell: str
) -> dict:
    wet_mean = round(sum(record_wet) / len(record_wet), 2)
    dry_mean = round(sum(record_dry) / len(record_dry), 2)
    ratio_mean = round(dry_mean / wet_mean, 2)

    wet_median = int(sorted(record_wet)[len(record_wet) // 2])
    dry_median = int(sorted(record_dry)[len(record_dry) // 2])
    ratio_median = round(dry_median / wet_median, 2)

    return {
        "wet_mean": wet_mean,
        "dry_mean": dry_mean,
        "ratio_mean": ratio_mean,
        "wet_median": wet_median,
        "dry_median": dry_median,
        "ratio_median": ratio_median,
        "source": source,
        "grid_cell": grid_cell
    }


def generate_rvi_chunk_wah(file: str) -> None:
    try:
        region = re.findall(r"region=(.*?)/", file)[0]
        source = re.findall(r"sim=(.*?)/", file)[0]
        year = "" if "year" not in file else re.findall(r"year=(.*?)/", file)[0]

        record_wet = dict()
        record_dry = dict()

        target_wet_percent = 0.25
        target_dry_percent = 0.25

        target_col = "precip_mm"
        input_cols = [
            "grid_cell",
            target_col
        ]

        for model_dir in sorted(glob(f"{file}model_tag=*/")):
            rainfall_variability_index(
                df=pd.read_parquet(model_dir, engine="pyarrow", columns=input_cols),
                record_wet=record_wet,
                record_dry=record_dry,
                target_wet_percent=target_wet_percent,
                target_dry_percent=target_dry_percent,
                target_col=target_col
            )

        finaL_outcomes = {
            "wet_mean": [],
            "dry_mean": [],
            "ratio_mean": [],
            "wet_median": [],
            "dry_median": [],
            "ratio_median": [],
            "source": [],
            "grid_cell": []
        }

        for grid_cell in record_wet.keys():
            for key, value in determine_final_outcome(
                    record_dry=record_dry[grid_cell],
                    record_wet=record_wet[grid_cell],
                    source=source,
                    grid_cell=grid_cell
            ).items():
                finaL_outcomes[key].append(value)

        parent_dir = f"{DATA_DIR_SIM_OUTPUT}/year={year}/region={region}"
        makedirs(parent_dir, exist_ok=True)

        df_outcome = pd.DataFrame(finaL_outcomes)
        df_outcome.to_parquet(f"{parent_dir}/outcome_{source}.parquet")

    except Exception as e:
        print(f"Encountered an error: {e}")


def process_files_wah(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} forcing I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()
    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(generate_rvi_chunk_wah, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10
COMMON_DIR = "/Volumes/T7/ExtremeWeather"
DATA_DIR_SIM_INPUT = f"{COMMON_DIR}/Data_WeatherAtHome/HAPPI/Processed"
DATA_DIR_SIM_OUTPUT = f"{COMMON_DIR}/Data_WeatherAtHome/HAPPI/RegionVariability"


if __name__ == "__main__":
    makedirs(DATA_DIR_SIM_OUTPUT, exist_ok=True)

    process_files_wah("ALL", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=ALL/")))
    process_files_wah("HOT", sorted(glob(f"{DATA_DIR_SIM_INPUT}/year=*/region=*/sim=HOT/")))
