import re

import pandas as pd

from time import time
from multiprocessing import Pool
from glob import glob
from tqdm.auto import tqdm


def process_data_by_month_wah(file: str) -> None:
    try:
        # Initial admin to determine metadata
        year = re.findall(r"year=(.*?)/", file)[0]
        region = re.findall(r"region=(.*?)/", file)[0]
        sim_type = re.findall(r"sim=(.*?)/", file)[0]
        model_tag = re.findall(r"model_tag=(.*?)/", file)[0]

        # Read in both datasets
        df = pd.read_parquet(file)
        df_soi = pd.read_parquet(f"../wah_soi/method=month/sim={sim_type}/model_tag={model_tag}/")

        # Apply group cols
        df["year"] = year
        df["month"] = df["time0"].str.slice(start=5, stop=7)

        # Replace the old SOI
        group_cols = ["year", "month"]
        df = df.drop(columns=["soi"])
        df = df.merge(
            right=df_soi[group_cols + ["soi"]],
            on=group_cols,
            how="left"
        )

        # Drop partition columns
        cols_to_drop = []
        for col in [
            "year",
            "region",
            "sim",
            "model_tag"
        ]:
            if col in df.columns:
                cols_to_drop.append(col)
        df = df.drop(columns=cols_to_drop)

        # Save the data
        parent_dir = f"{DATA_DIR_SIM}/Processed/year={year}/region={region}/sim={sim_type}/model_tag={model_tag}"
        df.to_parquet(f"{parent_dir}/data.parquet")

    except Exception as e:
        print(e)


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10
COMMON_DIR = "/Volumes/T7/ExtremeWeather"
DATA_DIR_SIM = f"{COMMON_DIR}/Data_WeatherAtHome/climatology_1986-2014"


def process_files(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} forcing I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()
    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(process_data_by_month_wah, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


if __name__ == "__main__":
    process_files("ALL", sorted(glob(f"{DATA_DIR_SIM}/Processed/year=*/region=*/sim=*/model_tag=*/data.parquet")))
    process_files("NAT", sorted(glob(f"{DATA_DIR_SIM}/Processed/year=*/region=*/sim=*/model_tag=*/data.parquet")))
