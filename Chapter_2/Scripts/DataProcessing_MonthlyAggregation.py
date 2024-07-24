import re

import pandas as pd

from time import time
from multiprocessing import Pool
from os import makedirs, path
from glob import glob
from tqdm.auto import tqdm


def process_data_by_month_wah(file: str) -> None:
    try:
        # Initial admin to determine metadata
        year = re.findall(r"year=(.*?)/", file)[0]
        region = re.findall(r"region=(.*?)/", file)[0]
        sim_type = re.findall(r"sim=(.*?)/", file)[0]
        model_tag = re.findall(r"model_tag=(.*?)/", file)[0]

        group_cols = [
            "grid_cell",
            "region",
            "sim",
            "model_tag",
            "sub_model_tag",
            "year",
            "month"
        ]

        metric_cols = [
            "precip_mm",
            "item16222_daily_mean",
            "soi"
        ]

        df_temp = pd.read_parquet(file, engine="pyarrow")
        df_temp["year"] = year
        df_temp["region"] = region
        df_temp["sim"] = sim_type
        df_temp["model_tag"] = model_tag
        df_temp["month"] = df_temp["time0"].str.slice(start=5, stop=7)

        for col in group_cols:
            df_temp[col] = df_temp[col].astype(str)

        # Get mean of core metrics - non-land-masked
        df_grouped = df_temp[group_cols + metric_cols].groupby(group_cols).agg(["mean"])
        df_grouped.columns = ["_".join([str(index) for index in multi_index]) for multi_index in df_grouped.columns]
        df_grouped.rename(columns={
            "item16222_daily_mean_mean": "mslp_monthly_mean",
            "soi_mean": "soi",
            "precip_mm_mean": "precip_monthly_mean"
        }, inplace=True)

        # Get cumulative monthly precip
        df_grouped["precip_monthly_cumulative"] = df_temp[group_cols + ["precip_mm"]].groupby(group_cols).sum()

        # Get mean of temperature - land-masked
        df_temp = df_temp.dropna()
        df_grouped["t_max_monthly_mean"] = df_temp[group_cols + ["item3236_daily_maximum"]].groupby(group_cols).mean()
        df_grouped["t_min_monthly_mean"] = df_temp[group_cols + ["item3236_daily_minimum"]].groupby(group_cols).mean()

        # Round them all
        for col in [
            "mslp_monthly_mean",
            "precip_monthly_mean",
            "precip_monthly_cumulative",
            "t_max_monthly_mean",
            "t_min_monthly_mean"
        ]:
            df_grouped[col] = df_grouped[col].astype(float)
        df_grouped = df_grouped.round(2).reset_index(drop=False)

        # Drop partition columns
        cols_to_drop = []
        for col in [
            "year",
            "region",
            "sim",
            "model_tag"
        ]:
            if col in df_grouped.columns:
                cols_to_drop.append(col)
        df_grouped = df_grouped.drop(columns=cols_to_drop)

        # Save the data
        meta_parent = "Processed_Monthly"
        parent_dir = f"{DATA_DIR_SIM}/{meta_parent}/year={year}/region={region}/sim={sim_type}/model_tag={model_tag}"
        makedirs(parent_dir, exist_ok=True)
        output_filename = f"{parent_dir}/data.parquet"

        df_grouped.to_parquet(output_filename)

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
    process_files("ALL", sorted(glob(f"{DATA_DIR_SIM}/Processed/year=*/region=*/sim=ALL/model_tag=*/data.parquet")))
    process_files("NAT", sorted(glob(f"{DATA_DIR_SIM}/Processed/year=*/region=*/sim=NAT/model_tag=*/data.parquet")))
