import re

import pandas as pd
import xarray as xr

from time import time
from multiprocessing import Pool
from os import makedirs, path
from glob import glob
from tqdm.auto import tqdm
from json import load


def process_data_into_regions_wah(file: str) -> None:
    try:
        # Initial admin to determine metadata
        model_tag = re.findall(r"(?:mean|mum)_(.*?)_[0-9]{4}", file)[0]
        sim_type = re.findall(r"raw_data/(.*?)/batch", file)[0]

        # Find all the metadata specifics for the data source
        col_longitude = "global_longitude0"
        col_latitude = "global_latitude0"
        col_time = "time0"
        region_map_file_name = f"{DATA_DIR_SIM}/region_map_WAH_new.json"

        dfs_by_metric = dict()
        model_metrics = [
            "item5216_daily_mean",
            "item3236_daily_maximum",
            "item3236_daily_minimum",
            "item16222_daily_mean"
        ]
        for model_metric in model_metrics:
            file_path = file.replace(model_metrics[0], model_metric)

            # Open the NetCDF4 data and convert to pandas dataframe
            ds_temp = xr.open_dataset(file_path)
            df_temp: pd.DataFrame = ds_temp.to_dataframe()

            # Grab the underlying NAT sub model directly from the NetCDF file_sst attribute
            sub_model_tag = ds_temp.file_sst if sim_type == "NAT" else ""
            ds_temp.close()  # close to save memory

            # Filter dataframe to just the target region
            df_temp: pd.DataFrame = df_temp.loc[
                (df_temp[col_longitude].between(target_lon_start, target_lon_end)) &
                (df_temp[col_latitude].between(target_lat_start, target_lat_end))
                ]
            df_temp = df_temp.reset_index(drop=False)

            # Prepare the dataframes for joins
            df_temp[col_time] = df_temp[col_time].astype(str)
            df_temp["grid_cell"] = df_temp[col_latitude].astype(str) + "_" + df_temp[col_longitude].astype(str)

            dfs_by_metric[model_metric] = df_temp

        # Join them together
        group_cols = ["grid_cell", col_time]
        df_joined = dfs_by_metric[model_metrics[0]]
        for model_metric in model_metrics[1:]:
            df_joined = df_joined.merge(
                right=dfs_by_metric[model_metric][group_cols + [model_metric]],
                on=group_cols,
                how="left"
            )

        # Create a new column for precipitation in mm/day
        df_joined["precip_mm"] = df_joined["item5216_daily_mean"] * 86.65581 * 1000 # convert from kg/m2/s

        # Attach sub model tag to dataframe to help identify later
        df_joined["sub_model_tag"] = sub_model_tag

        # Add the year
        df_joined["year"] = df_joined[col_time].str[:4]
        df_joined["month"] = df_joined[col_time].str[5:7]
        max_year = max(df_joined["year"].unique())

        # Attach regions to each grid_cell
        region_map = load(open(region_map_file_name))
        df_joined["region"] = df_joined["grid_cell"].map(region_map)
        df_joined = df_joined.dropna(subset=["region"]).reset_index(drop=True)

        # Attach the SOI to each model
        df_SOI = pd.read_parquet(f"../wah_soi/method=month/sim={sim_type}/model_tag={model_tag}", columns=["year", "month", "soi"])
        df_joined = df_joined.merge(
            right=df_SOI,
            on=["year", "month"],
            how="left"
        )

        # Loop through each "region" and save the data
        df_joined = df_joined.drop(columns=["z1", "rotated_pole0"])
        for region in sorted(df_joined["region"].unique()):
            df_region = df_joined.loc[df_joined["region"] == region].copy().reset_index(drop=True)

            # Drop partition columns
            df_region = df_region.drop(columns=[
                "year",
                "region"
            ])

            meta_parent = "Processed"
            parent_dir = f"{DATA_DIR_SIM}/{meta_parent}/year={max_year}/region={region}/sim={sim_type}/model_tag={model_tag}"
            makedirs(parent_dir, exist_ok=True)
            output_filename = f"{parent_dir}/data.parquet"

            df_region.to_parquet(output_filename)

    except Exception as e:
        print(e)


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10
COMMON_DIR = "/Volumes/T7/ExtremeWeather"
DATA_DIR_SIM = f"{COMMON_DIR}/Data_WeatherAtHome/climatology_1986-2014"
DATA_DIR_VCSN = f"{COMMON_DIR}/Data_VCSN"

# Roughly New Zealand
nz_lon_start = 166
nz_lon_end = 179
nz_lat_start = -48
nz_lat_end = -34

# Roughly Australia
aus_lon_start = 110
aus_lon_end = 155
aus_lat_start = -45
aus_lat_end = -5

# Determine which region to do
target_lon_start = nz_lon_start
target_lon_end = nz_lon_end
target_lat_start = nz_lat_start
target_lat_end = nz_lat_end


def process_files(sim_type: str, files: [str]) -> None:
    print(f"\nFor {sim_type} forcing I found {len(files):,} files, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()
    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(process_data_into_regions_wah, files), total=len(files)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(files):,} files. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


if __name__ == "__main__":
    # process_files("OBS", sorted(glob(f"{DATA_DIR_VCSN}/by_year/**.parquet")))

    process_files("ALL", sorted(glob(f"{DATA_DIR_SIM}/raw_data/ALL/batch_205/region/item5216_daily_mean/*.nc")))
    process_files("NAT", sorted(glob(f"{DATA_DIR_SIM}/raw_data/NAT/batch_289/region/item5216_daily_mean/*.nc")))
