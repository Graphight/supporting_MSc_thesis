import pandas as pd

from multiprocessing import Pool
from os import makedirs
from time import time
from tqdm.auto import tqdm


def process_data_by_region(input_dict: dict) -> None:
    try:
        chosen_region = input_dict["chosen_region"]
        chosen_source = input_dict["chosen_source"]
        chosen_col = "precip_monthly_cumulative"

        cols = [
            "grid_cell",
            "month",
            "soi",
            chosen_col
        ]

        filters = [
            ("region", "==", chosen_region),
        ]
        if chosen_source != "VCSN":
            filters.append(("sim", "==", chosen_source))

            file_path = f"../processed_monthly_WaH.parquet"
        else:
            file_path = f"../processed_monthly_VCSN.parquet"

        df = pd.read_parquet(
            file_path,
            engine="pyarrow",
            filters=filters,
            columns=cols
        )

        data = {
            "grid_cell": [],
            "lat": [],
            "lon": [],
            "season": [],
            "num_top_left": [],
            "num_top_right": [],
            "num_bot_left": [],
            "num_bot_right": [],
            "percent_top_left": [],
            "percent_top_right": [],
            "percent_bot_left": [],
            "percent_bot_right": [],
            "nino_balance_index": [],
            "nina_balance_index": [],
            "nino_threshold": [],
            "nina_threshold": []
        }

        for season, months in TIME_PERIODS.items():
            df_season = df.loc[df["month"].isin(months)]

            for grid_cell in df_season["grid_cell"].unique():
                df_grid = df_season.loc[df_season["grid_cell"] == grid_cell]
                # df_grid = df_grid[["soi", chosen_col]].groupby(["soi"]).median().reset_index(drop=False)

                value_threshold = df_grid[chosen_col].median()
                total_count = df_grid.shape[0]

                nino_threshold = df_grid["soi"].quantile(0.25)
                nina_threshold = df_grid["soi"].quantile(0.75)

                df_top_left = df_grid.loc[
                    (df_grid["soi"] <= nino_threshold) &
                    (df_grid[chosen_col] > value_threshold)
                    ]

                df_top_right = df_grid.loc[
                    (df_grid["soi"] >= nina_threshold) &
                    (df_grid[chosen_col] > value_threshold)
                    ]

                df_bot_left = df_grid.loc[
                    (df_grid["soi"] <= nino_threshold) &
                    (df_grid[chosen_col] <= value_threshold)
                    ]

                df_bot_right = df_grid.loc[
                    (df_grid["soi"] >= nina_threshold) &
                    (df_grid[chosen_col] <= value_threshold)
                    ]

                num_top_left = df_top_left.shape[0]
                num_top_right = df_top_right.shape[0]
                num_bot_left = df_bot_left.shape[0]
                num_bot_right = df_bot_right.shape[0]

                percent_top_left = num_top_left / total_count
                percent_top_right = num_top_right / total_count
                percent_bot_left = num_bot_left / total_count
                percent_bot_right = num_bot_right / total_count

                nino_balance_index = round(num_top_left / num_bot_left, 2)
                nina_balance_index = round(num_top_right / num_bot_right, 2)

                lat, lon = grid_cell.split("_")

                data["grid_cell"].append(grid_cell)
                data["lat"].append(float(lat))
                data["lon"].append(float(lon))
                data["season"].append(season)
                data["num_top_left"].append(num_top_left)
                data["num_top_right"].append(num_top_right)
                data["num_bot_left"].append(num_bot_left)
                data["num_bot_right"].append(num_bot_right)
                data["percent_top_left"].append(percent_top_left)
                data["percent_top_right"].append(percent_top_right)
                data["percent_bot_left"].append(percent_bot_left)
                data["percent_bot_right"].append(percent_bot_right)
                data["nino_balance_index"].append(nino_balance_index)
                data["nina_balance_index"].append(nina_balance_index)
                data["nino_threshold"].append(nino_threshold)
                data["nina_threshold"].append(nina_threshold)

        df_data = pd.DataFrame(data)

        # Save the data
        meta_parent = "data_enso_balance_index"
        parent_dir = f"../{meta_parent}/seasonal/region={chosen_region}/source={chosen_source}"
        makedirs(parent_dir, exist_ok=True)
        output_filename = f"{parent_dir}/data.parquet"

        df_data.to_parquet(output_filename)

    except Exception as e:
        print(e)


# ==============================================================
# MAIN LOOP, MULTI-THREADED
POOL_SIZE = 10
TIME_PERIODS = {
    "Summer": ["12", "01", "02"],
    "Autumn": ["03", "04", "05"],
    "Winter": ["06", "07", "08"],
    "Spring": ["09", "10", "11"],
}
POSSIBLE_REGIONS = [
    "Auckland Region",
    "Bay of Plenty Region",
    "Canterbury Region",
    "Gisborne Region",
    "Hawke's Bay Region",
    "Manawatu-Whanganui Region",
    "Marlborough Region",
    "Nelson Region",
    "Northland Region",
    "Otago Region",
    "Southland Region",
    "Taranaki Region",
    "Tasman Region",
    "Waikato Region",
    "Wellington Region",
    "West Coast Region",
]


def process_files(source: str, regions: [str]) -> None:
    print(f"\nI found {len(regions):,} regions, opening the pool [ size: {POOL_SIZE} ]")

    time_start = time()

    input_items = []
    for region in regions:
        input_items.append({"chosen_region": region, "chosen_source": source})

    pool = Pool(processes=POOL_SIZE)
    for _ in tqdm(pool.imap_unordered(process_data_by_region, input_items), total=len(regions)):
        pass    # We just need some dummy statement for the progress bar
    pool.close()

    print(f"Finished processing all {len(regions):,} regions. The pool has been closed.")
    print(f"The whole thing took: {(time() - time_start):,.2f} seconds")


if __name__ == "__main__":
    process_files("ALL", POSSIBLE_REGIONS)
    process_files("NAT", POSSIBLE_REGIONS)
    process_files("VCSN", POSSIBLE_REGIONS)
