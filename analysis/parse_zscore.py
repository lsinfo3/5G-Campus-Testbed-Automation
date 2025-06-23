import pandas as pd
import numpy as np
from scipy.stats import zscore

import argparse
from multiprocessing import Pool
import time
import os
import json





def build_zscore(path: str):
    stats = { "filename":path, "nonnumeric_val_in_col":[], "zero_val_in_col":[] }
    try:
        df = pd.read_csv(path)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)
        df.drop(columns=[col for col in df.columns if col.endswith("_z")], inplace=True)

        for col in df.columns:
            if col.casefold() == "timestamp".casefold():
                continue
            if len(df) == 0:
                df.columns = df.columns + [f"{col}_z"]
                continue

            # cleanup
            zero_vals_before = len(df.loc[lambda x: x[col] == 0])
            df.loc[lambda x:x[col] == 0, col ] = np.nan    # drop all 0s
            zero_vals_after = len(df.loc[lambda x: x[col] == 0])
            if zero_vals_before != zero_vals_after:
                stats["zero_val_in_col"] += [col]
            nan_before = df.loc[:,col].isna().sum()
            df[col] = pd.to_numeric(df.loc[:,col], errors="coerce")
            nan_after = df.loc[:,col].isna().sum()
            if nan_before != nan_after:
                stats["nonnumeric_val_in_col"] += [col]


            # If all values are identical
            if df.loc[df[col].notnull(), col].nunique() == 1:
                df.loc[df[col].notnull(), f"{col}_z"] = 0.0
            else:
                df.loc[df[col].notnull(), f"{col}_z"] = zscore(df.loc[df[col].notnull(), col])
        df.to_csv(f"{path}")
    except Exception as e:
        raise ValueError(f"Error during execution of '{path}'!") from e
    print(json.dumps(stats))



def main(path: str):
    start = time.time()

    test_configurations = [e.path for e in os.scandir(path) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    metrics_csvs = [os.path.join(r,"gnb_snr.csv") for r in runs] + [os.path.join(r,"modem-snr.csv") for r in runs]
    metrics_csvs =[ "/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/9ff091db__a6f1d465__004/gnb_snr_1.csv" ]
    # pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    with Pool(8) as p:
        p.map(build_zscore, metrics_csvs)

    print(f"Took {time.time()-start}s")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Add zscores to channel metrics",
        description="Scan given dir"
            )
    parser.add_argument("filename")
    args = parser.parse_args()
    ansible_dump = args.filename
    main(ansible_dump)

