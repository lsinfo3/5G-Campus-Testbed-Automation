import os
import multiprocessing as mp
import pandas as pd
import numpy as np
import yaml


""" Read CSVs in ansible pcap dump. Extract and aggregate data. Write back one parquet file """



ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps_c80/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"

test_configurations = [e.path for e in os.scandir(ansible_dump) if e.is_dir()]
runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]






def handle_ping_run(run_directory: str):
    # TODO: one more layer of redirection: don't aggregate in this function, instead return the prepared df (so it can be used on a per file bases!)
    # TODO: better(?): write combined df to file
    gnb_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("gnb.csv") or f.path.endswith("gnb.csv.gz")]
    if len(gnb_rec) != 1:
        raise ValueError(f"Expected exactly 1 file like 'xyz_gnb.csv[.gz]' but found {len(gnb_rec)}")
    gnb_rec = gnb_rec[0]

    ue_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("ue.csv") or f.path.endswith("ue.csv.gz")]
    if len(ue_rec) != 1:
        raise ValueError(f"Expected exactly 1 file like 'xyz_ue.csv[.gz]' but found {len(ue_rec)}")
    ue_rec = ue_rec[0]



    with open(f"{run_directory}/{os.path.basename(run_directory)}.yaml", "r") as f:
        config = yaml.unsafe_load(f)
        dc = pd.json_normalize(config, sep="__") # flatten config
        config = dc.to_dict(orient='records')[0]
        # if (run_directory.endswith("0")):
        #     for k in config.keys():
        #         print(f"{k}:{config[k]}")
    df_config = pd.DataFrame.from_records([config])

    df_ue = pd.read_csv(ue_rec)
    df_ue["location"] = "ue"
    df_ue["type"] = df_ue.apply(lambda x: "request" if x["SourceIPInner"] != "10.45.0.1" else "response" , axis=1)
    df_ue.sort_values(by=["SeqNum", "type"], ignore_index=True, inplace=True)
    df_ue["delay"] = np.nan
    df_ue["IAT"] = np.nan
    indexer = lambda d:d["type"] == "request"
    df_ue.loc[indexer,"IAT"] = df_ue.loc[indexer,"Timestamp"] - df_ue.loc[indexer,"Timestamp"].shift(1)
    indexer = lambda d:d["type"] == "response"
    df_ue.loc[indexer,"IAT"] = df_ue.loc[indexer,"Timestamp"] - df_ue.loc[indexer,"Timestamp"].shift(1)

    df_gnb = pd.read_csv(gnb_rec)
    df_gnb["location"] = "gnb"
    df_gnb["type"] = df_gnb.apply(lambda x: "request" if x["SourceIPInner"] != "10.45.0.1" else "response" , axis=1)
    df_gnb.sort_values(by=["SeqNum", "type"], ignore_index=True, inplace=True)
    df_gnb["delay"] = np.nan
    df_gnb["IAT"] = np.nan
    indexer = lambda d:d["type"] == "request"
    df_gnb.loc[indexer,"IAT"] = df_gnb.loc[indexer,"Timestamp"] - df_gnb.loc[indexer,"Timestamp"].shift(1)
    indexer = lambda d:d["type"] == "response"
    df_gnb.loc[indexer,"IAT"] = df_gnb.loc[indexer,"Timestamp"] - df_gnb.loc[indexer,"Timestamp"].shift(1)

    min_max_seq = min([int(df_ue["SeqNum"].max()), int(df_gnb["SeqNum"].max())])
    df_gnb= df_gnb[df_gnb["SeqNum"] < min_max_seq]
    print(f"Max: {min_max_seq}")

    len_before = len(df_ue)
    df_ue= df_ue[df_ue["SeqNum"] < min_max_seq]
    len_after = len(df_ue)
    print(f"New length: {len_after}, dropping {len_before-len_after} entries")
    print(df_ue)
    print(df_gnb)

    # TODO: vv
    # drop incomplete sets of packets
    # FIXME: only for icmp!
    # max_seq = max([int(df_ue["SeqNum"].max()), int(df_gnb["SeqNum"].max())])
    # drops = []
    # for i in range(max_seq+1):
    #     if len(df_ue.query(f"SeqNum == {i}")) != 2 or len(df_gnb.query(f"SeqNum == {i}")) != 2:
    #         df_ue.drop(df_ue[df_ue.SeqNum == i].index, inplace = True)
    #         df_gnb.drop(df_gnb[df_gnb.SeqNum == i].index, inplace = True)
    #         drops.append(i)
    # df_ue.reset_index(inplace=True, drop=True)
    # df_gnb.reset_index(inplace=True, drop=True)
    # if len(drops) > 0:
    #     print(f"Dropped {len(drops)} entries from df! {drops}")


    if len(df_ue) != len(df_gnb):
        print("WARNING: Dumping to csv")
        print("WARNING: Dumping to csv")
        print("WARNING: Dumping to csv")
        print("WARNING: Dumping to csv")
        df_ue.to_csv(f"./debug_{os.path.basename(run_directory)}_ue.csv")
        df_gnb.to_csv(f"./debug_{os.path.basename(run_directory)}_gnb.csv")

    # Iterate through both dataframes
    for idx, row in df_ue.iterrows():
        # if row["type"] == "request":
        #     assert(df_gnb.iloc[idx]["SeqNum"] == row["SeqNum"])
        #     assert(df_gnb.iloc[idx]["type"] == row["type"])
        #     delay = df_gnb.iloc[idx]["Timestamp"] - row["Timestamp"]
        # elif row["type"] == "response":
        #     assert(df_gnb.iloc[idx]["SeqNum"] == row["SeqNum"])
        #     assert(df_gnb.iloc[idx]["type"] == row["type"])
        #     delay = row["Timestamp"] - df_gnb.iloc[idx]["Timestamp"]
        # else:
        #     raise ValueError
        if row["type"] == "request":
            gnb_rows = df_gnb.loc[(df_gnb["SeqNum"] == row["SeqNum"]) & (df_gnb["type"] == "request")]
            assert(len(gnb_rows) <= 1)
            if len(gnb_rows) == 1:
                delay = (gnb_rows.head(1)["Timestamp"] - row["Timestamp"]).iloc[0]
            else:
                delay = np.nan
        elif row["type"] == "response":
            gnb_rows = df_gnb.loc[(df_gnb["SeqNum"] == row["SeqNum"]) & (df_gnb["type"] == "response")]
            assert(len(gnb_rows) <= 1)
            if len(gnb_rows) == 1:
                delay = (row["Timestamp"] - gnb_rows.head(1)["Timestamp"]).iloc[0]
            else:
                delay = np.nan
        else:
            raise ValueError
        df_ue.loc[idx, "delay"] = delay

    df_ue.dropna(subset=["delay"], inplace=True)
    # df = pd.concat([df_ue, df_gnb])
    df = pd.concat([df_ue])
    df.to_csv("test.csv", index=False)
    print(df)
    print(df_ue.head())
    print(df_gnb.head())
    print(df.dtypes)


    metrics_default = {
        "delay":np.nan,
        "throughput":np.nan,
        "iat":np.nan,
    }
    relevant_stats = ["min", "max", "mean", "std", "5%", "25%", "50%", "75%", "95%"]
    metrics = {"direction":'XXX'}
    for k,v in metrics_default.items():
        for s in relevant_stats:
            metrics[f"{k}__{s}"] = v

    ret = df.groupby("type").describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])



    for s in relevant_stats:
        metrics[f"delay__{s}"] = ret["delay"][s].loc["request"]
    metrics["direction"] = "Ul"
    ret1 = {**metrics, **config}
    # TODO: Do we have upstream und downstream simultaneously?
    if len(df.loc[df["type"] == "response"]) > 0:
        for s in relevant_stats:
            metrics[f"delay__{s}"] = ret["delay"][s].loc["response"]
        metrics["direction"] = "Dl"
        ret2 = {**metrics, **config}
        print(metrics)
        print("")
        return [ret1, ret2]
    print(metrics)
    print("")
    return ret1






with mp.Pool(8) as p:
    returns = p.map(handle_ping_run, runs)

records = []
for r in returns:
    if isinstance(r,list):
        records.extend(r)
    else:
        records.append(r)


final_df = pd.DataFrame.from_records(records)
print(final_df.columns)
print(final_df.loc[:,["tdd_config__tdd_dl_ul_ratio","tdd_config__tdd_dl_ul_tx_period" ]])
print(final_df)
print(final_df)
print(final_df)

final_df.to_parquet(f"{ansible_dump}/all_runs.parquet")
final_df.to_csv(f"{ansible_dump}/all_runs.csv.gz", compression="gzip")





## def mp_wrapper(infile:str):
##     outfile = infile
##     if outfile.endswith(".gz"):
##         outfile = outfile[:-3]
##     outfile = outfile[:-5] # strip .pcap
##     if infile.endswith("gnb.pcap") or infile.endswith("gnb.pcap.gz"):
##         pp.parse_pcap_gtp(infile=infile, outfile=outfile+".csv", udpport=2152)
##     elif infile.endswith("ue.pcap") or infile.endswith("ue.pcap.gz"):
##         pp.parse_pcap_ip(infile=infile, outfile=outfile+".csv", udpport=2152)
##     else:
##         raise ValueError(f"Wrong pcap format: {infile}")
