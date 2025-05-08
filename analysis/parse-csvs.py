import os
import sys
import multiprocessing as mp
import pandas as pd
import numpy as np
import yaml
import time
import copy
import argparse


""" Read CSVs in ansible pcap dump. Extract and aggregate data. Write back one parquet file """



ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps_c80/"
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps/"
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps_2025-03-28/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"

def calc_channel_metrics(run_directory, relevant_stats):
    assert(os.path.isfile(f"{run_directory}/modem-snr.csv"))
    assert(os.path.isfile(f"{run_directory}/gnb_snr.csv"))




def calc_pkt_metrics(run_directory, relevant_stats, metrics, config):
    print(run_directory)

    gnb_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("gnb.csv") or f.path.endswith("gnb.csv.gz")]
    if len(gnb_rec) != 1:
        raise ValueError(f"Expected exactly 1 file like 'xyz_gnb.csv[.gz]' but found {len(gnb_rec)}; {run_directory}")
    gnb_rec = gnb_rec[0]

    ue_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("ue.csv") or f.path.endswith("ue.csv.gz")]
    if len(ue_rec) != 1:
        raise ValueError(f"Expected exactly 1 file like 'xyz_ue.csv[.gz]' but found {len(ue_rec)}; {run_directory}")
    ue_rec = ue_rec[0]


    df_ue = pd.read_csv(ue_rec)
    df_ue = df_ue.astype({"SeqNum":'int'})
    df_ue["location"] = "ue"
    df_ue["trafficflow"] = df_ue.apply(lambda x: "egress" if x["SourceIPInner"] != "10.45.0.1" else "ingress" , axis=1)
    df_ue.sort_values(by=["SeqNum", "trafficflow"], ignore_index=True, inplace=True)
    df_ue["delay"] = np.nan
    df_ue["IAT"] = np.nan
    df_ue["IDT"] = np.nan
    indexer_egress = lambda d:d["trafficflow"] == "egress"
    indexer_ingress = lambda d:d["trafficflow"] == "ingress"
    df_ue.loc[indexer_egress,"IDT"] = df_ue.loc[indexer_egress,"Timestamp"] - df_ue.loc[indexer_egress,"Timestamp"].shift(1)
    df_ue.loc[indexer_ingress,"IAT"] = df_ue.loc[indexer_ingress,"Timestamp"] - df_ue.loc[indexer_ingress,"Timestamp"].shift(1)

    df_gnb = pd.read_csv(gnb_rec)
    try:
        df_gnb = df_gnb.astype({"SeqNum":'int'})
    except Exception as e:
        print(run_directory)
        print(df_gnb.dtypes)
        df_gnb["SeqNum"] = pd.to_numeric(df_gnb["SeqNum"],errors='coerce')
        df_gnb.to_csv(f"{run_directory}/err_gnb_.csv", index=False)
        df_gnb = df_gnb.dropna()
        df_gnb = df_gnb.astype({"SeqNum":'int'})
        seqnum1 = df_gnb.query("SourceIPInner == '10.45.0.1'")["SeqNum"]
        mini = int(seqnum1.min())
        maxi = int(seqnum1.max())
        print((mini,maxi))
        miss1 = [int(i) for i in range(mini,maxi+1) if int(i) not in seqnum1]
        print(f"len1: {len(miss1)}")
        seqnum2 = df_gnb.query("SourceIPInner != '10.45.0.1'")["SeqNum"]
        mini = int(seqnum2.min())
        maxi = int(seqnum2.max())
        print((mini,maxi))
        miss2 = [int(i) for i in range(mini,maxi+1) if int(i) not in seqnum2]
        print(f"len2: {len(miss2)}")
        seqnum1.to_csv(f"{run_directory}/err_gnb_seqnum1_.csv", index=False)
        seqnum2.to_csv(f"{run_directory}/err_gnb_seqnum2_.csv", index=False)
        raise e

    df_gnb["location"] = "gnb"
    df_gnb["trafficflow"] = df_gnb.apply(lambda x: "ingress" if x["SourceIPInner"] != "10.45.0.1" else "egress" , axis=1)
    df_gnb.sort_values(by=["SeqNum", "trafficflow"], ignore_index=True, inplace=True)
    df_gnb["delay"] = np.nan
    df_gnb["IAT"] = np.nan
    df_gnb["IDT"] = np.nan
    df_gnb.loc[indexer_egress,"IDT"] = df_gnb.loc[indexer_egress,"Timestamp"] - df_gnb.loc[indexer_egress,"Timestamp"].shift(1)
    df_gnb.loc[indexer_ingress,"IAT"] = df_gnb.loc[indexer_ingress,"Timestamp"] - df_gnb.loc[indexer_ingress,"Timestamp"].shift(1)


    # find missing seqnums,
    missing_pkts = 0
    # Only keep packets which are sent by the ue AND received by gnb, or the other way around
    for direction in ["ingress","egress"]:
        direction_complement = lambda x: "ingress" if x == "egress" else "egress"
        seqnums_ue = df_ue.query(f"trafficflow == '{direction}'")["SeqNum"]
        seqnums_gnb = df_gnb.query(f"trafficflow == '{direction_complement(direction)}'")["SeqNum"]
        missing_seqnums = set(seqnums_ue).symmetric_difference(seqnums_gnb)
        # drop rows for the correct direction
        indexes_to_drop = df_ue.loc[(df_ue["SeqNum"].isin(missing_seqnums)) & (df_ue["trafficflow"] == direction)].index
        missing_pkts += len(indexes_to_drop)
        df_ue.drop(indexes_to_drop, inplace=True)
        indexes_to_drop = df_gnb.loc[(df_gnb["SeqNum"].isin(missing_seqnums)) & (df_gnb["trafficflow"] == direction_complement(direction))].index
        missing_pkts += len(indexes_to_drop)
        df_gnb.drop(indexes_to_drop, inplace=True)

        if len(missing_seqnums)> 0:
            print(f"{run_directory}(ue:{direction}) missing: {{min:{min(missing_seqnums)},max:{max(missing_seqnums)},len:{len(missing_seqnums)}}}")
        else:
            continue
        df_ue.to_csv(f"{os.path.basename(run_directory)}__df_ue_.csv")
        df_gnb.to_csv(f"{os.path.basename(run_directory)}__df_gnb_.csv")
        # Verify both dataframes have the same sequence numbers
        ue_seqs = df_ue.query(f"trafficflow == '{direction}'")["SeqNum"]
        gnb_seqs = df_gnb.query(f"trafficflow == '{direction_complement(direction)}'")["SeqNum"]
        # print(ue_seqs.min())
        # print(ue_seqs.max())
        # print(gnb_seqs.min())
        # print(gnb_seqs.max())
        assert(set(ue_seqs).symmetric_difference(set(range(ue_seqs.min(),ue_seqs.max()+1))) == set(gnb_seqs).symmetric_difference(set(range(gnb_seqs.min(),gnb_seqs.max()+1))))

    print(f"dropped {missing_pkts} pkts")
    print(f"len ue: {len(df_ue)}, len gnb: {len(df_gnb)}")
    df_gnb.sort_values(by=["trafficflow", "SeqNum"], ignore_index=True, inplace=True)
    df_ue.sort_values(by=["trafficflow", "SeqNum"], ignore_index=True, inplace=True)
    assert(len(df_ue) == len(df_gnb))
    assert(df_ue["SeqNum"].equals(df_gnb["SeqNum"]))


    # df_ue.loc[(df_ue["trafficflow"] == 'egress'),"delay"] = df_gnb.loc[(df_gnb["trafficflow"] == 'ingress'),"Timestamp"] - df_ue.loc[(df_ue["trafficflow"] == 'egress'),"Timestamp"]
    df_gnb.loc[(df_gnb["trafficflow"] == 'ingress'),"delay"] = df_gnb.loc[(df_gnb["trafficflow"] == 'ingress'),"Timestamp"] - df_ue.loc[(df_ue["trafficflow"] == 'egress'),"Timestamp"]
    df_ue.loc[(df_ue["trafficflow"] == 'ingress'),"delay"] = df_ue.loc[(df_ue["trafficflow"] == 'ingress'),"Timestamp"] - df_gnb.loc[(df_gnb["trafficflow"] == 'egress'),"Timestamp"]



    df_ue.dropna(subset=["delay"], inplace=True)
    df = pd.concat([df_ue, df_gnb]) # INFO: required to accurate determin missing pkts
    # df = pd.concat([df_ue])
    df["SeqNum"] = df["SeqNum"].astype(np.int64)
    with open(f"/tmp/pandas-{os.path.basename(run_directory)}.txt", "w") as f:
        print(df.dtypes, file=f)


    df.to_csv(f"{run_directory}/combined.csv.gz", index=False,compression="gzip")


    percentiles = [0.05, 0.25, 0.5, 0.75, 0.95]
    assert(all([f"{int(p*100)}%" in relevant_stats for p in percentiles]))
    # INFO: determine packet metrics (delay/missing/throughput) at the ingress
    ret = df.groupby("location").describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])

    if config["traffic_config__direction"] == "UlDl":
        # Both Ul and Dl in the same pcaps
        assert(len(df.loc[df["trafficflow"] == "ingress"]) > 0)
        for s in relevant_stats:
            metrics[f"delay__{s}"] = ret["delay"][s].loc["gnb"]
        metrics["direction"] = "Ul"
        df_query = df.query(f"trafficflow == 'ingress' and location == 'gnb'")
        metrics["missing_pkts"] = len( set(range(df_query["SeqNum"].min(numeric_only=True), df_query["SeqNum"].max(numeric_only=True)+1)) .difference(set(df_query["SeqNum"])) )
        ret1 = {**metrics, **config}
        for s in relevant_stats:
            metrics[f"delay__{s}"] = ret["delay"][s].loc["ue"]
        metrics["direction"] = "Dl"
        df_query = df.query(f"trafficflow == 'ingress' and location == 'ue'")
        metrics["missing_pkts"] = len( set(range(df_query["SeqNum"].min(numeric_only=True), df_query["SeqNum"].max(numeric_only=True)+1)) .difference(set(df_query["SeqNum"])) )
        ret2 = {**metrics, **config}
        return [ret1,ret2]
    else:
        # TODO: this is a really bad heuristic?!
        if config["traffic_config__direction"] == "Ul":
            dir_label = "ingress"
            location_label = "gnb"
            df_query = df.query(f"trafficflow == 'ingress' and location == 'gnb'")
        else:
            dir_label = "ingress"
            location_label = "ue"
            df_query = df.query(f"trafficflow == 'ingress' and location == 'ue'")
        print(ret["delay"]["mean"])
        for s in relevant_stats:
            try:
                metrics[f"delay__{s}"] = ret["delay"][s].loc[location_label]
            except Exception as e:
                raise ValueError(f"Can't get metrics for {run_directory} {s}") from e
        # TODO: steamlined throughput calculation
        ts_min = df.query(f"trafficflow   == 'ingress' and location == '{location_label}'")["Timestamp"].min()
        ts_max = df.query(f"trafficflow   == 'ingress' and location == '{location_label}'")["Timestamp"].max()
        pkt_size = df.query(f"trafficflow == 'ingress' and location == '{location_label}'")["PacketSize"].mean()
        amount = len(df.query(f"trafficflow == 'ingress' and location == '{location_label}'"))
        print(f"Mi:{ts_min},Ma:{ts_max},S:{pkt_size},A:{amount}")
        print(f"SeqNum min max")
        print(df_query["SeqNum"].min(numeric_only=True))
        print(df_query["SeqNum"].max(numeric_only=True))
        if df_query["SeqNum"].min(numeric_only=True) < 1 and df_query["SeqNum"].min(numeric_only=True) > 1:
            print("Nan")
        metrics["missing_pkts"] = len( set(range(int(df_query["SeqNum"].min(numeric_only=True)), int(df_query["SeqNum"].max(numeric_only=True))+1)) .difference(set(df_query["SeqNum"])) )
        metrics["throughput__mean"] = amount * pkt_size * 8 /(ts_max - ts_min)
        metrics["direction"] = config["traffic_config__direction"]
        ret1 = {**metrics, **config}
        return ret1



def handle_ping_run(run_directory: str):
    # TODO: 2025-03-31: this function should be more high level. Then it would be easier and more clear how a failed run is handled

    # TODO: one more layer of redirection: don't aggregate in this function, instead return the prepared df (so it can be used on a per file bases!)

    metrics_default = {
        "delay":np.nan,
        "throughput":np.nan,
        "iat":np.nan,
    }
    relevant_stats = ["min", "max", "mean", "std", "5%", "25%", "50%", "75%", "95%"]
    # TODO: missing_pkts
    metrics = {"direction":'XXX',"failed_run":False, "missing_pkts":np.nan}
    for k,v in metrics_default.items():
        for s in relevant_stats:
            metrics[f"{k}__{s}"] = v

    with open(f"{run_directory}/{os.path.basename(run_directory)}.yaml", "r") as f:
        config = yaml.unsafe_load(f)
        dc = pd.json_normalize(config, sep="__") # flatten config
        config = dc.to_dict(orient='records')[0]

    faulty_run = os.path.isfile(f"{run_directory}/FAILED")
    fault_reason = ""

    if not faulty_run:
        return calc_pkt_metrics(run_directory=run_directory, metrics=metrics, relevant_stats=relevant_stats, config=config)
    else:
        with open(f"{run_directory}/FAILED", "r") as ff:
            fault_reason = ff.read().strip().strip("\n")
        # TODO: how do i know if there is up-&downstream?
        metrics["direction"]="Ul"
        metrics["failed_run"]=True
        ret1 = {**metrics, **config}
        ret2 = copy.deepcopy(ret1)
        ret2["direction"]="Dl"
        if config["traffic_config__direction"] == "UlDl":
            return [ret1,ret2]
        elif config["traffic_config__direction"] == "Ul":
            return [ret1]
        else:
            return [ret2]


def refactor_final_df(df: pd.DataFrame):
    # df["gnb_version__combined"] = df["gnb_version__version"] + df["gnb_version__commit"]
    # df["gnb_version__combined"] = df.apply(lambda x: x["gnb_version__version"] + str(x["gnb_version__commit"])[:7])
    df["gnb_version__combined"] = df["gnb_version__version"] + "__" + df["gnb_version__commit"].str.slice(0,7)
    return df



def handle_run(run_directory: str):
    dirname = os.path.basename(run_directory)
    run_config_path = f"{run_directory}/{dirname}.yaml"
    print(run_config_path)

    with open(run_config_path, "r") as rf:
        run_config = yaml.safe_load(rf)

    traffic_type = run_config["traffic_config"]["traffic_type"]

    if traffic_type == "scapyudpping":
        return handle_ping_run(run_directory)
    elif traffic_type == "iperfthroughput":
        # WARN: when changing this step: double check FAILED runs are marked correctly
        # return handle_throughput_run(run_directory)
        return handle_ping_run(run_directory)
    else:
        raise RuntimeError(f"Unknown traffic type: {traffic_type}")






def main():
    start = time.time()

    test_configurations = [e.path for e in os.scandir(ansible_dump) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    # handle_run("../ansible/dumps/f353745a/f353745a__c9d26484__001")
    # handle_run("../ansible/dumps/f353745a/f353745a__06e1f169__000")
    # if True:
    #     sys.exit(1)

# runs = runs[:1]

    print(runs)
    with mp.Pool(8) as p:
        # returns = p.map(handle_ping_run, runs)
        returns = p.map(handle_run, runs)
    # returns = [handle_run(r) for r in runs]

    records = []
    for r in returns:
        if isinstance(r,list):
            records.extend(r)
        else:
            records.append(r)


    final_df = pd.DataFrame.from_records(records)
# aggregate final df
    final_df = refactor_final_df(final_df)


    print(final_df)



    final_df.to_parquet(f"{ansible_dump}/all_runs.parquet")
    final_df.to_csv(f"{ansible_dump}/all_runs.csv.gz", compression="gzip")
    print(f"Took {time.time()-start}s")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Evaluate packet recordings in csvs",
        description="Scan given dir and"
            )
    parser.add_argument("filename")
    args = parser.parse_args()
    ansible_dump = args.filename
    main()





