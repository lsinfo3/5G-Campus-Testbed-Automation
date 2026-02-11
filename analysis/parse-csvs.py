import os
import sys
import io
import multiprocessing as mp
import pandas as pd
import numpy as np
import yaml
import time
import copy
import argparse
import gzip
import tarfile

SKIP_EXISTING = False

"""
Read CSVs in ansible pcap dump. Extract and aggregate data.
Also include additional data sources, like power consumption or perf counters.
Write back one parquet file.
"""



ansible_dump = ""


def _get_power_metrics(run_directory, start, end, empty=False):
    ret = {
            "ue_power":np.nan,      # milli Watt
            "ue_current":np.nan,    # milli Amp
            "ue_voltage":np.nan,   # milli Volt
            "sdr_power":np.nan,
            "sdr_current":np.nan,
            "sdr_voltage":np.nan,
            }

    for msm_location in ["ue", "sdr"]:
        csv = f"{run_directory}/power_{msm_location}.csv.gz"
        if empty:
            return ret
        if not os.path.isfile(csv) or os.path.getsize(csv)<=200:
            print(f"File does not exist!!! {csv}")
            time.sleep(5)
            ret[f"{msm_location}_power"] = np.nan
            ret[f"{msm_location}_current"] = np.nan
            ret[f"{msm_location}_voltage"] = np.nan
            continue

        df = pd.read_csv(csv)
        try:
            df = df.loc[(df["TIME"] < end) & (df["TIME"] > start),["VAL","TYPE"]]
        except BaseException as e:
            print(f"csv: {csv}")
            print(f"end: {end}({type(end)}); start: {start}({type(start)})")
            print(f"dftpes: {df.dtypes}")
            raise e

        if len(df) == 0:
            return ret
        ret[f"{msm_location}_power"] = df.groupby("TYPE").mean().loc["power", "VAL"]
        ret[f"{msm_location}_current"] = df.groupby("TYPE").mean().loc["current", "VAL"]
        ret[f"{msm_location}_voltage"] = df.groupby("TYPE").mean().loc["voltage", "VAL"]
        del df
    return ret

def _get_channel_metrics(run_directory, start, end, empty=False):
    ret = {
            "modem_snr": np.nan,
            "modem_sinr": np.nan,
            "modem_rsrp": np.nan,
            "modem_rsrq": np.nan,
            "gnb_snr": np.nan,
            "gnb_cqi": np.nan,
            "gnb_rsrp": np.nan,
            "gnb_mcs_dl": np.nan,
            "gnb_mcs_ul": np.nan,
            }
    csv_modem = f"{run_directory}/snr_ue.csv.gz"
    csv_gnb = f"{run_directory}/snr_gnb.csv.gz"

    # TODO: this
    if empty:
        return ret
    if not (os.path.isfile(csv_modem)):
        raise ValueError(f"Missing modem channel metrics for '{run_directory}'")
    df_modem = pd.read_csv(csv_modem)
    if not (os.path.isfile(csv_gnb)):
        raise ValueError(f"Missing gnb channel metrics for '{run_directory}'")
    df_gnb = pd.read_csv(csv_gnb)

    try:
        for cm in df_modem.columns:
            df_modem[cm] = pd.to_numeric(df_modem[cm], errors='coerce')
        for cm in df_gnb.columns:
            df_gnb[cm] = pd.to_numeric(df_gnb[cm], errors='coerce')

        df_modem = df_modem.loc[(df_modem["TIMESTAMP"] < end) & (df_modem["TIMESTAMP"] > start), : ]
        df_gnb = df_gnb.loc[(df_gnb["TIMESTAMP"] < end) & (df_gnb["TIMESTAMP"] > start), : ]
    except BaseException as e:
        print(f"dir: {run_directory}")
        print(f"end: {end}({type(end)}); start: {start}({type(start)})")
        print(f"dftpes: {df_modem.dtypes}")
        raise e

    if len(df_modem) > 10:
        ret["modem_snr"] = df_modem["SNR"].mean()
        ret["modem_sinr"] = df_modem["SINR"].mean()
        ret["modem_rsrp"] = df_modem["RSRP"].mean()
        ret["modem_rsrq"] = df_modem["RSRQ"].mean()
    if len(df_gnb) > 10:
        ret["gnb_snr"] = df_gnb["SNR"].mean()
        ret["gnb_cqi"] = df_gnb["CQI"].mean()
        ret["gnb_rsrp"] = df_gnb["RSRP"].mean()
        ret["gnb_mcs_dl"] = df_gnb["MCS_DL"].mean()
        ret["gnb_mcs_ul"] = df_gnb["MCS_UL"].mean()
    del df_modem
    del df_gnb
    return ret

def _parse_perf_csv(filepath: str):
    with gzip.open(filepath, "rt") as f:
        content = f.read()
    content_s = content.split("\n")
    content_ss = [ [ w.replace(",","") for w in l.split(" ") if w != "" and not w.startswith("(")] for l in content_s if "<not counted>" not in l]
    content_sd = [ l if "#" not in l else l[:l.index("#") ] for l in content_ss ]
    content_csv_l = [ ";".join(l) for l in content_sd ]
    content_csv = "\n".join(content_csv_l)
    df = pd.read_csv(io.StringIO(content_csv), sep=";", names=["Timestamp", "Value", "Metric"])
    df["Value"].astype(int)
    return df

def _get_perf_counters(run_directory, start, end, raw_values=False):
    run_config_path = f"{run_directory}/{os.path.basename(run_directory)}.yaml"
    with open(run_config_path, "r") as rf:
        run_config = yaml.safe_load(rf)

    timeoffset_str = 0
    if isinstance(run_config.get("gnb_version"), dict) and run_config.get("gnb_version").get("type") == "srsRAN":
        # print("Type srsRAN")
        with tarfile.open(f"{run_directory}/artefacts.tar.gz", "r:gz") as tar:
            member = tar.getmember("artefacts/srsran")
            extracted = tar.extractfile(member)
            assert(extracted != None)
            gnb_log_content = extracted.read().decode('utf-8')
    elif isinstance(run_config.get("gnb_version"), dict) and run_config.get("gnb_version").get("type") == "OAI":
        # print("Type OAI")
        with gzip.open(f"{run_directory}/gnb.log.gz", "rt") as rf:
            gnb_log_content = rf.read()
    else:
        raise ValueError(f"Invalid traffic configuration for run '{run_directory}'")

    gnb_log_lines = gnb_log_content.split("\n")
    idxs = [ i for i,l in enumerate(gnb_log_lines) if "Events enabled" in l ] # Indicates perf start; bash utility 'ts' applied timestamp in stdout before this message
    # print(len(idxs))
    if len(idxs) != 1 :
        raise ValueError(f"Did perf really start? '{run_directory}'")
    timeoffset_str = gnb_log_lines[idxs[0]].split(" ")[0]
    timeoffset = float(timeoffset_str)

    perfstats = f"{run_directory}/perf_gnb.csv.gz"
    if os.path.isfile(perfstats):
        perf_metrics = _parse_perf_csv(perfstats)
    elif os.path.isfile(perfstats[:-3]):
        perf_metrics = _parse_perf_csv(perfstats[:-3])
    else:
        raise ValueError(f"Perfstats not found for run '{run_directory}'")
    perf_metrics["Timestamp"] = perf_metrics["Timestamp"] + timeoffset
    perf_metrics = perf_metrics.pivot(index="Timestamp", columns="Metric", values="Value").reset_index().rename_axis(None, axis="columns")
    # INFO: columns:  Timestamp cache-misses      cycles dTLB-load-misses instructions

    perf_metrics = perf_metrics.loc[(perf_metrics["Timestamp"] < end) & (perf_metrics["Timestamp"] > start), : ]
    if raw_values:
        return perf_metrics

    ret = { }
    for m in ["cycles","instructions", "cache-misses", "dTLB-load-misses"]:
        # ret[f"perf_{m.strip("-")}"] = perf_metrics[m].mean()
        ret[f"perf_{m.strip("-")}"] = perf_metrics[m].cumsum().max() / ( perf_metrics['Timestamp'].max()-perf_metrics['Timestamp'].min() )
    del perf_metrics
    return ret






def calc_pkt_metrics(run_directory, relevant_stats, metrics, config):
    log_report = f"{run_directory}\n"

    gnb_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("tcpdump_gnb.csv") or f.path.endswith("tcpdump_gnb.csv.gz")]
    if len(gnb_rec) != 1:
        raise ValueError(f"Expected exactly 1 file like 'xyz_gnb.csv[.gz]' but found {len(gnb_rec)}; {run_directory}")
    gnb_rec = gnb_rec[0]

    ue_rec = [f.path for f in os.scandir(run_directory) if f.path.endswith("tcpdump_ue.csv") or f.path.endswith("tcpdump_ue.csv.gz")]
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
        log_report += f"{df_gnb.dtypes}\n"
        df_gnb["SeqNum"] = pd.to_numeric(df_gnb["SeqNum"],errors='coerce')
        df_gnb.to_csv(f"{run_directory}/err_gnb_.csv", index=False)
        df_gnb = df_gnb.dropna()
        df_gnb = df_gnb.astype({"SeqNum":'int'})
        seqnum1 = df_gnb.query("SourceIPInner == '10.45.0.1'")["SeqNum"]
        mini = int(seqnum1.min())
        maxi = int(seqnum1.max())
        log_report += f"1:{mini},{maxi}\n"
        miss1 = [int(i) for i in range(mini,maxi+1) if int(i) not in seqnum1]
        log_report += f"len1: {len(miss1)}\n"
        seqnum2 = df_gnb.query("SourceIPInner != '10.45.0.1'")["SeqNum"]
        mini = int(seqnum2.min())
        maxi = int(seqnum2.max())
        log_report += f"2:{mini},{maxi}\n"
        miss2 = [int(i) for i in range(mini,maxi+1) if int(i) not in seqnum2]
        log_report += f"len2: {len(miss2)}\n"
        seqnum1.to_csv(f"{run_directory}/err_gnb_seqnum1_.csv", index=False)
        seqnum2.to_csv(f"{run_directory}/err_gnb_seqnum2_.csv", index=False)
        print(log_report)
        raise e

    df_gnb["location"] = "gnb"
    df_gnb["trafficflow"] = df_gnb.apply(lambda x: "ingress" if x["SourceIPInner"] != "10.45.0.1" else "egress" , axis=1)
    df_gnb.sort_values(by=["SeqNum", "trafficflow"], ignore_index=True, inplace=True)
    df_gnb["delay"] = np.nan
    df_gnb["IAT"] = np.nan
    df_gnb["IDT"] = np.nan
    df_gnb.loc[indexer_egress,"IDT"] = df_gnb.loc[indexer_egress,"Timestamp"] - df_gnb.loc[indexer_egress,"Timestamp"].shift(1)
    df_gnb.loc[indexer_ingress,"IAT"] = df_gnb.loc[indexer_ingress,"Timestamp"] - df_gnb.loc[indexer_ingress,"Timestamp"].shift(1)
    if config["traffic_config__traffic_type"]!="idle" and ((len(df_ue)==0 and len(df_gnb)>0)  or (len(df_ue)>0 and len(df_gnb)==0)) :
        # INFO: this run is faulty; return without truthy value to mark failed
        log_report += f"Faulty run bcause of df len: {len(df_ue)} != {len(df_gnb)}\n"
        print(log_report)
        return False


    # find missing seqnums,
    missing_pkts = 0
    # Only keep packets which are sent by the ue AND received by gnb, or the other way around
    df_gnb = df_gnb.drop_duplicates(subset=['trafficflow', 'SeqNum'])
    df_ue = df_ue.drop_duplicates(subset=['trafficflow', 'SeqNum'])
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
            log_report += f"{run_directory}(ue:{direction}) missing: {{min:{min(missing_seqnums)},max:{max(missing_seqnums)},len:{len(missing_seqnums)}}}\n"
        else:
            continue
        # df_ue.to_csv(f"{os.path.basename(run_directory)}__df_ue_.csv")
        # df_gnb.to_csv(f"{os.path.basename(run_directory)}__df_gnb_.csv")

        # Verify both dataframes have the same sequence numbers
        ue_seqs = df_ue.query(f"trafficflow == '{direction}'")["SeqNum"]
        gnb_seqs = df_gnb.query(f"trafficflow == '{direction_complement(direction)}'")["SeqNum"]

        try:
            assert(set(ue_seqs).symmetric_difference(set(range(ue_seqs.min(),ue_seqs.max()+1))) == set(gnb_seqs).symmetric_difference(set(range(gnb_seqs.min(),gnb_seqs.max()+1))))
        except Exception as e:
            log_report += f"ASSERTIONERROR: run_directory: {run_directory}\n"
            log_report += f"ASSERTIONERROR: ue_seqs: {ue_seqs}\n"
            log_report += f"ASSERTIONERROR: gnb_seqs: {gnb_seqs}\n"
            print(log_report)
            raise e

    if missing_pkts > 0:
        print(f"dropped {missing_pkts} pkts")
        print(f"len ue: {len(df_ue)}, len gnb: {len(df_gnb)}")
    df_gnb.sort_values(by=["trafficflow", "SeqNum"], ignore_index=True, inplace=True)
    df_ue.sort_values(by=["trafficflow", "SeqNum"], ignore_index=True, inplace=True)
    try:
        assert(len(df_ue) == len(df_gnb))
    except AssertionError as ae:
        log_report += f"ERROR: {run_directory}, df_ue:{len(df_ue)} == df_gnb:{len(df_gnb)}\n"
        print(log_report)
        raise ae

    # Try to split the dataframes (along 'trafficflow') and use SeqNum as index
    df_gnb_ingress = df_gnb[df_gnb['trafficflow'] == 'ingress'].copy()
    df_gnb_egress = df_gnb[df_gnb['trafficflow'] == 'egress'].copy()
    df_ue_ingress = df_ue[df_ue['trafficflow'] == 'ingress'].copy()
    df_ue_egress = df_ue[df_ue['trafficflow'] == 'egress'].copy()
    # Use SeqNum as index from now on
    for d in [df_gnb_ingress,df_gnb_egress,df_ue_ingress,df_ue_egress]:
        d.loc[:,"SeqNum"] = d["SeqNum"].astype(np.int64)
        d.set_index("SeqNum", inplace=True, verify_integrity=True)

    df_gnb_ingress.loc[:,"delay"] = df_gnb_ingress["Timestamp"] - df_ue_egress["Timestamp"]
    df_ue_ingress.loc[:,"delay"] = df_ue_ingress["Timestamp"] - df_gnb_egress["Timestamp"]

    ## TODO: this calculates the batch times per percentile, which is a bit excessive
    ## df_gnb_ingress.loc[:, "IBT_gnb"] = df_gnb_ingress["IAT"].apply(lambda x : np.nan if x < 0.001 else x)
    ## df_gnb_ingress["IBT_ue"] = np.nan
    ## df_ue_ingress["IBT_gnb"] = np.nan
    ## df_ue_ingress.loc[:, "IBT_ue"] = df_ue_ingress["IAT"].apply(lambda x : np.nan if x < 0.001 else x)
    ## #-
    ## # df_ue.loc[indexer_ingress,"IAT"] = df_ue.loc[indexer_ingress,"Timestamp"] - df_ue.loc[indexer_ingress,"Timestamp"].shift(1)
    ## df_ue_ingress['batch_id'] = (df_ue_ingress['IAT'] > 0.001).cumsum()
    ## df_batches_ue = (df_ue_ingress
    ##                  .groupby(['batch_id'])['Timestamp']
    ##                  .agg(lambda x : x.iloc[0])
    ##                  .reset_index()
    ##                  .rename({"Timestamp":"batch_start"}, axis='columns')
    ##                  .assign(location="ue")
    ##                  )
    ## df_batches_ue["batch_stop"] = df_ue_ingress.groupby(['batch_id'])['Timestamp'].agg(lambda x : x.iloc[-1]).reset_index()["Timestamp"]
    ## df_batches_ue["BD"] = df_batches_ue["batch_stop"] - df_batches_ue["batch_start"]
    ## df_batches_ue["BT"] = df_batches_ue["batch_start"] - df_batches_ue["batch_start"].shift(1)
    ## df_batches_ue["IBT"] = df_batches_ue["batch_start"] - df_batches_ue["batch_stop"].shift(1)
    ## df_batches_ue["IBT_old_count"] = df_ue_ingress.groupby(['batch_id'])["IBT_ue"].count()
    ## df_batches_ue["IBT_old"] = df_ue_ingress.groupby(['batch_id'])["IBT_ue"].mean()
    ## #-
    ## df_gnb_ingress['batch_id'] = (df_gnb_ingress['IAT'] > 0.001).cumsum()
    ## df_batches_gnb = (df_gnb_ingress
    ##                  .groupby(['batch_id'])['Timestamp']
    ##                  .agg(lambda x : x.iloc[0])
    ##                  .reset_index()
    ##                  .rename({"Timestamp":"batch_start"}, axis='columns')
    ##                  .assign(location="gnb")
    ##                  )
    ## df_batches_gnb["batch_stop"] = df_gnb_ingress.groupby(['batch_id'])['Timestamp'].agg(lambda x : x.iloc[-1]).reset_index()["Timestamp"]
    ## df_batches_gnb["BD"] = df_batches_gnb["batch_stop"] - df_batches_gnb["batch_start"]
    ## df_batches_gnb["BT"] = df_batches_gnb["batch_start"] - df_batches_gnb["batch_start"].shift(1)
    ## df_batches_gnb["IBT"] = df_batches_gnb["batch_start"] - df_batches_gnb["batch_stop"].shift(1)
    ## df_batches = pd.concat([df_batches_ue, df_batches_gnb])
    ## df_batches.to_csv(f"{run_directory}/batches.csv.gz", index=False, compression="gzip")





    #df_ue.dropna(subset=["delay"], inplace=True)
    df = pd.concat([df_ue_ingress.reset_index(), df_gnb_ingress.reset_index()]) # INFO: required to accurate determin missing pkts
    dfcomplete = pd.concat([df_ue_ingress.reset_index(), df_gnb_ingress.reset_index(), df_ue_egress.reset_index(), df_gnb_egress.reset_index()]) # INFO: required to accurate determin missing pkts


    # df = pd.concat([df_ue])
    # df["SeqNum"] = df["SeqNum"].astype(np.int64)

    # with open(f"/tmp/pandas-{os.path.basename(run_directory)}.txt", "w") as f:
    #     print(df.dtypes, file=f)


    df.to_csv(f"{run_directory}/combined.csv.gz", index=False,compression="gzip")
    dfcomplete.to_csv(f"{run_directory}/complete.csv.gz", index=False,compression="gzip")


    percentiles = [0.05, 0.25, 0.5, 0.75, 0.95]
    assert(all([f"{int(p*100)}%" in relevant_stats for p in percentiles]))
    # INFO: determine packet metrics (delay/missing/throughput) at the ingress
    ret = df.groupby("location").describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])


    try:
        channel_msm = _get_channel_metrics(run_directory=run_directory, start=df["Timestamp"].min(), end=df["Timestamp"].max())
        if not np.isnan(channel_msm["modem_snr"]):
            metrics["ue_channelmetrics_failed"] = False
        if not np.isnan(channel_msm["gnb_snr"]):
            metrics["gnb_channelmetrics_failed"] = False
        metrics = { **metrics, **channel_msm }

        if config["traffic_config__traffic_type"] == "idle":
            with tarfile.open(f"{run_directory}/artefacts.tar.gz", "r:gz") as tar:
                member = tar.getmember("artefacts/setip")
                extracted = tar.extractfile(member)
                assert(extracted != None)
                setip_content = extracted.read().decode('utf-8')
                setip_content_lines = setip_content.split("\n")
                setip_success_line = [ l for l in setip_content_lines if "New IP: " in l]
                if len(setip_success_line) != 1:
                    raise ValueError("Failed run?")
                else:
                    ts = float(setip_success_line[0].split(" ")[0])
                    power_msm_results = _get_power_metrics(run_directory=run_directory, start=ts, end=ts+config["traffic_config__traffic_duration"])
                    gnb_perf_results = _get_perf_counters(run_directory=run_directory, start=ts, end=ts+config["traffic_config__traffic_duration"])
                metrics["actualduration"] = config["traffic_config__traffic_duration"]
        else:
            power_msm_results = _get_power_metrics(run_directory=run_directory, start=df["Timestamp"].min(), end=df["Timestamp"].max())
            gnb_perf_results = _get_perf_counters(run_directory=run_directory, start=df["Timestamp"].min(), end=df["Timestamp"].max())
            metrics["actualduration"] = df["Timestamp"].max() - df["Timestamp"].min()
            if metrics["actualduration"] < 0.9 * config["traffic_config__traffic_duration"]:
                return False
        metrics = { **metrics, **power_msm_results }
        metrics[f"ue_power_failed"] = np.isnan(power_msm_results["ue_power"])
        metrics[f"sdr_power_failed"] = np.isnan(power_msm_results["sdr_power"])

        metrics = { **metrics, **gnb_perf_results }

        # WARN: what layer are we talking about?
        metrics["pkt_size"] = 1376 if config["traffic_config__traffic_type"] == "iperfthroughput" else 36 if config["traffic_config__size"]=="small" else 1382


    except BaseException as e:
        raise ValueError(f"Error for: {run_directory}") from e

    ## TODO: this calculates the batch times per percentile, which is a bit excessive
    ## for i in range(0,101,1):
    ##     metrics[f"IBT_ue_{i:02d}"] = df_batches.query("location == 'ue'")["IBT"].dropna().quantile(i/100)
    ## for i in range(0,101,1):
    ##     metrics[f"IBT_gnb_{i:02d}"] = df_batches.query("location == 'gnb'")["IBT"].dropna().quantile(i/100)
    ## for i in range(0,101,1):
    ##     metrics[f"BT_ue_{i:02d}"] = df_batches.query("location == 'ue'")["BT"].dropna().quantile(i/100)
    ## for i in range(0,101,1):
    ##     metrics[f"BT_gnb_{i:02d}"] = df_batches.query("location == 'gnb'")["BT"].dropna().quantile(i/100)
    ## for i in range(0,101,1):
    ##     metrics[f"BD_ue_{i:02d}"] = df_batches.query("location == 'ue'")["BD"].dropna().quantile(i/100)
    ## for i in range(0,101,1):
    ##     metrics[f"BD_gnb_{i:02d}"] = df_batches.query("location == 'gnb'")["BD"].dropna().quantile(i/100)

    if config["traffic_config__traffic_type"] == "idle":
        metrics["direction"] = "None"
        return { **metrics, **config }

    elif config["traffic_config__direction"] == "UlDl" :
        # Both Ul and Dl in the same pcaps
        if len(df.loc[df["trafficflow"] == "ingress"]) <= 0:
            log_report += "No ingress?, how?\n"
            print(log_report)
            return False
        for s in relevant_stats:
            try:
                metrics[f"delay__{s}"] = ret["delay"][s].loc["gnb"]
            except BaseException as e:
                print(f"ERROR: {run_directory} - {repr(e)}")
                print(f"ERROR: {run_directory} - {repr(e)}")
                print(f"ERROR: {run_directory} - {repr(e)}")
                print(f"ERROR: {run_directory} - {repr(e)}")
                print(f"ERROR: {run_directory} - {repr(e)}")
                return False
        metrics["direction"] = "Ul"
        # df_query = df.query(f"trafficflow == 'ingress' and location == 'gnb'")
        df_query = df_gnb_ingress
        metrics["missing_pkts"] = len( set(range(df_query.index.min(), df_query.index.max()+1)) .difference(set(df_query.index)) )
        metrics["sent_pkts"] = len(df_query)

        ret1 = {**metrics, **config}
        for s in relevant_stats:
            try:
                metrics[f"delay__{s}"] = ret["delay"][s].loc["ue"]
            except BaseException as e:
                print(f"ERROR: {run_directory} -- {repr(e)}")
                print(f"ERROR: {run_directory} -- {repr(e)}")
                print(ret["delay"])
                print("--")
                print(ret["delay"][s])
                print("--")
                print(ret["delay"][s].loc["ue"])
                print("--")
                return False
        metrics["direction"] = "Dl"
        # df_query = df.query(f"trafficflow == 'ingress' and location == 'ue'")
        df_query = df_ue_ingress
        metrics["missing_pkts"] = len( set(range(df_query.index.min(), df_query.index.max()+1)) .difference(set(df_query.index)) )
        metrics["sent_pkts"] = len(df_query)
        ret2 = {**metrics, **config}
        print(log_report)
        return [ret1,ret2]
    else:
        # TODO: this is a really bad heuristic?!
        if config["traffic_config__direction"] == "Ul":
            dir_label = "ingress"
            location_label = "gnb"
            # df_ingress = df.query(f"trafficflow == 'ingress' and location == 'gnb'")
            df_ingress = df_gnb_ingress
            # df_egress = df.query(f"trafficflow == 'egress' and location == 'ue'")
            df_egress = df_ue_egress
        else:
            dir_label = "ingress"
            location_label = "ue"
            # df_ingress = df.query(f"trafficflow == 'ingress' and location == 'ue'")
            df_ingress = df_ue_ingress
            # df_egress = df.query(f"trafficflow == 'egress' and location == 'gnb'")
            df_egress = df_gnb_egress
        log_report += f"Delay mean: {ret["delay"]["mean"]}\n"
        for s in relevant_stats:
            try:
                metrics[f"delay__{s}"] = ret["delay"][s].loc[location_label]
            except Exception as e:
                log_report += f"Exception\n"
                print(log_report)
                raise ValueError(f"Can't get metrics for {run_directory} {s}") from e
        # TODO: steamlined throughput calculation
        ts_min = df_ingress["Timestamp"].min()
        ts_max = df_ingress["Timestamp"].max()
        pkt_size = df_ingress["PacketSize"].mean()
        amount = len(df_ingress)
        log_report += f"Mi:{ts_min},Ma:{ts_max},S:{pkt_size},A:{amount}\n"

        metrics["missing_pkts"] = len( set(range(int(df_ingress.index.min()), int(df_ingress.index.max())+1)) .difference(set(df_ingress.index)) )
        metrics["throughput__mean"] = amount * pkt_size * 8 /(ts_max - ts_min)
        metrics["sent_pkts"] = len(df_ingress)
        ts_min = df_egress["Timestamp"].min()
        ts_max = df_egress["Timestamp"].max()
        pkt_size = df_egress["PacketSize"].mean()
        amount = len(df_egress)
        metrics["throughputin__mean"] = amount * pkt_size * 8 /(ts_max - ts_min)
        log_report += f"metrics['throughputin__mean'] {metrics["throughputin__mean"]}\n"

        metrics["direction"] = config["traffic_config__direction"]
        ret1 = {**metrics, **config}
        print(log_report)
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
    metrics = {
            "direction":'XXX',"failed_run":False, "missing_pkts":np.nan, "sent_pkts":np.nan, "throughputin__mean": np.nan,
            "ue_power_failed": True,
            "sdr_power_failed": True,
            "ue_channelmetrics_failed": True,
            "gnb_channelmetrics_failed": True,
            }
    for k,v in metrics_default.items():
        for s in relevant_stats:
            metrics[f"{k}__{s}"] = v

    with open(f"{run_directory}/{os.path.basename(run_directory)}.yaml", "r") as f:
        config = yaml.unsafe_load(f)
        dc = pd.json_normalize(config, sep="__") # flatten config
        config = dc.to_dict(orient='records')[0]

    if 'dl_mcs' in config.keys():
        cast_value = np.nan
        try:
            cast_value = float(config['dl_mcs'])
        except:
            pass
        config['dl_mcs'] = cast_value
    if 'ul_mcs' in config.keys():
        cast_value = np.nan
        try:
            cast_value = float(config['ul_mcs'])
        except:
            pass
        config['ul_mcs'] = cast_value
    if 'rx_gain' in config.keys():
        cast_value = np.nan
        try:
            cast_value = float(config['rx_gain'])
        except:
            pass
        config['rx_gain'] = cast_value
    if 'tx_gain' in config.keys():
        cast_value = np.nan
        try:
            cast_value = float(config['tx_gain'])
        except:
            pass
        config['tx_gain'] = cast_value


    faulty_run = os.path.isfile(f"{run_directory}/FAILED")
    fault_reason = ""

    if not faulty_run:
        returnvalue = calc_pkt_metrics(run_directory=run_directory, metrics=metrics, relevant_stats=relevant_stats, config=config)
        if returnvalue:
            return returnvalue





    # INFO: faulty:
    #
    # with open(f"{run_directory}/FAILED", "r") as ff:
    #     fault_reason = ff.read().strip().strip("\n")
    # TODO: how do i know if there is up-&downstream?
    metrics["direction"]="Ul"
    metrics["failed_run"]=True
    ret1 = {**metrics, **config}
    if config["traffic_config__traffic_type"] == "idle":
        ret1["direction"] = "None"
        return ret1

    ret2 = copy.deepcopy(ret1)
    ret2["direction"]="Dl"
    if config["traffic_config__direction"] == "UlDl":
        return [ret1,ret2]
    elif config["traffic_config__direction"] == "Ul":
        return [ret1]
    else:
        return [ret2]


def refactor_final_df(df: pd.DataFrame):
    df["gnb_version__combined"] = df["gnb_version__version"] + "__" + df["gnb_version__commit"].str.slice(0,7)
    return df



def handle_run(run_directory: str):
    dirname = os.path.basename(run_directory)
    run_config_path = f"{run_directory}/{dirname}.yaml"
    # print(run_config_path)

    with open(run_config_path, "r") as rf:
        run_config = yaml.safe_load(rf)

    traffic_type = run_config["traffic_config"]["traffic_type"]

    if traffic_type == "scapyudpping":
        return handle_ping_run(run_directory)
    elif traffic_type == "iperfthroughput":
        # WARN: when changing this step: double check FAILED runs are marked correctly
        # return handle_throughput_run(run_directory)
        return handle_ping_run(run_directory)
    elif traffic_type == "scapyudpthroughput":
        # WARN: when changing this step: double check FAILED runs are marked correctly
        # return handle_throughput_run(run_directory)
        return handle_ping_run(run_directory)
    elif traffic_type == "idle":
        # WARN: when changing this step: double check FAILED runs are marked correctly
        # return handle_throughput_run(run_directory)
        return handle_ping_run(run_directory)
    else:
        raise RuntimeError(f"Unknown traffic type: {traffic_type}")






def main():
    start = time.time()

    test_configurations = [e.path for e in os.scandir(ansible_dump) if e.is_dir() and not os.path.basename(e).startswith(".")]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir() and not os.path.basename(r).startswith(".")]
    # pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    if SKIP_EXISTING and os.path.isfile(f"{ansible_dump}/all_runs.parquet"):
        df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
        existing_ids = list(df['identifier'])
        print("---")
        print(existing_ids)
        print("---")
        print([ os.path.basename(r) for r in runs])
        print("---")
        print(f'len runs{len(runs)}, len ids{len(existing_ids)}({len(set(existing_ids))})')
        print("---")
        lenruns = len(runs)
        runs = [ r for r in runs if os.path.basename(r) not in existing_ids ]
        print(f"len before:{lenruns}, len after:{len(runs)}")



    print(runs)
    with mp.Pool(8) as p:
        returns = p.map(handle_run, runs)
    # returns = [handle_run(r) for r in runs]
    # returns = [handle_run("/home/lks/Documents/datastore/5g-masterarbeit/performance-tuning/54e0b2b3/54e0b2b3__d5bd4e7b__001")]


    records = []
    for r in returns:
        if isinstance(r,list):
            records.extend(r)
        else:
            records.append(r)


    final_df = pd.DataFrame.from_records(records)
    print(f"len final_df\n{len(final_df)}\n{final_df}")
    # aggregate final df
    if len(final_df)>0:
        final_df = refactor_final_df(final_df)


    print(final_df)

    if SKIP_EXISTING and os.path.isfile(f"{ansible_dump}/all_runs.parquet"):
        final_df = pd.concat([df, final_df], ignore_index=True)

    final_df.to_parquet(f"{ansible_dump}/all_runs.parquet")
    # final_df.to_csv(f"{ansible_dump}/all_runs.csv.gz", compression="gzip")
    print(f"Took {time.time()-start}s")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Evaluate packet recordings in csvs",
        description="Scan given dir and"
            )
    parser.add_argument("--skip", action='store_true')
    parser.add_argument("filename")
    args = parser.parse_args()
    ansible_dump = args.filename
    SKIP_EXISTING = args.skip
    # ansible_dump = "/home/lks/Akten/datastore/5g-masterarbeit/gnb-versions-delay"
    #ansible_dump = "/home/lks/Akten/datastore/5g-masterarbeit/dockerization"
    main()





