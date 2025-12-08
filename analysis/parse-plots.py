import importlib

import plotninesettings
import natsort
import itertools
import math
import yaml
import scipy
import scipy.stats
import plots
import pandas as pd
import numpy as np
import plotnine as p9
import argparse
import os
import multiprocessing as mp

parsed = importlib.import_module("parse-parsed")


from allcolumns import columns_to_group_by


ansible_dump = "/home/lks/Documents/datastore/5g-masterarbeit/gnb-versions-delay"
plot_dir = ansible_dump

def get_pcap_paths(ansible_dump_path = ansible_dump):
    test_configurations = [e.path for e in os.scandir(ansible_dump_path) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    runs = sorted(runs)
    print(runs)
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    pcaps = [os.path.dirname(p) for p in pcaps]
    pcaps = list(set(pcaps))
    pcaps = [f"{p}/combined.csv.gz" for p in pcaps]
    return pcaps

def mean_confidence_interval(confidence=0.90):
    """
    Get -mean- and the lower and upper limit for the confidence interval
    """
    def mcd(data):
        data2 = [d for d in data if not np.isnan(d)] # INFO: drop NaNs!
        a = 1.0 * np.array(data2)
        n = len(a)
        m, se = np.mean(a), scipy.stats.sem(a)
        h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
        return h
    mcd.__name__ = f"ci_{int(confidence*100):02d}"
    return mcd

def labeler_gnb_version(x):
    if x == "release_24_04":
        return "srsRAN 24.04"
    elif x == "release_24_10":
        return "srsRAN 24.10"
    elif x == "v2.1.0":
        return "OAI 2.1.0"
    elif x == "v2.2.0":
        return "OAI 2.2.0"
    else:
        return x

def add_labels(df: pd.DataFrame):
    if "gnb_version__version" in df.columns:
        df["gnb_version_label"] = df["gnb_version__version"].apply(labeler_gnb_version)
    if "traffic_config__rate" in df.columns:
        df["traffic_config__rate_lbl"] = pd.Categorical(df["traffic_config__rate"], ordered=True, categories= natsort.natsorted(df["traffic_config__rate"].unique()))
        df["traffic_config__rate_int"] = df["traffic_config__rate"].apply(lambda x: int(x[:-1]) if x.endswith("M") else 0)
    if "direction" in df.columns:
        df["direction"] = df["direction"].apply(lambda x : x.upper())
    if "tdd_config__tdd_ratio" in df.columns:
        df["tdd_ratio_label"]=df["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df['tdd_ratio_label'] = pd.Categorical(df['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df['tdd_ratio_label'].unique()))
    if "tdd_config__tdd_period" in df.columns:
        df["tdd_period_label"]=df["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df['tdd_period_label'] = pd.Categorical(df['tdd_period_label'], ordered=True, categories= natsort.natsorted(df['tdd_period_label'].unique()))
    if "run" in df.columns:
        df['run'] = pd.Categorical(df['run'], ordered=True, categories= natsort.natsorted(df['run'].unique()))
    if "tdd_config__tdd_ratio" in df.columns and "tdd_config__tdd_period" in df.columns:
        df["tdd_label"]="Dl/Ul: " + df["tdd_config__tdd_ratio"].astype(str) + "; #: " + df["tdd_config__tdd_period"].astype(str)
    return df



def __distance_snr( ansible_dump, df):
    plot_dir = ansible_dump
    series_dirs = [ f"{ansible_dump}/{d}" for d in os.listdir(ansible_dump) if os.path.isdir(f"{ansible_dump}/{d}") and not os.path.basename(f"{ansible_dump}/{d}").startswith(".")]
    run_dirs = [ r.path[:-5] for series in series_dirs for r in os.scandir(series) if r.is_dir() and not os.path.basename(r).startswith(".")]
    run_dirs = list(dict.fromkeys(run_dirs))

    runs_total = 0
    runs_failed = 0
    runs_asserts = 0
    for d in run_dirs:
        runs_total += 1
        config = {}
        modem_snr = []
        modem_sinr = []
        modem_rsrp = []
        modem_rsrq = []
        gnb_snr = []
        gnb_cqi = []
        gnb_rsrp = []
        gnb_mcs_dl = []
        gnb_mcs_ul = []
        for i in range(10):
            this_dir = f"{d}__{i:03d}"
            if not os.path.isdir(this_dir):
                continue
            if os.path.exists(f"{this_dir}/FAILED"):
                runs_failed += 0
                continue

            modem_df = pd.read_csv(f"{this_dir}/modem-snr.csv")
            for col in modem_df.columns:
                if col.casefold() == "timestamp".casefold():
                    continue
                # cleanup
                modem_df.loc[lambda x:x[col] == 0, col ] = np.nan    # drop all 0s
                modem_df[col] = pd.to_numeric(modem_df.loc[:,col], errors="coerce")
            ###k
            gnb_df = pd.read_csv(f"{this_dir}/gnb_snr.csv")
            for col in gnb_df.columns:
                if col.casefold() == "timestamp".casefold():
                    continue
                # cleanup
                gnb_df.loc[lambda x:x[col] == 0, col ] = np.nan    # drop all 0s
                gnb_df[col] = pd.to_numeric(gnb_df.loc[:,col], errors="coerce")

            if config == {}:
                with open(f"{this_dir}/{os.path.basename(this_dir)}.yaml", "r") as f:
                    config = yaml.unsafe_load(f)
                    dc = pd.json_normalize(config, sep="__")
                    config = dc.to_dict(orient='records')[0]
            # find single run
            if config["traffic_config__traffic_type"] == "scapyudpping":
                continue
            loc_query = (df["distance_horizontal_in_m"] == config["distance_horizontal_in_m"]) \
               & (df["gnb_version__type"] == config["gnb_version__type"]) & (df["traffic_config__traffic_type"] == config["traffic_config__traffic_type"]) \
               & (df["gnb_version__version"] == config["gnb_version__version"]) \
               & (df["traffic_config__direction"] == config["traffic_config__direction"]) \
               & (df["tdd_config__tdd_dl_ul_tx_period"] == config["tdd_config__tdd_dl_ul_tx_period"]) \
               & (df["tdd_config__tdd_dl_ul_ratio"] == config["tdd_config__tdd_dl_ul_ratio"])
            if len(df.loc[loc_query,:]) != 1:
                print(df.loc[loc_query,:])
                for c in df.loc[loc_query,:].columns:
                    if (len(df.loc[loc_query,c].unique()) != 1):
                        print(f"{c}:\t{df.loc[loc_query,c].unique()}")
                print(df.loc[loc_query,:].value_counts())
                print(len(df.loc[loc_query,:]))
            assert(len(df.loc[loc_query,:]) == 1)
            runs_asserts += 1
            # df.loc[loc_query,"GNB_SNR"] = gnb_df["SNR"].mean()
            modem_snr.append(modem_df["SNR"].mean())
            modem_sinr.append(modem_df["SINR"].mean())
            modem_rsrp.append(modem_df["RSRP"].mean())
            modem_rsrq.append(modem_df["RSRQ"].mean())
            gnb_snr.append(gnb_df["SNR"].mean())
            gnb_cqi.append(gnb_df["CQI"].mean())
            gnb_rsrp.append(gnb_df["RSRP"].mean())
            gnb_mcs_dl.append(gnb_df["MCS_DL"].mean())
            gnb_mcs_ul.append(gnb_df["MCS_UL"].mean())

        def mean(data):
            if len(data) == 0:
                return np.nan
            return sum(data)/len(data)
        ci95 = mean_confidence_interval(0.95)
        if config == {}:
            continue
        df.loc[loc_query,"MODEM_SNR"] = mean(modem_snr)
        df.loc[loc_query,"MODEM_SNR_ci"] = ci95(modem_snr)
        df.loc[loc_query,"MODEM_SINR"] = mean(modem_sinr)
        df.loc[loc_query,"MODEM_SINR_ci"] = ci95(modem_sinr)
        df.loc[loc_query,"MODEM_RSRP"] = mean(modem_rsrp)
        df.loc[loc_query,"MODEM_RSRP_ci"] = ci95(modem_rsrp)
        df.loc[loc_query,"MODEM_RSRQ"] = mean(modem_rsrq)
        df.loc[loc_query,"MODEM_RSRQ_ci"] = ci95(modem_rsrq)
        df.loc[loc_query,"GNB_SNR"] = mean(gnb_snr)
        df.loc[loc_query,"GNB_SNR_ci"] = ci95(gnb_snr)
        df.loc[loc_query,"GNB_CQI"] = mean(gnb_cqi)
        df.loc[loc_query,"GNB_CQI_ci"] = ci95(gnb_cqi)
        df.loc[loc_query,"GNB_RSRP"] = mean(gnb_rsrp)
        df.loc[loc_query,"GNB_RSRP_ci"] = ci95(gnb_rsrp)
        df.loc[loc_query,"GNB_MCS_DL"] = mean(gnb_mcs_dl)
        df.loc[loc_query,"GNB_MCS_DL_ci"] = ci95(gnb_mcs_dl)
        df.loc[loc_query,"GNB_MCS_UL"] = mean(gnb_mcs_ul)
        df.loc[loc_query,"GNB_MCS_UL_ci"] = ci95(gnb_mcs_ul)
    print(f"Total:{runs_total}\nFailed:{runs_failed}\nAsserts:{runs_asserts}\n")

    print(df.loc[:,["MODEM_SNR","MODEM_SINR"]])

    df_plot_q = df.query("traffic_config__traffic_type == 'iperfthroughput' and distance_horizontal_in_m != 2.5")
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-modemsnr-c",
                          facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label"], scales="free_y")},
                           labels={"y":"modem SNR [dB]", "x":"distance [m]", "color":"DL:UL", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="MODEM_SNR", ymin="MODEM_SNR - MODEM_SNR_ci",ymax="MODEM_SNR + MODEM_SNR_ci", x="distance_horizontal_in_m", color="tdd_ratio_label"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-gnbsnr-c",
                          facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label"], scales="free_y")},
                           labels={"y":"gNB SNR [dB]", "x":"distance [m]", "color":"DL:UL", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="GNB_SNR", ymin="GNB_SNR - GNB_SNR_ci",ymax="GNB_SNR + GNB_SNR_ci", x="distance_horizontal_in_m", color="tdd_ratio_label"),
                          )
    df_p_agg = df.query("traffic_config__traffic_type == 'iperfthroughput'").groupby(["distance_horizontal_in_m"]) \
            ["MODEM_SNR"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
    plots.simple_line_plot(df=df_p_agg, filename=f"{plot_dir}/agg_performance_height_iperf-modemsnr-highlevel",
                          labels={"y":"modem SNR [dB]", "x":"distance [m]"},
                          errorbars=False, points=False, lines=False,
                          aesthetics=p9.aes(y="mean", ymin="mean - ci_95",ymax="mean + ci_95", x="distance_horizontal_in_m"),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/2,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*2,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                          )
    df_p_agg = df.query("traffic_config__traffic_type == 'iperfthroughput'").groupby(["distance_horizontal_in_m"]) \
            ["GNB_SNR"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
    plots.simple_line_plot(df=df_p_agg, filename=f"{plot_dir}/agg_performance_height_iperf-gnbsnr-highlevel",
                          labels={"y":"gNB SNR [dB]", "x":"distance [m]"},
                          errorbars=False, points=False, lines=False,
                          aesthetics=p9.aes(y="mean", ymin="mean - ci_95",ymax="mean + ci_95", x="distance_horizontal_in_m"),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/2,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*2,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                          )


def _scenario_initial_pwr_test():
    scenarios = ["/storage/power_new/"]
    # scenarios = ["/storage/power_new_02_ul_worked/"]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump + "/.plots"
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        df_plot = df
        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler_gnb_version)
        df_plot["traffic_config__rate_lbl"] = pd.Categorical(df_plot["traffic_config__rate"], ordered=True, categories= natsort.natsorted(df_plot["traffic_config__rate"].unique()))
        df_plot["direction"] = df_plot["direction"].apply(lambda x : x.upper())
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df_plot['tdd_ratio_label'] = pd.Categorical(df_plot['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_ratio_label'].unique()))
        df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df_plot['tdd_period_label'] = pd.Categorical(df_plot['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_period_label'].unique()))
        df_plot["traffic_config__rate_int"]=df_plot["traffic_config__rate"].apply(lambda x: int(x[:-1]) if x.endswith("M") else 0)
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_period"].astype(str)


        df_perf = df_plot.melt(id_vars=[c for c in columns_to_group_by if c != "run"], value_vars=[c for c in df_plot.columns if c.startswith("perf_") and (not c.endswith("_ci_95") and "_percent_" not in c) ] )
        df_perf["perf_value_ci_95"] = df_plot.melt(id_vars=[c for c in columns_to_group_by if c != "run"], value_vars=[c for c in df_plot.columns if c.startswith("perf_") and (c.endswith("_ci_95") and "_percent_" not in c) ] )["value"]
        df_perf = df_perf.rename(columns={"variable": "perf_metric", "value": "perf_value"} )
        df_perf["perf_metric"] = df_perf["perf_metric"].apply(lambda x : x.split("__")[0].replace("perf_","") )
        df_perf["gnb_version_label"] = df_perf["gnb_version__version"].apply(labeler_gnb_version)
        df_perf["traffic_config__rate_int"]=df_perf["traffic_config__rate"].apply(lambda x: int(x[:-1]) if x.endswith("M") else 0)
        df_perf["tdd_ratio_label"]=df_perf["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df_perf['tdd_ratio_label'] = pd.Categorical(df_perf['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_perf['tdd_ratio_label'].unique()))
        df_perf["tdd_period_label"]=df_perf["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df_perf['tdd_period_label'] = pd.Categorical(df_perf['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_perf['tdd_period_label'].unique()))


        # scapy
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping'")
        plot_subdir = "scapy"
        os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
        if len(df_plot_q)>0:
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/scapy__failed_power",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                   labels={"y":"failed energy measurements", "x":"IAT [s]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="ue_power_failed__agg__mean", x="traffic_config__iat", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/scapy__failed_runs",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"failed runs", "x":"IAT [s]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="traffic_config__iat", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/scapy__agg_energy_ue",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"IAT [s]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="ue_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="ue_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="ue_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__iat", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/scapy__agg_energy_sdr",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"IAT [s]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="sdr_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="sdr_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="sdr_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__iat", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/scapy__agg_delay",
                                  facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_ratio_label"], scales="free_y")},
                                  labels={"y":"delay [s]", "x":"IAT [s]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean",
                                                    ymin="delay__mean__agg__ci_95_l",
                                                    ymax="delay__mean__agg__ci_95_u",
                                                    x="traffic_config__iat", color="tdd_period_label"),
                                  )

            df_perf_plot = df_perf.query("traffic_config__traffic_type == 'scapyudpping'")
            plots.simple_line_plot(df=df_perf_plot, filename=f"{plot_dir}/{plot_subdir}/scapy__perf",
                                  facets={"facet":p9.facet_grid("perf_metric",cols=["tdd_period_label", "gnb_version_label"], scales="free_y")},
                                  labels={"y":"count [10^6]", "x":"IAT [s]", "color":"TDD ratio", "fill":"TDD ratio"},
                                  errorbars=True,
                                  lines=True,
                                  points=False,
                                  aesthetics=p9.aes(y="perf_value / 1e6",
                                                    ymin="(perf_value - perf_value_ci_95) / 1e6",
                                                    ymax="(perf_value + perf_value_ci_95) / 1e6",
                                                    group="tdd_ratio_label",
                                                    x="traffic_config__iat", color="tdd_ratio_label"),
                                  )





        # IPERF UPLINK

        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Ul'")
        plot_subdir = "iperfUL"
        os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
        if len(df_plot_q)>0:
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__failed_power",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"failed energy measurements", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="ue_power_failed__agg__mean", x="traffic_config__rate_lbl", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__failed_runs",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"failed runs", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="traffic_config__rate_lbl", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__throughput",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"throughput [Mbps]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                   points=False,
                                   bars=False,
                                   lines=True,
                                  aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000",
                                                    ymin="(throughput__mean__agg__ci_95_l)/ 1e6",
                                                    ymax="(throughput__mean__agg__ci_95_u)/ 1e6",
                                                    x="traffic_config__rate_int", fill="tdd_period_label", color="tdd_period_label", group="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_snr_modem",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"modem SNR [dB]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="modem_snr__agg__mean",
                                                    ymin="modem_snr__agg__mean + modem_snr__agg__ci_95",
                                                    ymax="modem_snr__agg__mean - modem_snr__agg__ci_95",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_snr_gnb",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"gNB SNR [dB]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="gnb_snr__agg__mean",
                                                    ymin="gnb_snr__agg__mean + gnb_snr__agg__ci_95",
                                                    ymax="gnb_snr__agg__mean - gnb_snr__agg__ci_95",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_energy_ue",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="ue_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="ue_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="ue_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_energy_sdr",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="sdr_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="sdr_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="sdr_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_delay",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"delay [s]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean",
                                                    ymin="delay__mean__agg__ci_95_l",
                                                    ymax="delay__mean__agg__ci_95_u",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            df_perf_plot = df_perf.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Ul'")
            plots.simple_line_plot(df=df_perf_plot, filename=f"{plot_dir}/{plot_subdir}/iperf__perf",
                                  facets={"facet":p9.facet_grid("perf_metric",cols=["tdd_period_label", "gnb_version_label"], scales="free_y")},
                                  labels={"y":"count [10^6]", "x":"rate [Mbps]", "color":"TDD ratio", "fill":"TDD ratio"},
                                  errorbars=True,
                                  lines=True,
                                  points=False,
                                  aesthetics=p9.aes(y="perf_value/ 1e6",
                                                    ymin="(perf_value - perf_value_ci_95) / 1e6",
                                                    ymax="(perf_value + perf_value_ci_95) / 1e6",
                                                    group="tdd_ratio_label",
                                                    x="traffic_config__rate_int", color="tdd_ratio_label"),
                                  )
        # IPERF DOWNLINK
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Dl'")
        plot_subdir = "iperfDL"
        os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
        if len(df_plot_q)>0:
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__failed_power",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"failed energy measurements", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="ue_power_failed__agg__mean", x="traffic_config__rate_lbl", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__failed_runs",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"failed runs", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=False,
                                   points=False,
                                   bars=True,
                                   lines=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="traffic_config__rate_lbl", fill="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__throughput",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"throughput [Mbps]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                   points=False,
                                   bars=False,
                                   lines=True,
                                  aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000",
                                                    ymin="(throughput__mean__agg__ci_95_l)/ 1e6",
                                                    ymax="(throughput__mean__agg__ci_95_u)/ 1e6",
                                                    x="traffic_config__rate_int", fill="tdd_period_label", color="tdd_period_label", group="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__agg_snr_modem",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"modem SNR [dB]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="modem_snr__agg__mean",
                                                    ymin="modem_snr__agg__mean + modem_snr__agg__ci_95",
                                                    ymax="modem_snr__agg__mean - modem_snr__agg__ci_95",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__agg_snr_gnb",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"gNB SNR [dB]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="gnb_snr__agg__mean",
                                                    ymin="gnb_snr__agg__mean + gnb_snr__agg__ci_95",
                                                    ymax="gnb_snr__agg__mean - gnb_snr__agg__ci_95",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__agg_energy_ue",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="ue_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="ue_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="ue_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__agg_energy_sdr",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="sdr_power__agg__mean * traffic_config__traffic_duration",
                                                    ymin="sdr_power__agg__ci_95_l * traffic_config__traffic_duration",
                                                    ymax="sdr_power__agg__ci_95_u * traffic_config__traffic_duration",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_dl__agg_delay",
                                  facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                  labels={"y":"delay [s]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean",
                                                    ymin="delay__mean__agg__ci_95_l",
                                                    ymax="delay__mean__agg__ci_95_u",
                                                    x="traffic_config__rate_int", color="tdd_period_label"),
                                  )
            df_perf_plot = df_perf.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Dl'")
            plots.simple_line_plot(df=df_perf_plot, filename=f"{plot_dir}/{plot_subdir}/iperf__perf",
                                  facets={"facet":p9.facet_grid("perf_metric",cols=["tdd_period_label", "gnb_version_label"], scales="free_y")},
                                  labels={"y":"count [10^6]", "x":"rate [Mbps]", "color":"TDD ratio", "fill":"TDD ratio"},
                                  errorbars=True,
                                  lines=True,
                                  points=False,
                                  aesthetics=p9.aes(y="perf_value / 1e6",
                                                    ymin="(perf_value - perf_value_ci_95) / 1e6",
                                                    ymax="(perf_value + perf_value_ci_95) / 1e6",
                                                    group="tdd_ratio_label",
                                                    x="traffic_config__rate_int", color="tdd_ratio_label"),
                                  )


#      _   _                   ____                ____                   _                                    _   _
#     | \ | | ___  _ __       |  _ \ ___ _ __     |  _ \ _   _ _ __      / \   __ _  __ _ _ __ ___  __ _  __ _| |_(_) ___  _ __
#     |  \| |/ _ \| '_ \ _____| |_) / _ \ '__|____| |_) | | | | '_ \    / _ \ / _` |/ _` | '__/ _ \/ _` |/ _` | __| |/ _ \| '_ \
#     | |\  | (_) | | | |_____|  __/  __/ | |_____|  _ <| |_| | | | |  / ___ \ (_| | (_| | | |  __/ (_| | (_| | |_| | (_) | | | |
#     |_| \_|\___/|_| |_|     |_|   \___|_|       |_| \_\\__,_|_| |_| /_/   \_\__, |\__, |_|  \___|\__, |\__,_|\__|_|\___/|_| |_|
#                                                                             |___/ |___/          |___/
        df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
        df["tdd_ratio_label"]=df["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df['tdd_ratio_label'] = pd.Categorical(df['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df['tdd_ratio_label'].unique()))
        df["tdd_period_label"]=df["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df['tdd_period_label'] = pd.Categorical(df['tdd_period_label'], ordered=True, categories= natsort.natsorted(df['tdd_period_label'].unique()))
        df["gnb_version_label"] = df["gnb_version__version"].apply(labeler_gnb_version)

        for uldl in ["Ul", "Dl"]:
            df_plot = df.query(f" traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == '{uldl}' ")
            plot_subdir = f"iperf{uldl.upper()}"
            plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/{plot_subdir}/throughput_and_power",
                                   facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_period_label"], scales="fixed")},
                                   labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD UL:DL", "fill":"TDD UL:DL", "shape":"TDD UL:DL"},
                                   errorbars=False,
                                   points = False,
                                   lines= False,
                                   aesthetics=p9.aes(y="ue_power * traffic_config__traffic_duration",
                                                     x="throughput__mean / 1000000", color="tdd_ratio_label", shape="tdd_ratio_label", fill="tdd_ratio_label"),
                                   add_to_plot=[
                                       p9.geom_point(size=plotninesettings.POINT_SIZE*2),
                                       p9.scale_fill_manual(plots.COLORS),
                                       # p9.scale_shape_manual(["p", "d", "*"])
                                       # p9.scale_shape_manual([6, 7, "x"])
                                       p9.scale_shape_manual(["1", "2", "+"])
                                       ]
                                   )
            plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/{plot_subdir}/throughput_and_power_",
                                   facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_period_label"], scales="fixed")},
                                   labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD UL:DL", "fill":"TDD UL:DL", "shape":"TDD UL:DL"},
                                   errorbars=False,
                                   points = False,
                                   lines= False,
                                   aesthetics=p9.aes(y="ue_power * actualduration",
                                                     x="throughput__mean / 1000000", color="tdd_ratio_label", shape="tdd_ratio_label", fill="tdd_ratio_label"),
                                   add_to_plot=[
                                       p9.geom_point(size=plotninesettings.POINT_SIZE*2),
                                       p9.scale_fill_manual(plots.COLORS),
                                       # p9.scale_shape_manual(["p", "d", "*"])
                                       p9.scale_shape_manual(["1", "2", "+"])
                                       ]
                                   )
            plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/{plot_subdir}/throughput_and_power_alt",
                                   facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                                   labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period", "shape":"TDD period"},
                                   errorbars=False,
                                   points = False,
                                   lines= False,
                                   aesthetics=p9.aes(y="ue_power * traffic_config__traffic_duration",
                                                     x="throughput__mean / 1000000", color="tdd_period_label", shape="tdd_period_label", fill="tdd_period_label"),
                                   add_to_plot=[
                                       p9.geom_point(size=plotninesettings.POINT_SIZE*2),
                                       p9.scale_fill_manual(plots.COLORS),
                                       # p9.scale_shape_manual(["p", "d", "*"])
                                       p9.scale_shape_manual(["1", "2", "+"])
                                       ]
                                   )
        df_perf = df.melt(id_vars=[c for c in columns_to_group_by] + ["throughput__mean"], value_vars=[c for c in df.columns if c.startswith("perf_") ] )
        # df_perf["perf_value_ci_95"] = df_plot.melt(id_vars=[c for c in columns_to_group_by if c != "run"], value_vars=[c for c in df_plot.columns if c.startswith("perf_") and (c.endswith("_ci_95") and "_percent_" not in c) ] )["value"]
        df_perf = df_perf.rename(columns={"variable": "perf_metric", "value": "perf_value"} )
        df_perf["perf_metric"] = df_perf["perf_metric"].apply(lambda x : x.split("__")[0].replace("perf_","") )
        df_perf["gnb_version_label"] = df_perf["gnb_version__version"].apply(labeler_gnb_version)
        df_perf["traffic_config__rate_int"]=df_perf["traffic_config__rate"].apply(lambda x: int(x[:-1]) if x.endswith("M") else 0)
        df_perf["tdd_ratio_label"]=df_perf["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df_perf['tdd_ratio_label'] = pd.Categorical(df_perf['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_perf['tdd_ratio_label'].unique()))
        df_perf["tdd_period_label"]=df_perf["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df_perf['tdd_period_label'] = pd.Categorical(df_perf['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_perf['tdd_period_label'].unique()))

        for uldl in ["Ul", "Dl"]:
            df_perf_plot = df_perf.query(f"traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == '{uldl}'")
            plot_subdir = f"iperf{uldl.upper()}"
            plots.simple_line_plot(df=df_perf_plot, filename=f"{plot_dir}/{plot_subdir}/iperf__perf_rate",
                                  facets={"facet":p9.facet_grid("perf_metric",cols=["tdd_period_label", "gnb_version_label"], scales="free_y")},
                                  labels={"y":"count [10^6]", "x":"actual rate [Mbps]", "color":"TDD ratio", "shape":"TDD ratio"},
                                  errorbars=False,
                                  lines=False,
                                  points=False,
                                  aesthetics=p9.aes(y="perf_value/ 1e6",
                                                    group="tdd_ratio_label",
                                                    x="throughput__mean / 1e6", color="tdd_ratio_label", shape="tdd_ratio_label"),
                                  add_to_plot=[
                                      p9.geom_point(size=plotninesettings.POINT_SIZE*2),
                                      p9.scale_fill_manual(plots.COLORS),
                                      # p9.scale_shape_manual(["p", "d", "*"])
                                       p9.scale_shape_manual(["1", "2", "+"])
                                      ]
                                  )


        #  ____        _       _
        # | __ )  __ _| |_ ___| |__   ___  ___
        # |  _ \ / _` | __/ __| '_ \ / _ \/ __|
        # | |_) | (_| | || (__| | | |  __/\__ \
        # |____/ \__,_|\__\___|_| |_|\___||___/

        ####################

        df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
        print(df)


        df_plot = df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="IBT_ue_percentile", value_vars=[f"IBT_ue_{i:02d}" for i in range(0,101)] ).rename({'value':'IBT_ue'},axis='columns')
        df_plot_ibt_gnb = df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="IBT_gnb_percentile", value_vars=[f"IBT_gnb_{i:02d}" for i in range(0,101)] ) \
                    .rename({'value':'IBT_gnb'},axis='columns')[[ "IBT_gnb_percentile","IBT_gnb" ]]
        df_plot_bt_ue =  df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="BT_ue_percentile", value_vars=[f"BT_ue_{i:02d}" for i in range(0,101)] ) \
                    .rename({'value':'BT_ue'},axis='columns')[[ "BT_ue_percentile","BT_ue" ]]
        df_plot_bt_gnb = df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="BT_gnb_percentile", value_vars=[f"BT_gnb_{i:02d}" for i in range(0,101)] ) \
                    .rename({'value':'BT_gnb'},axis='columns')[[ "BT_gnb_percentile","BT_gnb" ]]
        df_plot_bd_ue =  df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="BD_ue_percentile", value_vars=[f"BD_ue_{i:02d}" for i in range(0,101)] ) \
                    .rename({'value':'BD_ue'},axis='columns')[[ "BD_ue_percentile","BD_ue" ]]
        df_plot_bd_gnb = df.reset_index().melt(id_vars=["index"] + columns_to_group_by, var_name="BD_gnb_percentile", value_vars=[f"BD_gnb_{i:02d}" for i in range(0,101)] ) \
                    .rename({'value':'BD_gnb'},axis='columns')[[ "BD_gnb_percentile","BD_gnb" ]]
        batch_times_percentile_and_values = [
                    [ "IBT_ue_percentile","IBT_ue" ],
                    [ "IBT_gnb_percentile","IBT_gnb" ],
                    [ "BT_ue_percentile","BT_ue" ],
                    [ "BT_gnb_percentile","BT_gnb" ],
                    [ "BD_ue_percentile","BD_ue" ],
                    [ "BD_gnb_percentile","BD_gnb" ]]
        df_plot = pd.concat( [df_plot, df_plot_ibt_gnb, df_plot_bt_ue, df_plot_bt_gnb, df_plot_bd_ue, df_plot_bd_gnb], axis='columns' )

        for batch_times_perc,_ in batch_times_percentile_and_values:
            df_plot[batch_times_perc] = df_plot[batch_times_perc].apply(lambda x: x.split("_")[2] ).astype(float, errors='ignore') / 100

        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler_gnb_version)
        df_plot["traffic_config__rate_lbl"] = pd.Categorical(df_plot["traffic_config__rate"], ordered=True, categories= natsort.natsorted(df_plot["traffic_config__rate"].unique()))
        df_plot["traffic_config__iat_lbl"] = pd.Categorical(df_plot["traffic_config__iat"], ordered=True, categories= sorted(df_plot["traffic_config__iat"].astype(float).apply(lambda x: str(x)).unique()))
        df_plot["direction"] = df_plot["direction"].apply(lambda x : x.upper())
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_ratio"].apply(lambda x: f"{x}:1")
        df_plot['tdd_ratio_label'] = pd.Categorical(df_plot['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_ratio_label'].unique()))
        df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_period"].apply(lambda x: f"{x} slots")
        df_plot['tdd_period_label'] = pd.Categorical(df_plot['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_period_label'].unique()))
        df_plot['run'] = pd.Categorical(df_plot['run'], ordered=True, categories= natsort.natsorted(df_plot['run'].unique()))
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_period"].astype(str)


        for batch_times_perc,batch_times_val in batch_times_percentile_and_values:
            if "_ue_" in batch_times_perc:
                df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Dl' and run == 0")
                plot_subdir = "iperfDL-BATCHES"
                os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
                if len(df_plot_q)>0:
                    plots.simple_line_plot(df=df_plot_q,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_dl_r0_tddall__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_period_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_ratio_label"),
                                           add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                    dftmp = df_plot_q.query("tdd_config__tdd_ratio == 2 and (traffic_config__rate_lbl == '10M' or traffic_config__rate_lbl == '20M' or traffic_config__rate_lbl == '30M' or traffic_config__rate_lbl == '40M' or traffic_config__rate_lbl == '50M')")
                    dftmp["traffic_config__rate_lbl"] = pd.Categorical(dftmp["traffic_config__rate"], ordered=True, categories= natsort.natsorted(dftmp["traffic_config__rate"].unique()))
                    plots.simple_line_plot(df=dftmp,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_dl_r0_tddall_r21__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["tdd_period_label"],cols=["gnb_version_label"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"rate [Mbps]", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="traffic_config__rate_lbl"),
                                           add_to_plot=[
                                               p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                    plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2 and (traffic_config__rate_lbl == '10M' or traffic_config__rate_lbl == '20M' or traffic_config__rate_lbl == '30M' or traffic_config__rate_lbl == '40M' or traffic_config__rate_lbl == '50M')"),
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_dl_r0_tddall_r21__{batch_times_val}_ecdf_alt",
                                           facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"tdd_period_label", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_period_label"),
                                           add_to_plot=[
                                               p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Dl' and tdd_config__tdd_period == 5")
                if len(df_plot_q)>0:
                    plots.simple_line_plot(df=df_plot_q,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_dl_r0_tddper5__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_ratio_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"run", "fill":"run"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="run"),
                                           add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
            else:
                df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Ul' and run == 0")
                plot_subdir = "iperfUL-BATCHES"
                os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
                if len(df_plot_q)>0:
                    plots.simple_line_plot(df=df_plot_q,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_ul_r0_tddall__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_period_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_ratio_label"),
                                           add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                    dftmp = df_plot_q.query("tdd_config__tdd_ratio == 2 and (traffic_config__rate_lbl == '4M' or traffic_config__rate_lbl == '8M' or traffic_config__rate_lbl == '12M' or traffic_config__rate_lbl == '16M' or traffic_config__rate_lbl == '20M')")
                    dftmp["traffic_config__rate_lbl"] = pd.Categorical(dftmp["traffic_config__rate"], ordered=True, categories= natsort.natsorted(dftmp["traffic_config__rate"].unique()))
                    plots.simple_line_plot(df=dftmp,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_ul_r0_tddall_r21__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["tdd_period_label"],cols=["gnb_version_label"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"rate [Mbps]", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="traffic_config__rate_lbl"),
                                           add_to_plot=[
                                               p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                    plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2 and (traffic_config__rate_lbl == '4M' or traffic_config__rate_lbl == '8M' or traffic_config__rate_lbl == '12M' or traffic_config__rate_lbl == '16M' or traffic_config__rate_lbl == '20M')"),
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_ul_r0_tddall_r21__{batch_times_val}_ecdf_alt",
                                           facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"tdd_period_label", "fill":"Dl:Ul"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_period_label"),
                                           add_to_plot=[
                                               p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )
                df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and traffic_config__direction == 'Ul' and tdd_config__tdd_period == 5")
                if len(df_plot_q)>0:
                    plots.simple_line_plot(df=df_plot_q,
                                           filename=f"{plot_dir}/{plot_subdir}/iperf_ul_r0_tddper5__{batch_times_val}_ecdf",
                                           facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_ratio_label"],cols=["traffic_config__rate_lbl"], scales="free_x")},
                                           labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"run", "fill":"run"},
                                           errorbars=False,
                                           points=False,
                                           bars=False,
                                           lines=False,
                                           size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                           aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="run"),
                                           add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                               ]
                                           )

            df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping' and direction == 'DL' and run == 0")
            plot_subdir = "scapy-BATCHES"
            os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)
            print("This df")
            print(df_plot_q)
            print(df_plot_q.columns)
            if len(df_plot_q)>0:
                plots.simple_line_plot(df=df_plot_q,
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_dl_r0_tddall__{batch_times_val}_ecdf",
                                       facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_period_label"],cols=["traffic_config__iat_lbl"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_ratio_label"),
                                       add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )
                plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2"),
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_dl_r0_tddall_r21__{batch_times_val}_ecdf",
                                       facets={"facet":p9.facet_grid(["tdd_period_label"],cols=["gnb_version_label"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"IAT [s]", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="traffic_config__iat_lbl"),
                                       add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )
                plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2"),
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_dl_r0_tddall_r21__{batch_times_val}_ecdf_alt",
                                       facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["traffic_config__iat_lbl"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"tdd_period_label", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_period_label"),
                                       add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )
            df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping' and direction == 'UL' and run == 0")
            if len(df_plot_q)>0:
                plots.simple_line_plot(df=df_plot_q,
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_ul_r0_tddall__{batch_times_val}_ecdf",
                                       facets={"facet":p9.facet_grid(["gnb_version_label", "tdd_period_label"],cols=["traffic_config__iat_lbl"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_ratio_label"),
                                       add_to_plot=[
                                       p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )
                plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2"),
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_ul_r0_tddall_r21__{batch_times_val}_ecdf",
                                       facets={"facet":p9.facet_grid(["tdd_period_label"],cols=["gnb_version_label"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"IAT [s]", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="traffic_config__iat_lbl"),
                                       add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )
                plots.simple_line_plot(df=df_plot_q.query("tdd_config__tdd_ratio == 2"),
                                       filename=f"{plot_dir}/{plot_subdir}/scapy_ul_r0_tddall_r21__{batch_times_val}_ecdf_alt",
                                       facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["traffic_config__iat_lbl"], scales="free_x")},
                                       labels={"y":"", "x":f"{batch_times_val} [ms]", "color":"tdd_period_label", "fill":"Dl:Ul"},
                                       errorbars=False,
                                       points=False,
                                       bars=False,
                                       lines=False,
                                       size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                                       aesthetics=p9.aes(y=batch_times_perc, x=batch_times_val, color="tdd_period_label"),
                                       add_to_plot=[
                                           p9.geom_line(size=plots.LINE_SIZE/1),
                                           ]
                                       )


def _scenario_initial_pwr_test_timeseries():
    ansible_dump = "/storage/power/"
    plot_dir = ansible_dump + "/plots"
    df_ = pd.read_csv(f"/storage/power/4433973f/4433973f__0e9bf943__000/combined.csv.gz") # oai tdd per 5
    # /storage/power/4433973f/4433973f__01f8cb25__000 srsran, tddper 20
    df_pwr = pd.read_csv(f"/storage/power/4433973f/4433973f__0e9bf943__000/power.csv").query("TYPE == 'current' ")
    df_pwr["CM"] = df_pwr["VAL"].cumsum()
    df_pwr["MA5"] = df_pwr["VAL"].rolling(window=5).mean()
    df_pwr["MA15"] = df_pwr["VAL"].rolling(window=15).mean()
    df_pwr["MA50"] = df_pwr["VAL"].rolling(window=50).mean()
    min_ts = df_["Timestamp"].min()

    for duration_spacing in [5, 0.1]:
        df_plot = df_pwr[ ( df_pwr["TIME"] > min_ts - duration_spacing) & ( df_pwr["TIME"] < min_ts + duration_spacing) ]
        current_min = df_plot["TIME"].min()
        df_plot["TIME"] = df_plot["TIME"] - current_min
        min_ts_plot = min_ts - current_min
        plots.simple_line_plot(df=df_plot,
                               filename=f"{plot_dir}/timeseries/OAI_iperf_Ul_{duration_spacing}s",
                               labels={"y":"current [mA]", "x":f"timestamp [s]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                               errorbars=False,
                               points=False,
                               bars=False,
                               lines=False,
                               size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                               aesthetics=p9.aes(y="VAL", x="TIME"),
                               add_to_plot=[
                               p9.geom_line(size=plots.LINE_SIZE/1, color=plotninesettings.COLORS[0]),
                               p9.geom_vline(xintercept=min_ts_plot, size=plots.LINE_SIZE/2, color=plotninesettings.COLORS[1]),
                                   ]
                               )
        plots.simple_line_plot(df=df_plot,
                               filename=f"{plot_dir}/timeseries/OAI_iperf_CM_Ul_{duration_spacing}s",
                               labels={"y":"current [mA]", "x":f"timestamp [s]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                               errorbars=False,
                               points=False,
                               bars=False,
                               lines=False,
                               size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                               aesthetics=p9.aes(y="CM", x="TIME"),
                               add_to_plot=[
                               p9.geom_line(size=plots.LINE_SIZE/1, color=plotninesettings.COLORS[0]),
                               p9.geom_vline(xintercept=min_ts_plot, size=plots.LINE_SIZE/2, color=plotninesettings.COLORS[1]),
                                   ]
                               )

    for moving_avg in ["MA5", "MA15", "MA50"]:
        df_plot = df_pwr[ ( df_pwr["TIME"] > min_ts - 5) & ( df_pwr["TIME"] < min_ts + 5) ]
        current_min = df_plot["TIME"].min()
        df_plot["TIME"] = df_plot["TIME"] - current_min
        min_ts_plot = min_ts - current_min
        plots.simple_line_plot(df=df_plot,
                               filename=f"{plot_dir}/timeseries/OAI_iperf_Ul_{moving_avg}s",
                               labels={"y":"current [mA]", "x":f"timestamp [s]", "color":"Dl:Ul", "fill":"Dl:Ul"},
                               errorbars=False,
                               points=False,
                               bars=False,
                               lines=False,
                               size=(plotninesettings.PLOT_W, plotninesettings.PLOT_H),
                               aesthetics=p9.aes(y=moving_avg, x="TIME"),
                               add_to_plot=[
                               p9.geom_line(size=plots.LINE_SIZE/1, color=plotninesettings.COLORS[0]),
                               p9.geom_vline(xintercept=min_ts_plot, size=plots.LINE_SIZE/2, color=plotninesettings.COLORS[1]),
                                   ]
                               )



def _main_effects_bandwidth():
    df_20mhz = pd.read_parquet(f"/storage/power_new/all_runs.parquet").query("traffic_config__traffic_type == 'iperfthroughput'")
    df_20mhz["bandwidth"] = "20MHz"
    df_40mhz = pd.read_parquet(f"/storage/power_new_40mhz/all_runs.parquet").query("traffic_config__traffic_type == 'iperfthroughput'")
    df_40mhz["bandwidth"] = "40MHz"

    plot_dir = "/storage/power_new_40mhz/"
    plot_subdir = "maineffects"
    os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)

    df = pd.concat([df_20mhz, df_40mhz],ignore_index=True, axis='rows').reset_index()
    df = add_labels(df)


    filename = f"{plot_dir}/{plot_subdir}/throughput"
    df["group"] = df["bandwidth"].astype(str) + df["tdd_ratio_label"].astype(str)
    plot = (p9.ggplot(df)
            + p9.facet_grid(rows=["direction", "gnb_version_label"], cols=["tdd_period_label"])
            + p9.aes(y="throughput__mean/ 1e6",
                     x="traffic_config__rate_int", color="tdd_ratio_label", fill="bandwidth", linetype="bandwidth",group="group")
            + p9.geom_line(size=plotninesettings.POINT_SIZE)
            + p9.labs(y="throughput [Mbps]", x="rate [Mbps]", color="Dl:Ul", fill="bandwidth", linetype="bandwidth")
            + p9.scale_color_manual(plotninesettings.COLOR_MAP_EXTRACTOR(3), drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H, verbose=False, dpi=450)# }}}

    for uldl in ["Ul", "Dl"]:
        filename = f"{plot_dir}/{plot_subdir}/throughput_{uldl}"
        df["group"] = df["bandwidth"].astype(str) + df["tdd_ratio_label"].astype(str)
        plot = (p9.ggplot(df[ df["direction"] == uldl.upper() ])
                + p9.facet_grid(rows=[ "gnb_version_label"], cols=["tdd_period_label"])
                + p9.aes(y="throughput__mean/ 1e6",
                         x="traffic_config__rate_int", color="tdd_ratio_label", fill="bandwidth", linetype="bandwidth",group="group")
                + p9.geom_line(size=plotninesettings.POINT_SIZE)
                + p9.labs(y="throughput [Mbps]", x="rate [Mbps]", color="Dl:Ul", fill="bandwidth", linetype="bandwidth")
                + p9.scale_color_manual(plotninesettings.COLOR_MAP_EXTRACTOR(3), drop=True)
                + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
                + plotninesettings.GLOBAL_THEME()
                )
        for e in [".pdf", ".jpg"]:
            plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H, verbose=False, dpi=450)# }}}

    for sdrue in ["ue", "sdr"]:
        for uldl in ["Ul", "Dl"]:
            filename = f"{plot_dir}/{plot_subdir}/throughput_{uldl}-energy-{sdrue}"
            df["group"] = df["bandwidth"].astype(str) + df["tdd_ratio_label"].astype(str)
            df_plot = df[ df["direction"] == uldl.upper() ]
            df_plot[f"energy_{sdrue}"] = df_plot[f"{sdrue}_power"] * df["traffic_config__traffic_duration"]
            assert(isinstance(df_plot, pd.DataFrame))
            plot = (p9.ggplot(df_plot)
                    + p9.facet_grid(rows=[ "gnb_version_label"], cols=["tdd_period_label"])
                    + p9.aes(y=f"energy_{sdrue} /1e3",
                             x="throughput__mean/ 1e6", color="tdd_ratio_label", shape="bandwidth", linetype="bandwidth", group="group")
                    + p9.geom_smooth(size=plotninesettings.LINE_SIZE)
                    + p9.geom_point(size=plotninesettings.POINT_SIZE*1.6)
                    + p9.labs(y=f"{sdrue} energy [KJ]", x="throughput [Mbps]", color="Dl:Ul", fill="bandwidth", linetype="bandwidth", shape="bandwidth")
                    + p9.scale_x_log10()
                    + p9.scale_shape_manual(["1", "2", "+"])
                    + p9.scale_color_manual(plotninesettings.COLOR_MAP_EXTRACTOR(3), drop=True)
                    + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
                    + p9.scale_linetype_manual(["solid", "dotted", "dashed", "dashdot"])
                    + plotninesettings.GLOBAL_THEME()
                    )
            for e in [".pdf", ".jpg"]:
                plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W/1.2, height=plotninesettings.PLOT_H/1.5, verbose=False, dpi=450)# }}}

    for sdrue in ["ue", "sdr"]:
        filename = f"{plot_dir}/{plot_subdir}/energy-bandwidth-main-{sdrue}"

        df_plot = df[ df["traffic_config__rate"] == df.groupby(["bandwidth","direction"])["traffic_config__rate"].transform('max') ]
        additional_aggs = { "ue_power":['mean', *[parsed.percentile(p) for p in [0.05, 0.25, 0.75, 0.95]], parsed.mean_confidence_interval(0.95) ],
                            "sdr_power":['mean', *[parsed.percentile(p) for p in [0.05, 0.25, 0.75, 0.95]], parsed.mean_confidence_interval(0.95) ],
                           }
        df_plot = df_plot.groupby(["bandwidth", "direction", "gnb_version_label"]).agg( { **parsed.build_agg_dictionary(), **additional_aggs }).reset_index()
        df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
        # from IPython import embed; embed()

        df_plot[f"energy_{sdrue}"] = df_plot[f"{sdrue}_power__agg__mean"] * df["traffic_config__traffic_duration"]
        df_plot[f"energy_{sdrue}_ci_95"] = df_plot[f"{sdrue}_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
        assert(isinstance(df_plot, pd.DataFrame))
        plot = (p9.ggplot(df_plot)
                + p9.facet_grid(rows=[ "gnb_version_label"], cols=["direction"])
                + p9.aes(y=f"energy_{sdrue} /1e3",
                         ymin=f"(energy_{sdrue} - energy_{sdrue}_ci_95) / 1e3",
                         ymax=f"(energy_{sdrue} + energy_{sdrue}_ci_95) / 1e3",
                         fill="bandwidth",
                         x="bandwidth")
                + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000000", width=0.6)
                + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(0.6), color="#000000")
                + p9.labs(y=f"{sdrue} energy [KJ]", x="bandwidth", color="", fill="")
                + p9.scale_shape_manual(["1", "2", "+"])
                + p9.scale_color_manual(plotninesettings.COLOR_MAP_EXTRACTOR(3), drop=True)
                + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
                + p9.scale_linetype_manual(["solid", "dotted", "dashed", "dashdot"])
                + plotninesettings.GLOBAL_THEME()
                )
        for e in [".pdf", ".jpg"]:
            plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W/1.2, height=plotninesettings.PLOT_H/1.5, verbose=False, dpi=450)# }}}




def _main_effects():
    basedir = "/storage/power_new/"
    df = pd.read_parquet(f"/{basedir}/all_runs.parquet")
    plot_dir =basedir

    plot_subdir = "maineffects"
    os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)

    bar_width = 0.7

    filename = f"{plot_dir}/{plot_subdir}/powerue_gnb-dl"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["gnb_version__type", "traffic_config__direction"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    plot = (p9.ggplot(df_plot)
            + p9.aes(y="energy_ue",
                                             ymin="(energy_ue - energy_ue_ci_95)",
                                             ymax="(energy_ue + energy_ue_ci_95)",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="ue energy [J]", x="", color="direction", fill="direction")
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W/1.4, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}

    filename = f"{plot_dir}/{plot_subdir}/powersdr_gnb-dl"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["gnb_version__type", "traffic_config__direction"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    plot = (p9.ggplot(df_plot)
            + p9.aes(y="energy_sdr",
                                             ymin="(energy_sdr - energy_sdr_ci_95)",
                                             ymax="(energy_sdr + energy_sdr_ci_95)",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="sdr energy [J]", x="", color="direction", fill="direction")
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W/1.4, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}

    filename = f"{plot_dir}/{plot_subdir}/powerue_gnb-dl-tdd"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_ue",
                                             ymin="(energy_ue - energy_ue_ci_95)",
                                             ymax="(energy_ue + energy_ue_ci_95)",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="ue energy [J]", x="", color="direction", fill="direction")
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}

    filename = f"{plot_dir}/{plot_subdir}/powersdr_gnb-dl-tdd"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_sdr",
                                             ymin="(energy_sdr - energy_sdr_ci_95)",
                                             ymax="(energy_sdr + energy_sdr_ci_95)",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="sdr energy [J]", x="", color="direction", fill="direction")
            + p9.coord_cartesian(ylim=(150000,200000))
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}


    filename = f"{plot_dir}/{plot_subdir}/powerue-per-byte_tdd"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["pkt_size", "gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot[ "volume" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__mean"]
    df_plot[ "volume__agg__ci_95" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__ci_95"]
    df_plot[ "energy_per_byte" ] =  df_plot["energy_ue"] / df_plot["volume"]
    df_plot[ "energy_per_byte__agg__ci_95_l" ] =  (df_plot["energy_ue"] - df_plot["energy_ue_ci_95"]) / (df_plot["volume"] + df_plot["volume__agg__ci_95"])
    df_plot[ "energy_per_byte__agg__ci_95_u" ] =  (df_plot["energy_ue"] + df_plot["energy_ue_ci_95"]) / (df_plot["volume"] - df_plot["volume__agg__ci_95"])
    print(df_plot[ "volume" ])
    print(df_plot[ "energy_per_byte" ])
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_per_byte* 1e6",
                                             ymin="energy_per_byte__agg__ci_95_l *1e6",
                                             ymax="energy_per_byte__agg__ci_95_u *1e6",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="ue energy per Byte [µJ]", x="", color="direction", fill="direction")
            + p9.scale_y_continuous()
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}

    filename = f"{plot_dir}/{plot_subdir}/powersdr-per-byte_tdd"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput")] .groupby(["pkt_size", "gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot[ "volume" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__mean"]
    df_plot[ "volume__agg__ci_95" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__ci_95"]
    df_plot[ "energy_per_byte" ] =  df_plot["energy_sdr"] / df_plot["volume"]
    df_plot[ "energy_per_byte__agg__ci_95_l" ] =  (df_plot["energy_sdr"] - df_plot["energy_sdr_ci_95"]) / (df_plot["volume"] + df_plot["volume__agg__ci_95"])
    df_plot[ "energy_per_byte__agg__ci_95_u" ] =  (df_plot["energy_sdr"] + df_plot["energy_sdr_ci_95"]) / (df_plot["volume"] - df_plot["volume__agg__ci_95"])
    print(df_plot[ "volume" ])
    print(df_plot[ "energy_per_byte" ])
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_per_byte *1e6",
                                             ymin="energy_per_byte__agg__ci_95_l *1e6",
                                             ymax="energy_per_byte__agg__ci_95_u *1e6",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="sdr energy per Byte [µJ]", x="", color="direction", fill="direction")
            + p9.scale_y_continuous()
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}


    filename = f"{plot_dir}/{plot_subdir}/powerue-per-byte_tdd_maxTP"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput") &
                 ( ((df["traffic_config__rate"] == "100M")&((df["direction"] == "Dl"))) |
                   ((df["traffic_config__rate"] == "20M")&((df["direction"] == "Ul"))) ) ] .groupby(["pkt_size", "gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot[ "volume" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__mean"]
    df_plot[ "volume__agg__ci_95" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__ci_95"]
    df_plot[ "energy_per_byte" ] =  df_plot["energy_ue"] / df_plot["volume"]
    df_plot[ "energy_per_byte__agg__ci_95_l" ] =  (df_plot["energy_ue"] - df_plot["energy_ue_ci_95"]) / (df_plot["volume"] + df_plot["volume__agg__ci_95"])
    df_plot[ "energy_per_byte__agg__ci_95_u" ] =  (df_plot["energy_ue"] + df_plot["energy_ue_ci_95"]) / (df_plot["volume"] - df_plot["volume__agg__ci_95"])
    print(df_plot[ "volume" ])
    print(df_plot[ "energy_per_byte" ])
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_per_byte *1e6",
                                             ymin="energy_per_byte__agg__ci_95_l *1e6",
                                             ymax="energy_per_byte__agg__ci_95_u *1e6",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="ue energy per Byte [µJ]", x="", color="direction", fill="direction")
            + p9.scale_y_continuous()
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}

    filename = f"{plot_dir}/{plot_subdir}/powersdr-per-byte_tdd_maxTP"
    df_plot = df[ (df["traffic_config__traffic_type"]=="iperfthroughput") &
                 ( ((df["traffic_config__rate"] == "100M")&((df["direction"] == "Dl"))) |
                   ((df["traffic_config__rate"] == "20M")&((df["direction"] == "Ul"))) ) ] .groupby(["pkt_size", "gnb_version__type", "traffic_config__direction", "tdd_config__tdd_period", "tdd_config__tdd_ratio"]).agg(parsed.build_agg_dictionary())
    df_plot.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), df_plot.columns.values))
    df_plot = df_plot.reset_index()
    df_plot = add_labels(df_plot)
    df_plot["energy_ue"] = df_plot["ue_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_ue_ci_95"] = df_plot["ue_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr"] = df_plot["sdr_power__agg__mean"] * df["traffic_config__traffic_duration"]
    df_plot["energy_sdr_ci_95"] = df_plot["sdr_power__agg__ci_95"] * df["traffic_config__traffic_duration"]
    df_plot[ "volume" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__mean"]
    df_plot[ "volume__agg__ci_95" ] = df_plot["pkt_size"] * df_plot["sent_pkts__agg__ci_95"]
    df_plot[ "energy_per_byte" ] =  df_plot["energy_sdr"] / df_plot["volume"]
    df_plot[ "energy_per_byte__agg__ci_95_l" ] =  (df_plot["energy_sdr"] - df_plot["energy_sdr_ci_95"]) / (df_plot["volume"] + df_plot["volume__agg__ci_95"])
    df_plot[ "energy_per_byte__agg__ci_95_u" ] =  (df_plot["energy_sdr"] + df_plot["energy_sdr_ci_95"]) / (df_plot["volume"] - df_plot["volume__agg__ci_95"])
    print(df_plot[ "volume" ])
    print(df_plot[ "energy_per_byte" ])
    plot = (p9.ggplot(df_plot)
            + p9.facet_grid(cols=["tdd_period_label", "tdd_ratio_label"])
            + p9.aes(y="energy_per_byte *1e6",
                                             ymin="energy_per_byte__agg__ci_95_l *1e6",
                                             ymax="energy_per_byte__agg__ci_95_u *1e6",
                                             group="gnb_version__type + traffic_config__direction",
                                             x="gnb_version__type", color="traffic_config__direction", fill="traffic_config__direction")
            + p9.geom_col(size=plotninesettings.LINE_SIZE/1.2, position=p9.position_dodge2(), color="#000", width=bar_width)
            + p9.geom_errorbar(size=plotninesettings.LINE_SIZE,width=plotninesettings.WIDTH/1.3, linetype="solid", position=p9.position_dodge(bar_width), color="#000000")
            + p9.labs(y="sdr energy per Byte [µJ]", x="", color="direction", fill="direction")
            + p9.scale_y_continuous()
            + p9.scale_color_manual(plotninesettings.COLORS_DARK, drop=True)
            + p9.scale_fill_manual(plotninesettings.COLORS, drop=True)
            + plotninesettings.GLOBAL_THEME()
            )
    for e in [".pdf", ".jpg"]:
        plot.save(f"{filename}{e}", width=plotninesettings.PLOT_W, height=plotninesettings.PLOT_H/2, verbose=False, dpi=450)# }}}



def _energy_idle():
    df = pd.read_parquet("/storage/power_idle/all_runs_groupby_agg.parquet")
    df = add_labels(df)
    plot_dir = "/storage/power_idle/.plots"
    plot_subdir = "power"
    os.makedirs(f"{plot_dir}/{plot_subdir}", exist_ok=True)

    df_plot_q = df

    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__failed_power",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                          labels={"y":"failed energy measurements", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                          errorbars=False,
                           points=False,
                           bars=True,
                           lines=False,
                          aesthetics=p9.aes(y="ue_power_failed__agg__mean", x="traffic_config__rate_lbl", fill="tdd_period_label"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_energy_ue",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                          labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                          errorbars=True,
                          aesthetics=p9.aes(y="ue_power__agg__mean * traffic_config__traffic_duration",
                                            ymin="ue_power__agg__ci_95_l * traffic_config__traffic_duration",
                                            ymax="ue_power__agg__ci_95_u * traffic_config__traffic_duration",
                                            x="traffic_config__rate_int", color="tdd_period_label"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/{plot_subdir}/iperf_ul__agg_energy_sdr",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["tdd_ratio_label"], scales="fixed")},
                          labels={"y":"energy [J]", "x":"rate [Mbps]", "color":"TDD period", "fill":"TDD period"},
                          errorbars=True,
                          aesthetics=p9.aes(y="sdr_power__agg__mean * traffic_config__traffic_duration",
                                            ymin="sdr_power__agg__ci_95_l * traffic_config__traffic_duration",
                                            ymax="sdr_power__agg__ci_95_u * traffic_config__traffic_duration",
                                            x="traffic_config__rate_int", color="tdd_period_label"),
                          )


if __name__ == "__main__":
    # _scenario_initial_pwr_test()
    # _main_effects()
    # _main_effects_bandwidth()

    _energy_idle()

    # _scenario_initial_pwr_test_timeseries()
