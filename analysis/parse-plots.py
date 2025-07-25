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





ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps_c80/"
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps/"
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps_2025-03-28/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps_2025-04-11/"

ansible_dump = "/home/lks/Documents/datastore/5g-masterarbeit/dumps/"
ansible_dump = "/home/lks/Akten/datastore/5g-masterarbeit/dumps_small_throughput"
# ansible_dump = "/home/lks/Documents/datastore/5g-masterarbeit/dockerization/"
ansible_dump = "/home/lks/Documents/datastore/5g-masterarbeit/gnb-versions-delay"
# ansible_dump = "/home/lks/Akten/datastore/5g-masterarbeit/dumps_2025-04-11"

# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/"
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/plottests"


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


def _scenario_main_measurements():
    scenarios = [
            "/mnt/ext1/5g-masterarbeit-daten/main_measurement/"
            ]
    for ansible_dump in scenarios:
        plot_dir = f"{ansible_dump}/.plots/"
        if not os.path.isdir(plot_dir):
            os.mkdir(plot_dir)

        ## # CDFs
        ## for cdf_msm_location, cdf_run in [
        ##         # iperf
        ##         ("gnb","9ff091db__0ba354c8"), ("gnb","9ff091db__580213e7"), ("gnb","9ff091db__2ef697cf"), ("gnb","9ff091db__5394720c"),
        ##         # scapy
        ##         ("ue","9ff091db__4b3d60f0")
        ##         ]:


        ##     df_r0 = pd.read_csv(f"/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/{cdf_run}__000/combined.csv.gz")
        ##     df_r0 = df_r0[ df_r0["location"]==cdf_msm_location ]
        ##     df_r0["run"] = "0"
        ##     df_r0["delay_cdf"] = df_r0["delay"].apply(lambda x : scipy.stats.percentileofscore(df_r0["delay"], x, kind='weak'))
        ##     df_r0["Timestamp"] = df_r0["Timestamp"] - df_r0["Timestamp"].min()
        ##     df_r1 = pd.read_csv(f"/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/{cdf_run}__001/combined.csv.gz")
        ##     df_r1 = df_r1[ df_r1["location"]==cdf_msm_location ]
        ##     df_r1["run"] = "1"
        ##     df_r1["delay_cdf"] = df_r1["delay"].apply(lambda x : scipy.stats.percentileofscore(df_r1["delay"], x, kind='weak'))
        ##     df_r1["Timestamp"] = df_r1["Timestamp"] - df_r1["Timestamp"].min()
        ##     df_r2 = pd.read_csv(f"/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/{cdf_run}__002/combined.csv.gz")
        ##     df_r2 = df_r2[ df_r2["location"]==cdf_msm_location ]
        ##     df_r2["run"] = "2"
        ##     df_r2["delay_cdf"] = df_r2["delay"].apply(lambda x : scipy.stats.percentileofscore(df_r2["delay"], x, kind='weak'))
        ##     df_r2["Timestamp"] = df_r2["Timestamp"] - df_r2["Timestamp"].min()
        ##     df_r3 = pd.read_csv(f"/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/{cdf_run}__003/combined.csv.gz")
        ##     df_r3 = df_r3[ df_r3["location"]==cdf_msm_location ]
        ##     df_r3["run"] = "3"
        ##     df_r3["delay_cdf"] = df_r3["delay"].apply(lambda x : scipy.stats.percentileofscore(df_r3["delay"], x, kind='weak'))
        ##     df_r3["Timestamp"] = df_r3["Timestamp"] - df_r3["Timestamp"].min()
        ##     df_r4 = pd.read_csv(f"/mnt/ext1/5g-masterarbeit-daten/main_measurement/9ff091db/{cdf_run}__004/combined.csv.gz")
        ##     df_r4 = df_r4[ df_r4["location"]==cdf_msm_location ]
        ##     df_r4["run"] = "4"
        ##     df_r4["delay_cdf"] = df_r4["delay"].apply(lambda x : scipy.stats.percentileofscore(df_r4["delay"], x, kind='weak'))
        ##     df_r4["Timestamp"] = df_r4["Timestamp"] - df_r4["Timestamp"].min()
        ##     df_cdf = pd.concat([df_r0,df_r1, df_r2, df_r3, df_r4])
        ##     df_cdf["datavolume"] = df_cdf["SeqNum"] * df_cdf["PacketSize"]
        ##     plots.simple_line_plot(df=df_cdf.loc[ df_cdf.index %31 == 0 ], filename=f"{plot_dir}/CDF_datavolume_{cdf_run}_full",
        ##                            # facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
        ##                            labels={"y":"cumulative data [MB]", "x":"time [s]", "color": "run"},
        ##                            errorbars=False,
        ##                            points=False,
        ##                            ratio="16:9",
        ##                            aesthetics=p9.aes(y="datavolume / 1000000",
        ##                                              # ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
        ##                                              # ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
        ##                                              x="Timestamp", color="run", group="run")
        ##                            )
        ##     plots.simple_line_plot(df=df_cdf.loc[ df_cdf["Timestamp"] < 10 ], filename=f"{plot_dir}/CDF_datavolume_{cdf_run}_10s",
        ##                            # facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
        ##                            labels={"y":"cumulative data [MB]", "x":"time [s]", "color": "run"},
        ##                            errorbars=False,
        ##                            points=False,
        ##                            ratio="16:9",
        ##                            aesthetics=p9.aes(y="datavolume / 1000000",
        ##                                              # ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
        ##                                              # ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
        ##                                              x="Timestamp", color="run", group="run")
        ##                            )
        ##     plots.simple_line_plot(df=df_cdf.loc[ df_cdf.index %31 == 0 ], filename=f"{plot_dir}/CDF_delay_{cdf_run}_full",
        ##                            # facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
        ##                            labels={"y":"CDF(delay)", "x":"delay [s]", "color": "run"},
        ##                            errorbars=False,
        ##                            points=False,
        ##                            ratio="16:9",
        ##                            aesthetics=p9.aes(y="delay_cdf / 100",
        ##                                              # ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
        ##                                              # ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
        ##                                              x="delay", color="run", group="run")
        ##                            )
        ##     plots.simple_line_plot(df=df_cdf.loc[ df_cdf.index %31 == 0 ], filename=f"{plot_dir}/CDF_delay_{cdf_run}_full_4:3",
        ##                            # facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
        ##                            labels={"y":"CDF(delay)", "x":"delay [s]", "color": "run"},
        ##                            errorbars=False,
        ##                            points=False,
        ##                            ratio="4:3",
        ##                            size=(plotninesettings.PLOT_W/2,plotninesettings.PLOT_W/2),
        ##                            aesthetics=p9.aes(y="delay_cdf / 100",
        ##                                              # ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
        ##                                              # ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
        ##                                              x="delay", color="run", group="run")
        ##                            )


        ## aggregations
        df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
        def labeler(x):
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
        df["gnb_label"] = df["gnb_version__version"].apply(labeler)
        df["tdd_label"]="Dl/Ul: " + df["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df["tdd_config__tdd_dl_ul_tx_period"].astype(str)
        df["gnb_version"]= df["gnb_label"]
        df["uhd_version"]= df["gnb_version__uhd_version"]

        querys = [
                { "str": "sdr == 'B210'", "label": "q_sdr_B210"},
                { "str": "traffic_config__traffic_type == 'scapyudpping'", "label": "q_traf_scapyping"},
                { "str": "traffic_config__traffic_type == 'iperfthroughput'", "label": "q_traf_iperf"},
                ]


        params_base = [
        "direction",
        "gnb_type",
        "tdd_ratio",
        "tdd_period",
        "gnb_version",
        "uhd_version",
        ]

        df["direction"] = df["direction"].apply(lambda x : x.upper())
        df["gnb_type"] = df["gnb_version__type"].astype(str)
        df["tdd_ratio"] = df["tdd_config__tdd_dl_ul_ratio"].astype(str) + ":1"
        df['tdd_ratio'] = pd.Categorical(df['tdd_ratio'], ordered=True, categories= natsort.natsorted(df['tdd_ratio'].unique()))
        df["tdd_period"] = df["tdd_config__tdd_dl_ul_tx_period"].astype(str) + " slots"
        df['tdd_period'] = pd.Categorical(df['tdd_period'], ordered=True, categories= natsort.natsorted(df['tdd_period'].unique()))


        ### GNB/UHD Ver
        df_tdd = df[ df["traffic_config__traffic_type"] == "iperfthroughput" ].drop(columns=[c for c in df.columns if c != "throughput__mean" and c not in ["direction", "gnb_version","uhd_version","tdd_period", "tdd_ratio"] ])
        df_tdd_agg = df_tdd.groupby(["direction", "gnb_version","uhd_version","tdd_period", "tdd_ratio"])["throughput__mean"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
        df_tdd_agg.rename(columns={"mean":"throughput__mean___mean", "ci_95":"throughput__mean___ci_95"}, inplace=True)
        df_tdd_agg["uhd_version"] = df_tdd_agg["uhd_version"].apply(lambda x : "4.0" if x == "UHD-4.0" else "3.15")
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/UHD__iperf_throughput",
                               facets={"facet":p9.facet_grid("direction",cols=["tdd_period", "tdd_ratio"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"USRP Hardware Driver", "color": "gNB"},
                               errorbars=True,
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="uhd_version", color="gnb_version", group="gnb_version")
                               )
        df_tdd = df[ df["traffic_config__traffic_type"] == "iperfthroughput" ].drop(columns=[c for c in df.columns if c != "throughput__mean" and c not in ["direction", "uhd_version","tdd_period", "tdd_ratio"] ])
        df_tdd_agg = df_tdd.groupby(["direction", "uhd_version","tdd_period", "tdd_ratio"])["throughput__mean"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
        df_tdd_agg.rename(columns={"mean":"throughput__mean___mean", "ci_95":"throughput__mean___ci_95"}, inplace=True)
        df_tdd_agg["uhd_version"] = df_tdd_agg["uhd_version"].apply(lambda x : "4.0" if x == "UHD-4.0" else "3.15")
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/UHD__iperf_throughput_alt01",
                               facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"USRP Hardware Driver", "color": "DL:UL"},
                               errorbars=True,
                               ratio="16:9",
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="uhd_version", color="tdd_ratio", group="tdd_ratio")
                               )
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/UHD__iperf_throughput_alt02",
                               facets={"facet":p9.facet_grid("direction",cols=["tdd_ratio"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"USRP Hardware Driver", "color": "TDD period"},
                               errorbars=True,
                               ratio="16:9",
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="uhd_version", color="tdd_period", group="tdd_period")
                               )
        df_tdd = df[ df["traffic_config__traffic_type"] == "iperfthroughput" ].drop(columns=[c for c in df.columns if c != "throughput__mean" and c not in ["direction", "gnb_version","tdd_period", "tdd_ratio", "gnb_type"] ])
        df_tdd_agg = df_tdd.groupby(["direction", "gnb_version","tdd_period", "tdd_ratio"])["throughput__mean"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
        df_tdd_agg.rename(columns={"mean":"throughput__mean___mean", "ci_95":"throughput__mean___ci_95"}, inplace=True)
        df_tdd_agg["group"] = df_tdd_agg["tdd_ratio"].astype(str) + df_tdd_agg["gnb_version"].astype(str).apply(lambda x : x[:3] )
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/GNB__iperf_throughput",
                               facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"", "color": "DL:UL"},
                               errorbars=True,
                               ratio="16:9",
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="gnb_version", color="tdd_ratio", group="group")
                               )


        ### TDD patterns
        df_tdd = df[ df["traffic_config__traffic_type"] == "iperfthroughput" ].drop(columns=[c for c in df.columns if c != "throughput__mean" and c not in params_base ])
        df_tdd_agg = df_tdd.groupby(["direction", "gnb_type","tdd_period", "tdd_ratio"])["throughput__mean"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
        df_tdd_agg.rename(columns={"mean":"throughput__mean___mean", "ci_95":"throughput__mean___ci_95"}, inplace=True)
        # df_tdd_agg.columns = ['___'.join(filter(None,col)) for col in df_tdd_agg.columns.values]
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/TDD__iperf_throughput",
                               facets={"facet":p9.facet_grid("direction",cols=["tdd_period"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"DL:UL ratio", "color": "gNB"},
                               errorbars=True,
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="tdd_ratio", color="gnb_type", group="gnb_type")
                               )
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/TDD__iperf_throughput_alt01",
                               facets={"facet":p9.facet_grid("direction",cols=["gnb_type"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"DL:UL ratio", "color": "TDD period"},
                               errorbars=True,
                               ratio="16:9",
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="tdd_ratio", color="tdd_period", group="tdd_period")
                               )
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/TDD__iperf_throughput_alt01_4:3",
                               facets={"facet":p9.facet_grid("direction",cols=["gnb_type"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"DL:UL ratio", "color": "TDD period"},
                               errorbars=True,
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="tdd_ratio", color="tdd_period", group="tdd_period")
                               )
        plots.simple_line_plot(df=df_tdd_agg, filename=f"{plot_dir}/TDD__iperf_throughput_alt02",
                               facets={"facet":p9.facet_grid("direction",cols=["gnb_type"], scales="free_y")},
                               labels={"y":"throughput [Mbps]", "x":"TDD period", "color": "UL:DL ratio"},
                               errorbars=True,
                               aesthetics=p9.aes(y="throughput__mean___mean / 1000000",
                                                 ymin="(throughput__mean___mean - throughput__mean___ci_95) / 1000000",
                                                 ymax="(throughput__mean___mean + throughput__mean___ci_95) / 1000000",
                                                 x="tdd_period", color="tdd_ratio", group="tdd_ratio")
                               )

        for query in querys:
            df_queried = df.query(query["str"])

            ### Main effects
            always_group_by_params = ["gnb_label"]
            main_effects_params_list = ["tdd_label", "direction", "traffic_config__traffic_type", "gnb_version__uhd_version"]
            if query["label"] == "q_traf_scapyping":
                main_effects_params_list.append("traffic_config__size")
            for main_effect_parameter in main_effects_params_list:
                print(f"main_effect_param: {main_effect_parameter}")
                ## drop non-numeric columns
                columns_to_drop_because_nonnumeric = []
                for c in df_queried.columns:
                    if c != main_effect_parameter and c not in always_group_by_params and not pd.api.types.is_numeric_dtype(df_queried[c]):
                        columns_to_drop_because_nonnumeric.append(c)

                df_agg = df_queried.drop(columns_to_drop_because_nonnumeric, axis="columns").copy(deep=True)
                df_agg = df_agg.groupby(always_group_by_params + [main_effect_parameter]).agg(["mean", "count", mean_confidence_interval(0.95)]).reset_index()
                df_agg.columns = ['___'.join(filter(None,col)) for col in df_agg.columns.values]
                # print(df_agg["failed_run"])
                # print(df_agg[main_effect_parameter,"failed_run mean","failed_run ci95"])
                print(list(df_agg.columns))
                print(df_agg[["failed_run___mean","failed_run___ci_95"]+always_group_by_params+[main_effect_parameter]])
                print("\n\n")

                plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effect_parameter}__total_count",
                                       facets={"facet":p9.facet_grid("gnb_label",scales="fixed")},
                                       labels={"y":"total runs [#]"},
                                       errorbars=False,
                                       aesthetics=p9.aes(y="failed_run___count", x=main_effect_parameter)
                                       )
                plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effect_parameter}__failed",
                                       facets={"facet":p9.facet_grid("gnb_label",scales="fixed")},
                                       labels={"y":"failed [%]"},
                                       errorbars=True,
                                       aesthetics=p9.aes(y="failed_run___mean * 100", ymin="(failed_run___mean - failed_run___ci_95) * 100", ymax="(failed_run___mean + failed_run___ci_95) * 100", x=main_effect_parameter)
                                       )
                plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effect_parameter}__total_success",
                                       facets={"facet":p9.facet_grid("gnb_label",scales="fixed")},
                                       labels={"y":"successful runs [#]"},
                                       errorbars=False,
                                       aesthetics=p9.aes(y="delay__mean___count", x=main_effect_parameter)
                                       )
                plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effect_parameter}__delay",
                                       facets={"facet":p9.facet_grid("gnb_label",scales="fixed")},
                                       labels={"y":"delay [s]"},
                                       errorbars=True,
                                       aesthetics=p9.aes(y="delay__mean___mean", ymin="delay__mean___mean - delay__mean___ci_95", ymax="delay__mean___mean + delay__mean___ci_95", x=main_effect_parameter)
                                       )
                plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effect_parameter}__throughput",
                                       facets={"facet":p9.facet_grid("gnb_label",scales="fixed")},
                                       labels={"y":"throughput [Mbps]"},
                                       errorbars=True,
                                       aesthetics=p9.aes(y="throughput__mean___mean / 1000000", ymin="(throughput__mean___mean - throughput__mean___ci_95)/ 1000000", ymax="(throughput__mean___mean + throughput__mean___ci_95)/ 1000000", x=main_effect_parameter)
                                       )

            ### Main effects with 1 less degrees of freedom
            for main_effects_param_1, main_effects_param_2 in itertools.permutations(main_effects_params_list, 2):
                columns_to_drop_because_nonnumeric = []
                for c in df_queried.columns:
                    if c not in [main_effects_param_1, main_effects_param_2] and c not in always_group_by_params and not pd.api.types.is_numeric_dtype(df_queried[c]):
                        columns_to_drop_because_nonnumeric.append(c)

                df_agg = df_queried.drop(columns_to_drop_because_nonnumeric, axis="columns").copy(deep=True)
                df_agg = df_agg.groupby(always_group_by_params + [main_effects_param_1, main_effects_param_2]).agg(["mean", "count", mean_confidence_interval(0.95)]).reset_index()
                df_agg.columns = ['___'.join(filter(None,col)) if len(col)==2 else col for col in df_agg.columns.values]
                # print(df_agg["failed_run"])
                # print(df_agg[main_effect_parameter,"failed_run mean","failed_run ci95"])
                print(f"len multiindex: {df_agg.index.get_level_values(0)}")
                print(list(df_agg.columns))
                print("Print excerpt: ")
                print(df_agg[["failed_run___mean","failed_run___ci_95"]+always_group_by_params+[main_effects_param_1, main_effects_param_2]])
                print("\n\n")

                print("New df:")
                print(df_agg)
                try:
                    plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effects_param_1}x{main_effects_param_2}__total_count",
                                           facets={"facet":p9.facet_grid("gnb_label", cols=main_effects_param_2,scales="fixed")},
                                           labels={"y":"total runs [#]"},
                                           errorbars=False,
                                           aesthetics=p9.aes(y="failed_run___count", x=main_effects_param_1)
                                           )
                    plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effects_param_1}x{main_effects_param_2}__failed",
                                           facets={"facet":p9.facet_grid("gnb_label", cols=main_effects_param_2,scales="fixed")},
                                           labels={"y":"failed [%]"},
                                           errorbars=True,
                                           aesthetics=p9.aes(y="failed_run___mean * 100", ymin="(failed_run___mean - failed_run___ci_95) * 100", ymax="(failed_run___mean + failed_run___ci_95) * 100", x=main_effects_param_1)
                                           )
                except Exception as e:
                    print(df_agg)
                    print(f"p1: {main_effects_param_1}, p2: {main_effects_param_2}")
                    df_agg.to_csv("~/tmp.csv.gz")
                    raise e
                try:
                    plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effects_param_1}x{main_effects_param_2}__total_success",
                                           facets={"facet":p9.facet_grid("gnb_label", cols=main_effects_param_2,scales="fixed")},
                                           labels={"y":"successful runs [#]"},
                                           errorbars=False,
                                           aesthetics=p9.aes(y="delay__mean___count", x=main_effects_param_1)
                                           )
                    plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effects_param_1}x{main_effects_param_2}__delay",
                                           facets={"facet":p9.facet_grid("gnb_label", cols=main_effects_param_2,scales="fixed")},
                                           labels={"y":"delay [s]"},
                                           errorbars=True,
                                           aesthetics=p9.aes(y="delay__mean___mean", ymin="delay__mean___mean - delay__mean___ci_95", ymax="delay__mean___mean + delay__mean___ci_95", x=main_effects_param_1)
                                           )
                except Exception as e:
                    print(df_agg)
                    print(f"p1: {main_effects_param_1}, p2: {main_effects_param_2}")
                    df_agg.to_csv("~/tmp.csv.gz")
                    raise e
                try:
                    plots.simple_line_plot(df=df_agg, filename=f"{plot_dir}/agg_main__{query["label"]}__{main_effects_param_1}x{main_effects_param_2}__throughput",
                                           facets={"facet":p9.facet_grid("gnb_label", cols=main_effects_param_2,scales="fixed")},
                                           labels={"y":"throughput [Mbps]"},
                                           errorbars=True,
                                           aesthetics=p9.aes(y="throughput__mean___mean / 1000000", ymin="(throughput__mean___mean - throughput__mean___ci_95)/ 1000000", ymax="(throughput__mean___mean + throughput__mean___ci_95)/ 1000000", x=main_effects_param_1)
                                           )
                except Exception as e:
                    print(df_agg)
                    print(f"p1: {main_effects_param_1}, p2: {main_effects_param_2}")
                    df_agg.to_csv("~/tmp.csv.gz")
                    raise e


def _scenario_tdd_algo():
    scenarios = [
            "/mnt/ext1/5g-masterarbeit-daten/tdd-pattern-algo/"
            ]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        df_plot = df
        df_plot["tdd_allocation_label"] = df_plot["location"].apply(lambda x : "OLD" if x =="B205-oldtdd" else "NEW")
        df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x : f"Dl/Ul ={x}")
        plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_iperf_throughput_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_allocation_label", scales="fixed")},
                              labels={"y":"throughput [Mbps]", "x":"tdd ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="tdd_config__tdd_dl_ul_ratio", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_iperf_throughput_compare-bandwidth-alt01",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"throughput [Mbps]", "x":"tdd alloc algo", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="tdd_allocation_label", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_iperf_delay_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_allocation_label", scales="fixed")},
                              labels={"y":"delay [s]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="tdd_config__tdd_dl_ul_ratio", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )

        plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'scapyudpping'"), filename=f"{plot_dir}/agg_scapy_delay_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_allocation_label", scales="fixed")},
                              labels={"y":"delay [s]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="tdd_config__tdd_dl_ul_ratio", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )


def _scenario_distance_wall():
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/distance_wall"]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        def labeler(x):
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

        df_plot = df
        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
        # df_plot["docker_label"] = df_plot["dockerization"].apply(lambda x: "docker" if x else "bare")
        # df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"TDD Dl/Ul {x}")
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + ":1"
        df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].apply(lambda x: f"{x} slots")
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)
        df_plot["distance_wall_label"]="" + df_plot["distance_nearest_wall"].astype(str)


        # iperf
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay",
                              facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"", "shape":"direction"},
                              errorbars=False,
                              points=False,
                              lines=False,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_wall_label"),
                              size=(plotninesettings.PLOT_W*0.65, plotninesettings.PLOT_H*0.65),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/1.5,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*3,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput",
                              facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"", "shape":"direction"},
                              errorbars=False,
                              points=False,
                              lines=False,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_wall_label"),
                              size=(plotninesettings.PLOT_W*0.65, plotninesettings.PLOT_H*0.65),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/1.5,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*3,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                              )

        # scapy
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay",
                              facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="free_y")},
                              labels={"y":"delay [ms]", "x":"distance [m]", "color":"", "shape":"direction"},
                              errorbars=False,
                              points=False,
                              lines=False,
                              aesthetics=p9.aes(y="delay__mean__agg__mean * 1000", ymin="delay__mean__agg__ci_95_l *1000",ymax="delay__mean__agg__ci_95_u *1000", x="distance_wall_label"),
                              size=(plotninesettings.PLOT_W*0.65, plotninesettings.PLOT_H*0.65),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/1.5,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*3,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                              )



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
                          labels={"y":"modem SNR [dB]", "x":"distance [m]", "color":"tdd ratio", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="MODEM_SNR", ymin="MODEM_SNR - MODEM_SNR_ci",ymax="MODEM_SNR + MODEM_SNR_ci", x="distance_horizontal_in_m", color="tdd_ratio_label"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-gnbsnr-c",
                          facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label"], scales="free_y")},
                          labels={"y":"gNB SNR [dB]", "x":"distance [m]", "color":"tdd ratio", "shape":"direction"},
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


def _scenario_distance():
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/distance"]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)



        def labeler(x):
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

        df_plot = df
        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
        # df_plot["docker_label"] = df_plot["dockerization"].apply(lambda x: "docker" if x else "bare")
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"TDD Dl/Ul {x}")
        df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].apply(lambda x: f"{x} slots")
        df_plot['tdd_period_label'] = pd.Categorical(df_plot['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_period_label'].unique()))
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)

        __distance_snr(ansible_dump, df_plot)

        # iperf
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and distance_horizontal_in_m != 2.5")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="fixed")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 2 and tdd_config__tdd_dl_ul_tx_period == 10")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s01",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s01-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 4 and tdd_config__tdd_dl_ul_tx_period == 10")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s02",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s02-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 1 and tdd_config__tdd_dl_ul_tx_period == 5")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s03",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free-s03-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )

        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )

        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-c",
                              facets={"facet":p9.facet_grid(["gnb_version__type", "direction"],cols=["tdd_period_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd ratio", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m", color="tdd_ratio_label"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 2 and tdd_config__tdd_dl_ul_tx_period == 10")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s01",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s01-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 4 and tdd_config__tdd_dl_ul_tx_period == 10")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s02",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s02-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )
        df_plot_qs = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and tdd_config__tdd_dl_ul_ratio == 1 and tdd_config__tdd_dl_ul_tx_period == 5")
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s03",
                              facets={"facet":p9.facet_grid(["direction"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_qs, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-free-s03-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="distance_horizontal_in_m"),
                              )

        df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping' and distance_horizontal_in_m != 2.5")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="fixed")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay_free",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label", "tdd_ratio_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"distance [m]", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="distance_horizontal_in_m"),
                              )


        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free",
        #                       facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="free_y")},
        #                       labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-alt02",
        #                       facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["gnb_version_label"], scales="free_y")},
        #                       labels={"y":"delay [s]", "x":"", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-alt02-flip",
        #                       facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["direction", "tdd_config__tdd_dl_ul_ratio"], scales="free_y")},
        #                       labels={"y":"delay [s]", "x":"", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput",
        #                       facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
        #                       labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-alt",
        #                       facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["docker_label"], scales="free_y")},
        #                       labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="gnb_version_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-alt02",
        #                       facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["gnb_version_label"], scales="free_y")},
        #                       labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-log",
        #                       facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
        #                       labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=True,
        #                       scale="log2",
        #                       aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
        #                       )
        # plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-failed",
        #                       facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
        #                       labels={"y":"failed runs [%]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
        #                       errorbars=False,
        #                       aesthetics=p9.aes(y="failed_run__agg__mean * 100.0", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
        #                       )

        # scapy
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay-free",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-throughput",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-failed",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"failed runs [%]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=False,
                              aesthetics=p9.aes(y="failed_run__agg__mean * 100.0", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )


def _scenario_qam():
    ansible_dump = f"/mnt/ext1/5g-masterarbeit-daten/qam"
    plot_dir = ansible_dump
    if not os.path.isdir(plot_dir):
        os.mkdir(plot_dir)

    df_64qam = pd.read_parquet(f"/mnt/ext1/5g-masterarbeit-daten/dockerization_qam64256/all_runs_groupby_agg.parquet")
    df_64qam.drop(df_64qam.index[df_64qam["gnb_version__type"] != "srsRAN"], inplace = True)
    # df_64qam.reset_index(inplace=True)
    df_64qam.drop(df_64qam.index[df_64qam["dockerization"] == False], inplace = True)
    # df_64qam.reset_index(inplace=True)
    df_64qam.loc[:,"qam"] = 64
    df_256qam = pd.read_parquet(f"/mnt/ext1/5g-masterarbeit-daten/dockerization/all_runs_groupby_agg.parquet")
    df_256qam.drop(df_256qam.index[df_256qam["gnb_version__type"] != "srsRAN"], inplace = True)
    # df_256qam.reset_index(inplace=True)
    df_256qam.drop(df_256qam.index[df_256qam["dockerization"] == False], inplace = True)
    # df_256qam.reset_index(inplace=True)
    df_256qam.loc[:,"qam"] = 256

    df = pd.concat([df_64qam,df_256qam],ignore_index=True)
    df.reset_index(inplace=True)

    def labeler(x):
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

    df_plot = df
    df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
    # df_plot["docker_label"] = df_plot["dockerization"].apply(lambda x: "docker" if x else "bare")
    df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"{x}:1")
    df_plot['tdd_ratio_label'] = pd.Categorical(df_plot['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_ratio_label'].unique()))
    df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].apply(lambda x: f"{x} slots")
    df_plot['tdd_period_label'] = pd.Categorical(df_plot['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_period_label'].unique()))
    df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)

    df_plot['tdd_config__tdd_dl_ul_ratio'] = pd.Categorical(df_plot['tdd_config__tdd_dl_ul_ratio'], ordered=True, categories= natsort.natsorted(df_plot['tdd_config__tdd_dl_ul_ratio'].unique()))
    df_plot['tdd_config__tdd_dl_ul_tx_period'] = pd.Categorical(df_plot['tdd_config__tdd_dl_ul_tx_period'], ordered=True, categories= natsort.natsorted(df_plot['tdd_config__tdd_dl_ul_tx_period'].unique()))
    df_plot['qam'] = pd.Categorical(df_plot['qam'], ordered=True, categories= natsort.natsorted(df_plot['qam'].unique()))
    df_plot['qam_label'] = df_plot['qam'].astype(str) + "QAM"
    df_plot['qam_label'] = pd.Categorical(df_plot['qam_label'], ordered=True, categories= natsort.natsorted(df_plot['qam_label'].unique()))

    with open(f"{plot_dir}/raw.txt", "w") as f:
        collected_data =pd.DataFrame(None, pd.MultiIndex.from_product([["tt"],["A"],["b"]], names=["Dir","Period", "Ratio"]),["speedup"]).dropna()
        dd = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and gnb_version__version == 'release_24_10'")
        for dir in ["Ul", "Dl"]:
            for per in [5,10,20]:
                for ratio in [1,2,4]:
                    mean_64qam = dd.loc[ (dd["qam_label"] == "64QAM") & (dd["direction"] == dir) &
                                             (dd["tdd_config__tdd_dl_ul_tx_period"] == per ) &
                                             (dd["tdd_config__tdd_dl_ul_ratio"] == ratio) , "throughput__mean__agg__mean" ].mean()
                    mean_256qam = dd.loc[ (dd["qam_label"] == "256QAM") & (dd["direction"] == dir) &
                                             (dd["tdd_config__tdd_dl_ul_tx_period"] == per ) &
                                             (dd["tdd_config__tdd_dl_ul_ratio"] == ratio) , "throughput__mean__agg__mean" ].mean()
                    collected_data.loc[ (str(dir),str(per),str(ratio)), "speedup"] = mean_256qam/mean_64qam
                    collected_data.loc[ (str(dir),str(per),str(ratio)), "calc"] = f"{mean_256qam}/{mean_64qam}"
        # f.write(f" 64QAM:{mean_64qam}\n256QAM:{mean_256qam}")
        print(collected_data, file=f)

    # iperf
    df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput'")
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "qam_label"], scales="free_y")},
                          labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"TDD period", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "qam_label"], scales="fixed")},
                          labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"TDD period", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                          )
    df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput' and gnb_version__version == 'release_24_10'")
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-24_10",
                          facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "qam_label"], scales="fixed")},
                          labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"TDD period", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-24_10-alt",
                          facets={"facet":p9.facet_grid("direction",cols=["tdd_config__tdd_dl_ul_tx_period"], scales="free_y")},
                          labels={"y":"throughput [Mps]", "x":"QAM", "color":"TDD ratio"},
                          errorbars=True,
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="qam", color="factor(tdd_config__tdd_dl_ul_ratio)"),
                          )
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-24_10-alt-169",
                          facets={"facet":p9.facet_grid("direction",cols=["tdd_period_label"], scales="free_y")},
                          labels={"y":"throughput [Mps]", "x":"", "color":"TDD ratio"},
                          errorbars=True, ratio="16:9",
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="qam_label", color="tdd_ratio_label"),
                          )

def __height_snr( ansible_dump, df):

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
               & (df["distance_vertical_in_m"] == config["distance_vertical_in_m"]) \
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

    df_plot_q = df.query("traffic_config__traffic_type == 'iperfthroughput'")
    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_height_iperf-modemsnr",
                          facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label"], scales="fixed")},
                          labels={"y":"modem SNR [dB]", "x":"height difference [m]", "color":"tdd ratio", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="MODEM_SNR", ymin="MODEM_SNR - MODEM_SNR_ci",ymax="MODEM_SNR + MODEM_SNR_ci", x="height_difference",
                                            color="tdd_ratio_label"),
                          )

    plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_height_iperf-gnbsnr",
                          facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label"], scales="fixed")},
                          labels={"y":"gNB SNR [dB]", "x":"height difference [m]", "color":"tdd ratio", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="GNB_SNR", ymin="GNB_SNR - GNB_SNR_ci",ymax="GNB_SNR + GNB_SNR_ci", x="height_difference",
                                            color="tdd_ratio_label"),
                          )
    df_p_agg = df.query("traffic_config__traffic_type == 'iperfthroughput'").groupby(["height_difference"]) \
            ["MODEM_SNR"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
    plots.simple_line_plot(df=df_p_agg, filename=f"{plot_dir}/agg_performance_height_iperf-modemsnr-highlevel",
                          labels={"y":"modem SNR [dB]", "x":"height difference [m]"},
                          errorbars=False, points=False, lines=False,
                          aesthetics=p9.aes(y="mean", ymin="mean - ci_95",ymax="mean + ci_95", x="height_difference"),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/2,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*2,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                          )
    df_p_agg = df.query("traffic_config__traffic_type == 'iperfthroughput'").groupby(["height_difference"]) \
            ["GNB_SNR"].agg(["mean",mean_confidence_interval(0.95)]).reset_index()
    plots.simple_line_plot(df=df_p_agg, filename=f"{plot_dir}/agg_performance_height_iperf-gnbsnr-highlevel",
                          labels={"y":"gNB SNR [dB]", "x":"height difference [m]"},
                          errorbars=False, points=False, lines=False,
                          aesthetics=p9.aes(y="mean", ymin="mean - ci_95",ymax="mean + ci_95", x="height_difference"),
                              add_to_plot=[
                                  p9.geom_errorbar(size=plotninesettings.LINE_SIZE/2,width=plotninesettings.WIDTH/2, linetype="solid",color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_point(size=plotninesettings.POINT_SIZE*2,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  p9.geom_line(size=plotninesettings.LINE_SIZE,color=plotninesettings.COLOR_MAP_EXTRACTOR(5)[0]),
                                  ]
                          )

def _scenario_height():
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/height"]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        def labeler(x):
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

        df_plot = df
        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
        df_plot["docker_label"] = df_plot["dockerization"].apply(lambda x: "docker" if x else "bare")
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"{x}:1")
        df_plot['tdd_ratio_label'] = pd.Categorical(df_plot['tdd_ratio_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_ratio_label'].unique()))
        df_plot["tdd_period_label"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].apply(lambda x: f"{x} slots")
        df_plot['tdd_period_label'] = pd.Categorical(df_plot['tdd_period_label'], ordered=True, categories= natsort.natsorted(df_plot['tdd_period_label'].unique()))
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)
        df_plot.loc[:,"height_difference"]=df_plot["distance_vertical_in_m"].astype(str)

        __height_snr(ansible_dump, df_plot)

        # iperf
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_height_iperf-throughput",
                              facets={"facet":p9.facet_grid(["gnb_version_label", "direction"],cols=["tdd_period_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"height difference [m]", "color":"tdd ratio", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",
                                                ymax="throughput__mean__agg__ci_95_u / 1000000", x="height_difference",
                                                color="tdd_ratio_label"),
                              )




def _scenario_dockerization():
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/dockerization"]
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        def labeler(x):
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

        df_plot = df
        df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
        df_plot["docker_label"] = df_plot["dockerization"].apply(lambda x: "docker" if x else "bare")
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"TDD Dl/Ul {x}")
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)


        # iperf
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'iperfthroughput'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-free",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-alt02",
                              facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-delay-alt02-flip",
                              facets={"facet":p9.facet_grid(["gnb_version_label"],cols=["direction", "tdd_config__tdd_dl_ul_ratio"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-alt",
                              facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["docker_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="gnb_version_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-alt02",
                              facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"throughput [Mps]", "x":"", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-throughput-log",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              scale="log2",
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_iperf-failed",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"failed runs [%]", "x":"tdd dl/ul ratio", "color":"tdd period", "shape":"direction"},
                              errorbars=False,
                              aesthetics=p9.aes(y="failed_run__agg__mean * 100.0", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )

        # scapy
        df_plot_q = df_plot.query("traffic_config__traffic_type == 'scapyudpping'")
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay-free",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-throughput",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"throughput [Mps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              # aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-failed",
                              facets={"facet":p9.facet_grid("gnb_version_label",cols=["direction", "docker_label"], scales="fixed")},
                              labels={"y":"failed runs [%]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=False,
                              aesthetics=p9.aes(y="failed_run__agg__mean * 100.0", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )
        plots.simple_line_plot(df=df_plot_q, filename=f"{plot_dir}/agg_performance_tuning_scapy-delay-alt02",
                              facets={"facet":p9.facet_grid(["direction", "tdd_config__tdd_dl_ul_ratio"],cols=["gnb_version_label"], scales="free_y")},
                              labels={"y":"delay [s]", "x":"", "color":"tdd period", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="docker_label", color="factor(tdd_config__tdd_dl_ul_tx_period)"),
                              )




def _scenario_throughput_overshoot():
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/throughput-overshoot", "/mnt/ext1/5g-masterarbeit-daten/throughput-overshoot-scapy"]
    scenarios.reverse()
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        df_plot = df
        df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
        df_plot["bandwidth_sent"]=df_plot["traffic_config__rate"].apply(lambda x: int(x[:-1]))
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"TDD Dl/Ul {x}")
        plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_throughput_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"throughput [Mbps]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="bandwidth_sent", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        df_plot.loc[:,"5gcalcthroughput"] = np.nan
        df_plot.loc[(df_plot["direction"] == "Dl") & (df_plot["tdd_config__tdd_dl_ul_tx_period"] == 5)
                    & (df_plot["tdd_config__tdd_dl_ul_ratio"] == 1),"5gcalcthroughput"] = 58.471 # 56.132
        df_plot.loc[(df_plot["direction"] == "Dl") & (df_plot["tdd_config__tdd_dl_ul_tx_period"] == 5)
                    & (df_plot["tdd_config__tdd_dl_ul_ratio"] == 1),"5gcalcthroughput"] = 27.522 # 25.020
        df_plot.loc[(df_plot["direction"] == "Dl") & (df_plot["tdd_config__tdd_dl_ul_tx_period"] == 5)
                    & (df_plot["tdd_config__tdd_dl_ul_ratio"] == 4),"5gcalcthroughput"] = 58.471 # 56.132
        df_plot.loc[(df_plot["direction"] == "Dl") & (df_plot["tdd_config__tdd_dl_ul_tx_period"] == 5)
                    & (df_plot["tdd_config__tdd_dl_ul_ratio"] == 4),"5gcalcthroughput"] = 27.522 # 25.020
        plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_throughput_compare-bandwidth_incl5gcalc",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"throughput [Mbps]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="bandwidth_sent", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_throughputin_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"generated throughput [Mbps]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="throughputin__mean__agg__mean / 1000000", ymin="throughputin__mean__agg__ci_95_l / 1000000",ymax="throughputin__mean__agg__ci_95_u / 1000000", x="bandwidth_sent", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_delay_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"delay [s]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="bandwidth_sent", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )

        # FIXME:
        # # for p in get_pcap_paths(ansible_dump)[:4]:
        # for p in get_pcap_paths(ansible_dump):
        #     if not p.endswith("__000/combined.csv.gz"):
        #         continue
        #     try:
        #         plot_per_run(p)
        #     except Exception as e:
        #         print(f"Exception during per_run plots for <{p}>")
        #         raise e


def _scenario_performance_tuning():
    subsets = [
            # "performance-tuning",
            "performance-tuning_cstate_recommends",
            # "performance-tuning_nocstate_recommends",
            # "performance-tuning_smt_cstate_recommends",
            ]
    # scenarios = ["/home/lks/Documents/datastore/5g-masterarbeit/" + sub for sub in subsets]
    scenarios = ["/mnt/ext1/5g-masterarbeit-daten/" + sub for sub in subsets]
    scenarios.reverse()
    for ansible_dump in scenarios:
        plot_dir = ansible_dump
        df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
        print(df)

        df_plot = df
        df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
        df_plot["group_alt01"]=df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str)+ df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)+df_plot["performance_tuning"].astype(str)+df_plot["direction"].astype(str) + df_plot["direction"].astype(str)
        df_plot["bandwidth_sent"]=df_plot["traffic_config__rate"].apply(lambda x: int(x[:-1]))
        df_plot["tdd_ratio_label"]=df_plot["tdd_config__tdd_dl_ul_ratio"].apply(lambda x: f"TDD Dl/Ul {x}")
        df_plot["tdd_label"]="Dl/Ul: " + df_plot["tdd_config__tdd_dl_ul_ratio"].astype(str) + "; #: " + df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str)

        # traffic_config__traffic_type == iperfthroughput
        if len(df_plot.query("traffic_config__traffic_type == 'iperfthroughput'")) > 0 :
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-bandwidth_alt01",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="direction", scales="free_y")},
                                  labels={"y":"throughput [Mbps]", "x":"tdd patern", "color":"tuning"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="tdd_label", color="factor(performance_tuning)", group="group_alt01"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-delay_alt01",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="direction", scales="fixed")},
                                  labels={"y":"delay [s]", "x":"tdd pattern", "color":"tuning"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="tdd_label", color="factor(performance_tuning)", group="group_alt01"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-failedrun_alt01",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="direction", scales="fixed")},
                                  labels={"y":"failed runs [#]", "x":"tdd pattern", "color":"tuning"},
                                  errorbars=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="tdd_label", color="factor(performance_tuning)", group="group_alt01"),
                                  )

            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-bandwidth",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"throughput [Mbps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-delay",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-missingpkts",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"missing pkts [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="missing_pkts__agg__mean", ymin="missing_pkts__agg__min",ymax="missing_pkts__agg__max", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-sentpkts",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"transmitted pkts [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="sent_pkts__agg__mean", ymin="sent_pkts__agg__ci_95_l",ymax="sent_pkts__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'iperfthroughput'"), filename=f"{plot_dir}/agg_performance_tuning_iperf-failedruns",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"failed runs [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )

        # traffic_config__traffic_type == scapyudpping
        if len(df_plot.query("traffic_config__traffic_type == 'scapyudpping'")) > 0 :
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'scapyudpping'"), filename=f"{plot_dir}/agg_performance_tuning_scapyping-delay_alt01",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="direction", scales="fixed")},
                                  labels={"y":"delay [s]", "x":"tdd pattern", "color":"performance_tuning"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="tdd_label", color="factor(performance_tuning)", group="group_alt01"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'scapyudpping'"), filename=f"{plot_dir}/agg_performance_tuning_scapyping-missingpkts",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"missing pkts [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="missing_pkts__agg__mean", ymin="missing_pkts__agg__min",ymax="missing_pkts__agg__max", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'scapyudpping'"), filename=f"{plot_dir}/agg_performance_tuning_scapyping-sentpkts",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"transmitted pkts [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=True,
                                  aesthetics=p9.aes(y="sent_pkts__agg__mean", ymin="sent_pkts__agg__ci_95_l",ymax="sent_pkts__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )
            plots.simple_line_plot(df=df_plot.query("traffic_config__traffic_type == 'scapyudpping'"), filename=f"{plot_dir}/agg_performance_tuning_scapyping-failedruns",
                                  facets={"facet":p9.facet_grid("gnb_version__type",cols="performance_tuning", scales="fixed")},
                                  labels={"y":"failed runs [#]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                                  errorbars=False,
                                  aesthetics=p9.aes(y="failed_run__agg__mean", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                                  )


        # FIXME: uncomment
        # for p in get_pcap_paths(ansible_dump)[:4]:
        #     if not p.endswith("__000/combined.csv.gz"):
        #         continue
        #     try:
        #         print(f"{p}")
        #         plot_per_run(p)
        #     except Exception as e:
        #         print(f"Exception during per_run plots for <{ansible_dump}>")
        #         # TODO: failed per plot run handling
        #         # raise e



def _scenario_gnb_versions_delay():
    ansible_dump = "/home/lks/Documents/datastore/5g-masterarbeit/gnb-versions-delay"
    df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
    df = df.query(f"run == 0")
    print(df)

    plots.simple_line_plot(df=df, filename=f"{plot_dir}/simple-line",
            labels={"y":"delay [s]", "x":"tdd dl slots", "color":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%", x="tdd_config__tdd_dl_slots", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            errorbars=True
            )


    df_plot = df
    df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
    df_plot["direction_width"] = df_plot["direction"].apply(lambda x: 0.2 if x == "Ul" else 0.4)
    df_plot["direction_line"] = df_plot["direction"].apply(lambda x: "dashed" if x == "Ul" else "solid")
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/throughput_compare-tdd",
                          facets={"facet":p9.facet_grid(cols='gnb_version__type', scales="fixed")},
                           labels={"y":"throughput [Mbps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                          aesthetics=p9.aes(y="throughput__mean / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="direction", group="group"),
                          )
    df_plot = df_plot.query("failed_run == False")
    df_plot.drop(columns=[c for c in df_plot.columns if c not in ["gnb_version__type", "throughput__mean", "tdd_config__tdd_dl_ul_ratio", "tdd_config__tdd_dl_ul_tx_period", "direction", "direction_line", "direction_width", "dockerization"]], inplace=True)
    df_plot.to_csv("/tmp/ttt.csv")
    df_plot = pd.read_csv("/tmp/ttt.csv")
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/throughput_compare-tdd-b",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols='direction')},
                           labels={"y":"throughput [bps]", "x":"tdd dl/ul ratio", "color":"tdd period","fill":"tdd period", "linetype":"dockerization", "shape":"direction"},
                            lines=False,points=False,bars=True,errorbars=False,
                          aesthetics=p9.aes(y="throughput__mean", x="factor(tdd_config__tdd_dl_ul_ratio)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="dockerization"),
                           add_to_plot=[p9.scale_linetype_manual(["solid", "dotted"]), p9.guides(linetype = p9.guide_legend(title="dockerization", override_aes = {"size": 1.4, "fill":"#fff"}))]
                          )



    ## UHD and GNB VERSIONS
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 5 and tdd_config__tdd_dl_ul_ratio == 1")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp5--tddr1",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 10 and tdd_config__tdd_dl_ul_ratio == 1")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp10--tddr1",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 5 and tdd_config__tdd_dl_ul_ratio == 2")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp5--tddr2",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 10 and tdd_config__tdd_dl_ul_ratio == 2")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp10--tddr2",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )





    df_plot = df.query("distance_vertical_in_m == 0.0 and traffic_config__iat == '0.001'")
    print(df_plot)
    plot_name=f"{plot_dir}/boxplots-cmp-0.5h-0.001s"
    df_plot.to_csv(f"{plot_name}.csv")
    # TODO: move renaming/labeling to it's own function
    def labeler(x):
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
    df_plot["gnb_version_label"] = df_plot["gnb_version__version"].apply(labeler)
    plots.box_plot_manual(df=df_plot, filename=plot_name,
                          facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version_label', scales="fixed")},
                          limits={"ylim":[0.0, 0.035], "cartesian":True},
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )




    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )

    #

    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )



def plot_per_setup():
    plot_per_setup_single_runs()
    plot_per_setup_aggregated_runs()


def plot_per_setup_single_runs():
    df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
    df = df.query(f"run == 0")
    print(df)

    plots.simple_line_plot(df=df, filename=f"{plot_dir}/simple-line",
            labels={"y":"delay [s]", "x":"tdd dl slots", "color":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%", x="tdd_config__tdd_dl_slots", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            errorbars=True
            )


    df_plot = df
    df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
    df_plot["direction_width"] = df_plot["direction"].apply(lambda x: 0.2 if x == "Ul" else 0.4)
    df_plot["direction_line"] = df_plot["direction"].apply(lambda x: "dashed" if x == "Ul" else "solid")
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/throughput_compare-tdd",
                          facets={"facet":p9.facet_grid(cols='gnb_version__type', scales="fixed")},
                           labels={"y":"throughput [Mbps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                          aesthetics=p9.aes(y="throughput__mean / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="direction", group="group"),
                          )
    df_plot = df_plot.query("failed_run == False")
    df_plot.drop(columns=[c for c in df_plot.columns if c not in ["gnb_version__type", "throughput__mean", "tdd_config__tdd_dl_ul_ratio", "tdd_config__tdd_dl_ul_tx_period", "direction", "direction_line", "direction_width", "dockerization"]], inplace=True)
    df_plot.to_csv("/tmp/ttt.csv")
    df_plot = pd.read_csv("/tmp/ttt.csv")
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/throughput_compare-tdd-b",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols='direction')},
                           labels={"y":"throughput [bps]", "x":"tdd dl/ul ratio", "color":"tdd period","fill":"tdd period", "linetype":"dockerization", "shape":"direction"},
                            lines=False,points=False,bars=True,errorbars=False,
                          aesthetics=p9.aes(y="throughput__mean", x="factor(tdd_config__tdd_dl_ul_ratio)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="dockerization"),
                           add_to_plot=[p9.scale_linetype_manual(["solid", "dotted"]), p9.guides(linetype = p9.guide_legend(title="dockerization", override_aes = {"size": 1.4, "fill":"#fff"}))]
                          )



    ## UHD and GNB VERSIONS
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 5 and tdd_config__tdd_dl_ul_ratio == 1")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp5--tddr1",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 10 and tdd_config__tdd_dl_ul_ratio == 1")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp10--tddr1",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 5 and tdd_config__tdd_dl_ul_ratio == 2")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp5--tddr2",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )
    df_plot = df.query("tdd_config__tdd_dl_ul_tx_period == 10 and tdd_config__tdd_dl_ul_ratio == 2")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-compare-gnb--tddp10--tddr2",
                          labels={"y":"delay [s]", "x":"gnb_version__combined", "color":"gnb_version__uhd_version", "fill":"gnb_version__uhd_version"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(gnb_version__combined)", color="factor(gnb_version__uhd_version)", fill="factor(gnb_version__uhd_version)", linetype="direction"),
                          )





    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001'")
    print(df_plot)
    plot_name=f"{plot_dir}/boxplots-cmp-0.35h-0.001s"
    df_plot.to_csv(f"{plot_name}.csv")
    plots.box_plot_manual(df=df_plot, filename=plot_name,
                          facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
                          limits={"ylim":[0.0, 0.025], "cartesian":True},
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )




    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )

    #

    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
                              labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                              aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                              )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    if len(df_plot) > 0:
        plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
                labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                              facets={"facet":p9.facet_grid(cols='dockerization', scales="fixed")},
                aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                )


def plot_per_setup_aggregated_runs(ansible_dump = ansible_dump):
    df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")

    df_plot = df
    df_plot["group"]=df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(str) + df_plot["direction"].astype(str)
    df_plot["dockerization_label"] = df_plot["dockerization"].apply(lambda x : "docker" if x else "bare")
    df_plot["throughput__mean__agg__ci_95_l"] = df_plot["throughput__mean__agg__ci_95_l"].apply(lambda x: 0 if x<0 else x)
    df_plot["delay__mean__agg__ci_95_l"] = df_plot["delay__mean__agg__ci_95_l"].apply(lambda x: 0 if x<0 else x)
    df_plot["tdd_config__tdd_dl_ul_ratio"] = df_plot["tdd_config__tdd_dl_ul_ratio"].astype(pd.api.types.CategoricalDtype(categories=[1,2,4], ordered=True))
    # df_plot["tdd_config__tdd_dl_ul_tx_period"] = df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(pd.api.types.CategoricalDtype(categories=[5,10,20], ordered=True))
    # df_plot["tdd_config__tdd_dl_ul_tx_period"] = df_plot["tdd_config__tdd_dl_ul_tx_period"].apply(lambda x : f"{x:02d}")
    # df_plot["tdd_config__tdd_dl_ul_tx_period"] = df_plot["tdd_config__tdd_dl_ul_tx_period"].astype(pd.api.types.CategoricalDtype(categories=["05","10","20"], ordered=True))
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_throughput_compare-tdd",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols="dockerization_label", scales="fixed")},
                          labels={"y":"throughput [Mbps]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="direction", group="group"),
                          )
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_delay_compare-tdd",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols="dockerization_label", scales="fixed")},
                          labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                          errorbars=True,
                          aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="direction", group="group"),
                          )
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_throughput_compare-tdd-b",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols=["direction", "dockerization_label"], scales="fixed")},
                          labels={"y":"throughput [Mbps]", "x":"tdd dl/ul ratio", "fill":"tdd period", "linetype":"dockerization", "shape":"direction"},
                          errorbars=True,bars=True,lines=False,points=False,
                          aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000", ymin="throughput__mean__agg__ci_95_l / 1000000",ymax="throughput__mean__agg__ci_95_u / 1000000", x="factor(tdd_config__tdd_dl_ul_ratio)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="dockerization_label", group="group"),
                          add_to_plot=[p9.scale_linetype_discrete(guide=None)]
                          )
    plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_delay_compare-tdd-b",
                          facets={"facet":p9.facet_grid("gnb_version__type",cols=["direction", "dockerization_label"], scales="free_y")},
                          labels={"y":"delay [s]", "x":"tdd dl/ul ratio", "fill":"tdd period", "linetype":"dockerization", "shape":"direction"},
                          errorbars=True,bars=True,lines=False,points=False,
                          aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="factor(tdd_config__tdd_dl_ul_ratio)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", shape="direction", linetype="dockerization_label", group="group"),
                          add_to_plot=[p9.scale_linetype_discrete(guide=None)]
                          )









def plot_per_run(p: str):
    print(p)
    df = pd.read_csv(p)
    query_range = int(df["SeqNum"].max() * 0.7)
    query_range1 = int(df["SeqNum"].max() * 0.6)
    query_range2 = int(df["SeqNum"].max() * 0.5)

    for l in [100, 1000]:
        if query_range -l < 50:
            continue
        plots.simple_line_plot(df=df.query(f"SeqNum > {query_range -l } and SeqNum < {query_range}"), filename=f"{os.path.dirname(p)}/timeline-delay-{l}",
                labels={"y":"delay [s]", "x":"sequence number [#]", "color":"trafficflow"},
                aesthetics=p9.aes(y="delay", x="SeqNum", color="trafficflow"),
                errorbars=False
                )
        plots.simple_line_plot(df=df.query(f"SeqNum > {query_range -l } and SeqNum < {query_range}"), filename=f"{os.path.dirname(p)}/timeline-delay-{l}",
                labels={"y":"IAT [s]", "x":"sequence number [#]", "color":"trafficflow"},
                aesthetics=p9.aes(y="IAT", x="SeqNum", color="trafficflow"),
                errorbars=False
                )

    """ time x seq.num plots """
    for s in [query_range2, query_range1, query_range]:
        for l in [100, 1000, 10000]:
            if s -l < 50:
                continue
            plots.simple_line_plot(df=df.query(f"SeqNum > {s -l } and SeqNum < {s}"), filename=f"{os.path.dirname(p)}/timeline-sending-{l}-s{s}",
                    lines= False,
                    labels={"y":"seq. number [#]", "x":"timestamp [s]", "color":"location"},
                    aesthetics=p9.aes(y="SeqNum", x="Timestamp", color="location"),
                    errorbars=False
                    )

    """ delay vs IAT -> retransmission? """
    for s in [query_range2, query_range1, query_range]:
        for l in [100, 1000, 10000]:
            if s -l < 50:
                continue
            plots.simple_line_plot(df=df.query(f"SeqNum > {s -l } and SeqNum < {s}"), filename=f"{os.path.dirname(p)}/timeline-delay_x_iat-{l}-s{s}",
                    lines= False,
                    labels={"y":"seq. number [#]", "x":"timestamp [s]", "color":"location"},
                    aesthetics=p9.aes(y="SeqNum", x="Timestamp", color="location"),
                    errorbars=False
                    )


    """ timeline plots, showing delay in combination with channel values """
    query_range = int(df["SeqNum"].max() * 0.7)

    def exctract_columns(zscore:bool, column_name: str):
        if zscore:
            ret = column_name.endswith("_z")
        else:
            ret = not column_name.endswith("_z")
        return column_name !="TIMESTAMP" and ret


    for zstd in [True, False]:
        for l in [100, 1000, 10000, 20000]:
            if query_range -l < 50:
                continue
            dft = df.query(f"SeqNum > {query_range - l} and SeqNum < {query_range}")
            dft["channel"] = "Delay"
            ts_min = dft["Timestamp"].min()
            ts_max = dft["Timestamp"].max()
            if pd.isna(ts_min) or pd.isna(ts_max):
                continue

            df_modem = pd.read_csv(os.path.dirname(p) + "/modem-snr_z.csv")
            df_modem = df_modem.drop(columns=[c for c in df_modem.columns if exctract_columns(not zstd, c)])
            df_modem = pd.melt(df_modem, id_vars=["TIMESTAMP"], var_name="metrictype").dropna().reset_index(drop=True)
            df_modem_t = df_modem.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
            if len(df_modem_t)>0:
                # df_modem_t["channel"] = "MODEM"
                df_modem_t.loc[:,"channel"] = "MODEM"
                # df_modem_t["value"] = pd.to_numeric(df_modem_t["value"], errors="coerce")
                df_modem_t.loc[:,"value"] = pd.to_numeric(df_modem_t["value"], errors="coerce")

            df_gnb = pd.read_csv(os.path.dirname(p) + "/gnb_snr_z.csv")
            df_gnb = df_gnb.drop(columns=[c for c in df_gnb.columns if exctract_columns(not zstd, c)])
            df_gnb = pd.melt(df_gnb, id_vars=["TIMESTAMP"], var_name="metrictype").dropna().reset_index(drop=True)
            df_gnb_t = df_gnb.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
            if len(df_gnb_t)>0:
                # df_gnb_t["channel"] = "GNB"
                df_gnb_t.loc[:,"channel"] = "GNB"
                # df_gnb_t["value"] = pd.to_numeric(df_gnb_t["value"], errors="coerce")
                df_gnb_t.loc[:,"value"] = pd.to_numeric(df_gnb_t["value"], errors="coerce")


            dft[f"delay_r10"] = dft["delay"].rolling(10).sum() / 10
            dft[f"delay_r50"] = dft["delay"].rolling(50).sum() / 50
            dft[f"delay_r100"] = dft["delay"].rolling(100).sum() / 100
            dft[f"delay_r500"] = dft["delay"].rolling(500).sum() / 500
            dft[f"delay_r1000"] = dft["delay"].rolling(1000).sum() / 1000
            zstd_label = "-zstd" if zstd else ""
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay", x="Timestamp"),
                    points=False,
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_point(p9.aes(y="delay",x="Timestamp"),data=dft, size=plots.POINT_SIZE*0.5),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel_r10{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay_r10", x="Timestamp"),
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel_r50{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay_r50", x="Timestamp"),
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel_r100{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay_r100", x="Timestamp"),
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel_r500{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay_r500", x="Timestamp"),
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
            plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel_r1000{zstd_label}-{l}",
                    facets={"facet":p9.facet_grid('channel', scales="free")},
                    labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                    aesthetics=p9.aes(y="delay_r1000", x="Timestamp"),
                    errorbars=False,
                    add_to_plot=[
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_modem_t, size=plots.LINE_SIZE),
                        p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="metrictype"),data=df_gnb_t, size=plots.LINE_SIZE),
                        ]
                    )
    print(f"Completed: {p}\n\n")



def plots_per_run_mp(pcaps):
    with mp.Pool(1) as p:
        returns = p.map(plot_per_run, get_pcap_paths())




def _antenna_gain():
    antenna_msm_dumps = [
        # "/home/lks/Documents/datastore/5g-masterarbeit/antenna-gain_ul/",
        # "/home/lks/Documents/datastore/5g-masterarbeit/antenna_gain_dl/",
        # "/home/lks/Documents/datastore/5g-masterarbeit/antenna-gain-b205/"
        "/mnt/ext1/5g-masterarbeit-daten/antenna-gain-b205/"
            ]
    for ansible_dump in antenna_msm_dumps:
        plots_antenna_gain_aggregated_runs(ansible_dump)
        plots_antenna_gain_single_runs(ansible_dump)

def plots_antenna_gain_single_runs(ansible_dump):
    plot_dir = ansible_dump
    df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
    df = df.query("run == 0")
    df["gain_type"] = df["rx_gain"].apply(lambda x: "Rx" if x >= 0 else "Tx")
    df["gain_value"] = df.apply(lambda x: x["rx_gain"] if x["rx_gain"] >= 0 else x["tx_gain"], axis=1)
    print(df)

    plots.simple_line_plot(df=df, filename=f"{plot_dir}/simple-line",
            # facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
            facets={"facet":p9.facet_grid('gnb_version__type', scales="fixed")},
            labels={"y":"throughput [bps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean", x="gain_value", color="factor(gain_type)"),
            errorbars=False
            )

def plots_antenna_gain_aggregated_runs(ansible_dump):
    plot_dir = ansible_dump
    df = pd.read_csv(f"{ansible_dump}/all_runs_groupby_agg.csv")
    df["gain_type"] = df["rx_gain"].apply(lambda x: "Rx" if x >= 0 else "Tx")
    df["gain_value"] = df.apply(lambda x: x["rx_gain"] if x["rx_gain"] >= 0 else x["tx_gain"], axis=1)

    series_dirs = [ f"{ansible_dump}/{d}" for d in os.listdir(ansible_dump) if os.path.isdir(f"{ansible_dump}/{d}") and not os.path.basename(f"{ansible_dump}/{d}").startswith(".")]
    run_dirs = [ r.path[:-5] for series in series_dirs for r in os.scandir(series) if r.is_dir() and not os.path.basename(r).startswith(".")]
    run_dirs = list(dict.fromkeys(run_dirs))
    # print(ansible_dump)
    # print(run_dirs)


    ## Get SNR and channel metrics
    run_dir_stats = {}
    # TODO: erst auf einen run kürzen!
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
            if config["rx_gain"] != None:
                loc_query = (df["gain_value"] == config["rx_gain"]) & (df["gain_type"] == "Rx") \
                   & (df["gnb_version__type"] == config["gnb_version__type"]) & (df["traffic_config__traffic_type"] == config["traffic_config__traffic_type"]) \
                   & (df["traffic_config__direction"] == config["traffic_config__direction"]) \
                   & (df["tdd_config__tdd_dl_ul_tx_period"] == config["tdd_config__tdd_dl_ul_tx_period"]) \
                   & (df["tdd_config__tdd_dl_ul_ratio"] == config["tdd_config__tdd_dl_ul_ratio"])
            elif config["tx_gain"] != None:
                loc_query = (df["gain_value"] == config["tx_gain"]) & (df["gain_type"] == "Tx") \
                   & (df["gnb_version__type"] == config["gnb_version__type"]) & (df["traffic_config__traffic_type"] == config["traffic_config__traffic_type"]) \
                   & (df["traffic_config__direction"] == config["traffic_config__direction"]) \
                   & (df["tdd_config__tdd_dl_ul_tx_period"] == config["tdd_config__tdd_dl_ul_tx_period"]) \
                   & (df["tdd_config__tdd_dl_ul_ratio"] == config["tdd_config__tdd_dl_ul_ratio"])
            else:
                raise ValueError("Oops")
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

    # df_ = pd.melt(df, id_vars=["gain_value", "direction", "gnb_version__type"], value_vars=["MODEM_SNR", "GNB_SNR"] )
    # plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-modem_snr-agg_alt01",
    #         facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
    #         labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
    #         aesthetics=p9.aes(y="MODEM_SNR", x="gain_value", color="factor(gain_type)"),
    #         ratio="16:9",
    #         errorbars=False
    #         )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-modem_snr-agg_alt01",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="MODEM_SNR",ymax="MODEM_SNR + MODEM_SNR_ci", ymin="MODEM_SNR - MODEM_SNR_ci", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df[ df["MODEM_SNR"] == df["MODEM_SNR"] ], filename=f"{plot_dir}/rxtx-gain-modem_snr-agg_alt01_nona",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="MODEM_SNR", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=False
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-gnb_snr-agg_alt01",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="GNB_SNR",ymax="GNB_SNR + GNB_SNR_ci", ymin="GNB_SNR - GNB_SNR_ci",  x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-gnb_snr-agg_alt01-noebar",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="GNB_SNR",ymax="GNB_SNR + GNB_SNR_ci", ymin="GNB_SNR - GNB_SNR_ci",  x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=False
            )
    plots.simple_line_plot(df=df[ df["MODEM_SNR"] == df["MODEM_SNR"] ], filename=f"{plot_dir}/rxtx-gain-gnb_snr-agg_alt01_nona",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"SNR [dB]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="GNB_SNR", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=False
            )


    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg",
            facets={"facet":p9.facet_grid('gnb_version__type', cols="direction", scales="free_y")},
            labels={"y":"throughput [Mbps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000",ymin="throughput__mean__agg__ci_95_l / 1000000", ymax="throughput__mean__agg__ci_95_u / 1000000", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg_alt01",
            facets={"facet":p9.facet_grid('direction', cols="gnb_version__type", scales="free_y")},
            labels={"y":"throughput [Mbps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000",ymin="throughput__mean__agg__ci_95_l / 1000000", ymax="throughput__mean__agg__ci_95_u / 1000000", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg_zoom",
            facets={"facet":p9.facet_grid('gnb_version__type', cols="direction", scales="fixed")},
            labels={"y":"throughput [Mbps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__mean / 1000000",ymin="throughput__mean__agg__ci_95_l / 1000000", ymax="throughput__mean__agg__ci_95_u / 1000000", x="gain_value", color="factor(gain_type)"),
            limits={"ylim":[0,60], "cartesian": False},
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg-ci-width",
            facets={"facet":p9.facet_grid('gnb_version__type', cols="direction", scales="free_y")},
            labels={"y":"width of 95% CI [Mbps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__ci_95 / 1000000", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=False
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-delay-agg",
            facets={"facet":p9.facet_grid('gnb_version__type', cols="direction", scales="free_y")},
            labels={"y":"delay [s]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="delay__mean__agg__mean",ymin="delay__mean__agg__ci_95_l", ymax="delay__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-delay-agg_alt01",
            facets={"facet":p9.facet_grid("direction", cols='gnb_version__type', scales="free_y")},
            labels={"y":"delay [s]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="delay__mean__agg__mean",ymin="delay__mean__agg__ci_95_l", ymax="delay__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-delay-agg_alt01_zoom",
            facets={"facet":p9.facet_grid("direction", cols='gnb_version__type', scales="free_y")},
            labels={"y":"delay [s]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="delay__mean__agg__mean",ymin="delay__mean__agg__ci_95_l", ymax="delay__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
                           limits={"ylim":[0,5], "cartesian": True},
            ratio="16:9",
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-delay-agg_zoom",
            facets={"facet":p9.facet_grid('gnb_version__type', cols="direction", scales="free_y")},
            labels={"y":"delay [s]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="delay__mean__agg__mean",ymin="delay__mean__agg__ci_95_l", ymax="delay__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
                           limits={"ylim":[0,5], "cartesian": False},
            ratio="16:9",
            errorbars=True
            )


# TODO: take cli flag: either parse the generall eval or work on a per packet level


if __name__ == "__main__":

    # _antenna_gain()
    # # _scenario_gnb_versions_delay()
    # _scenario_throughput_overshoot()
    # _scenario_performance_tuning()
    # _scenario_tdd_algo()
    _scenario_dockerization()
    # _scenario_qam()
    # _scenario_height()
    # _scenario_distance()
    # _scenario_distance_wall()
    # _scenario_main_measurements()





    # plot_per_setup()
    # plots_per_run_mp(get_pcap_paths())

    # plot_per_run("/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/srsRAN_651c9a37/srsRAN_651c9a37__rx-40__002/combined.csv.gz")
    #plot_per_run("/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/srsRAN_651c9a37/srsRAN_651c9a37__tx-62__000/combined.csv.gz")





    ## parser = argparse.ArgumentParser(
    ##     prog="create test configuration",
    ##     description="Create .yaml file which contains all variables need for ansible"
    ##         )
    ## parser.add_argument("filename")
    ## args = parser.parse_args()



    # system["identifier"] = dict_to_small_hash(fixed_params)
    # system["fixed_params"] = fixed_params
    # d = {"system":system, "run_to_run_variation":create_param_combinations()}
    # run_config_str = yaml.dump(d, sort_keys=False, indent=4)
    # print(run_config_str)

    # with open(args.filename, "w") as f:
    #     f.write(run_config_str)

