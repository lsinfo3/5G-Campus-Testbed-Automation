import plotninesettings
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
# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"

# ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/plottests"


plot_dir = ansible_dump

def get_pcap_paths():
    test_configurations = [e.path for e in os.scandir(ansible_dump) if e.is_dir()]
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    pcaps = [os.path.dirname(p) for p in pcaps]
    pcaps = list(set(pcaps))
    pcaps = [f"{p}/combined.csv.gz" for p in pcaps]
    return pcaps




def plot_per_setup():
    df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
    print(df)

    plots.simple_line_plot(df=df, filename=f"{plot_dir}/simple-line",
            labels={"y":"delay [s]", "x":"tdd dl slots", "color":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%", x="tdd_config__tdd_dl_slots", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            errorbars=True
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





    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__scapy_iat == '0.001'")
    print(df_plot)
    plot_name=f"{plot_dir}/boxplots-cmp-0.35h-0.001s"
    df_plot.to_csv(f"{plot_name}.csv")
    plots.box_plot_manual(df=df_plot, filename=plot_name,
                          facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
                          limits={"ylim":[0.0, 0.025], "cartesian":True},
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )




    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__scapy_iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__scapy_iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__scapy_iat == '0.01' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.01s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )
    df_plot = df.query("distance_vertical_in_m == 0.34 and traffic_config__scapy_iat == '0.001' and gnb_version__type == 'srsRAN'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-srsRAN-0.35h-0.001s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )

    #

    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.01' and gnb_version__type == 'OAI'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.01s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.001' and gnb_version__type == 'OAI'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-OAI-0.35h-0.001s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
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
                labels={"y":"delay [s]", "x":"sequence number [#]", "color":"type"},
                aesthetics=p9.aes(y="delay", x="SeqNum", color="type"),
                errorbars=False
                )
        plots.simple_line_plot(df=df.query(f"SeqNum > {query_range -l } and SeqNum < {query_range}"), filename=f"{os.path.dirname(p)}/timeline-delay-{l}",
                labels={"y":"IAT [s]", "x":"sequence number [#]", "color":"type"},
                aesthetics=p9.aes(y="IAT", x="SeqNum", color="type"),
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
    for l in [100, 1000, 10000, 20000]:
        if query_range -l < 50:
            continue
        dft = df.query(f"SeqNum > {query_range - l} and SeqNum < {query_range}")
        dft["channel"] = "Delay"
        ts_min = dft["Timestamp"].min()
        ts_max = dft["Timestamp"].max()

        df_modem = pd.read_csv(os.path.dirname(p) + "/modem-snr.csv")
        df_modem = pd.melt(df_modem, id_vars=["TIMESTAMP"], var_name="type").dropna().reset_index(drop=True)
        df_modem["channel"] = "MODEM"
        df_modem_t = df_modem.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
        df_modem_t["value"] = pd.to_numeric(df_modem_t["value"], errors="coerce")

        df_gnb = pd.read_csv(os.path.dirname(p) + "/gnb_snr.csv")
        df_gnb = pd.melt(df_gnb, id_vars=["TIMESTAMP"], var_name="type").dropna().reset_index(drop=True)
        df_gnb["channel"] = "GNB"
        df_gnb_t = df_gnb.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
        df_gnb_t["value"] = pd.to_numeric(df_gnb_t["value"], errors="coerce")

        plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel-{l}",
                facets={"facet":p9.facet_grid('channel', scales="free")},
                labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                aesthetics=p9.aes(y="delay", x="Timestamp"),
                errorbars=False,
                add_to_plot=[
                    p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="type"),data=df_modem_t, size=plots.LINE_SIZE),
                    p9.geom_line(p9.aes(y="value",x="TIMESTAMP",color="type"),data=df_gnb_t, size=plots.LINE_SIZE),
                    ]
                )
    print(f"Completed: {p}\n\n")



def plots_per_run_mp(pcaps):
    with mp.Pool(1) as p:
        returns = p.map(plot_per_run, get_pcap_paths())




def plots_antenna_gain():
    plots_antenna_gain_single_runs()
    plots_antenna_gain_aggregated_runs()

def plots_antenna_gain_single_runs():
    ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/"
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

def plots_antenna_gain_aggregated_runs():
    ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/"
    plot_dir = ansible_dump
    df = pd.read_parquet(f"{ansible_dump}/all_runs_groupby_agg.parquet")
    df["gain_type"] = df["rx_gain"].apply(lambda x: "Rx" if x >= 0 else "Tx")
    df["gain_value"] = df.apply(lambda x: x["rx_gain"] if x["rx_gain"] >= 0 else x["tx_gain"], axis=1)

    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg",
            # facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
            facets={"facet":p9.facet_grid('gnb_version__type', scales="fixed")},
            labels={"y":"throughput [bps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__mean",ymin="throughput__mean__agg__ci_95_l", ymax="throughput__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
            errorbars=True
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-throughput-agg-ci-width",
            # facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
            facets={"facet":p9.facet_grid('gnb_version__type', scales="free")},
            labels={"y":"width of 95% CI [bps]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="throughput__mean__agg__ci_95", x="gain_value", color="factor(gain_type)"),
            errorbars=False
            )
    plots.simple_line_plot(df=df, filename=f"{plot_dir}/rxtx-gain-delay-agg",
            # facets={"facet":p9.facet_grid('gnb_version__uhd_version', cols='gnb_version__combined', scales="fixed")},
            facets={"facet":p9.facet_grid('gnb_version__type', scales="fixed")},
            labels={"y":"delay [s]", "x":"gain [dB]", "color":"gain_type"},
            aesthetics=p9.aes(y="delay__mean__agg__mean",ymin="delay__mean__agg__ci_95_l", ymax="delay__mean__agg__ci_95_u", x="gain_value", color="factor(gain_type)"),
            errorbars=True
            )


# TODO: take cli flag: either parse the generall eval or work on a per packet level


if __name__ == "__main__":

    # plots_antenna_gain()

    # plot_per_setup()
    plots_per_run_mp(get_pcap_paths())

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

