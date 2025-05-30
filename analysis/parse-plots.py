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
    test_configurations = sorted(test_configurations)
    runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir()]
    pcaps = [pcap.path for r in runs for pcap in os.scandir(r) if pcap.is_file() and (pcap.path.endswith(".pcap") or pcap.path.endswith(".pcap.gz"))]

    pcaps = [os.path.dirname(p) for p in pcaps]
    pcaps = list(set(pcaps))
    pcaps = [f"{p}/combined.csv.gz" for p in pcaps]
    return pcaps



def _scenario_throughput_overshoot():
    for ansible_dump in ["/home/lks/Documents/datastore/5g-masterarbeit/throughput-overshoot", "/home/lks/Documents/datastore/5g-masterarbeit/throughput-overshoot-scapy"]:
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
        plots.simple_line_plot(df=df_plot, filename=f"{plot_dir}/agg_delay_compare-bandwidth",
                              facets={"facet":p9.facet_grid("gnb_version__type",cols="tdd_ratio_label", scales="fixed")},
                              labels={"y":"delay [s]", "x":"generated data rate [Mbps]", "color":"tdd period", "linetype":"direction", "shape":"direction"},
                              errorbars=True,
                              aesthetics=p9.aes(y="delay__mean__agg__mean", ymin="delay__mean__agg__ci_95_l",ymax="delay__mean__agg__ci_95_u", x="bandwidth_sent", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction", group="group"),
                              )
        # for p in get_pcap_paths(ansible_dump)[:4]:
        #     plot_per_run(p)



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
    for l in [100, 1000, 10000, 20000]:
        if query_range -l < 50:
            continue
        dft = df.query(f"SeqNum > {query_range - l} and SeqNum < {query_range}")
        dft["channel"] = "Delay"
        ts_min = dft["Timestamp"].min()
        ts_max = dft["Timestamp"].max()

        df_modem = pd.read_csv(os.path.dirname(p) + "/modem-snr.csv")
        df_modem = pd.melt(df_modem, id_vars=["TIMESTAMP"], var_name="metrictype").dropna().reset_index(drop=True)
        df_modem["channel"] = "MODEM"
        df_modem_t = df_modem.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
        df_modem_t["value"] = pd.to_numeric(df_modem_t["value"], errors="coerce")

        df_gnb = pd.read_csv(os.path.dirname(p) + "/gnb_snr.csv")
        df_gnb = pd.melt(df_gnb, id_vars=["TIMESTAMP"], var_name="metrictype").dropna().reset_index(drop=True)
        df_gnb["channel"] = "GNB"
        df_gnb_t = df_gnb.query(f"TIMESTAMP >= {ts_min} and TIMESTAMP <= {ts_max}")
        df_gnb_t["value"] = pd.to_numeric(df_gnb_t["value"], errors="coerce")

        plots.simple_line_plot(df=dft, filename=f"{os.path.dirname(p)}/iat-channel-{l}",
                facets={"facet":p9.facet_grid('channel', scales="free")},
                labels={"y":"delay [s] / dB", "x":"time [s]", "color":"metric"},
                aesthetics=p9.aes(y="delay", x="Timestamp"),
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




def plots_antenna_gain():
    antenna_msm_dumps = [
        "/home/lks/Documents/datastore/5g-masterarbeit/antenna-gain_ul/",
        "/home/lks/Documents/datastore/5g-masterarbeit/antenna_gain_dl/"
            ]
    for ansible_dump in antenna_msm_dumps:
        plots_antenna_gain_single_runs(ansible_dump)
        plots_antenna_gain_aggregated_runs(ansible_dump)

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

    # _scenario_gnb_versions_delay()
    _scenario_throughput_overshoot()


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

