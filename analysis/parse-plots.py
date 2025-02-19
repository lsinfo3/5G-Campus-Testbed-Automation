import plotninesettings
import plots
import pandas as pd
import plotnine as p9
import argparse
import os
import multiprocessing as mp





ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps_c80/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/data/dumps/"
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

    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.01'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-0.35h-0.01s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.001'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-0.35h-0.001s",
                          labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
                          aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
                          )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.01'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-0.35h-0.01s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )
    df_plot = df.query("distance_vertical_in_m == 0.35 and traffic_config__scapy_iat == '0.001'")
    print(df_plot)
    plots.box_plot_manual(df=df_plot, filename=f"{plot_dir}/boxplots-0.35h-0.001s",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )


def plot_per_run(p: str):
    df = pd.read_csv(p)
    plots.simple_line_plot(df=df.query("SeqNum > 1000 and SeqNum < 1100"), filename=f"{os.path.dirname(p)}/delay-timeline",
            labels={"y":"delay [s]", "x":"sequence number [#]", "color":"type"},
            aesthetics=p9.aes(y="delay", x="SeqNum", color="type"),
            errorbars=False
            )
    plots.simple_line_plot(df=df.query("SeqNum > 1000 and SeqNum < 1100"), filename=f"{os.path.dirname(p)}/iat-timeline",
            labels={"y":"IAT [s]", "x":"sequence number [#]", "color":"type"},
            aesthetics=p9.aes(y="IAT", x="SeqNum", color="type"),
            errorbars=False
            )

def plots_per_run_mp(pcaps):
    with mp.Pool(8) as p:
        returns = p.map(plot_per_run, get_pcap_paths())




# TODO: take cli flag: either parse the generall eval or work on a per packet level


if __name__ == "__main__":
    # plot_per_setup()
    plots_per_run_mp(get_pcap_paths())



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

