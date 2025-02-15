import plotninesettings
import plots
import pandas as pd
import plotnine as p9
import argparse



ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps_c80/"
ansible_dump = "/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/"




def plot_per_setup():
    df = pd.read_parquet(f"{ansible_dump}/all_runs.parquet")
    print(df)

    plots.simple_line_plot(df=df, filename="simple-line",
            labels={"y":"delay [s]", "x":"tdd dl slots", "color":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%", x="tdd_config__tdd_dl_slots", color="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            errorbars=True
            )
    plots.box_plot_manual(df=df.query("distance_vertical_in_m == 0"), filename="boxplots-0h",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )
    plots.box_plot_manual(df=df.query("distance_vertical_in_m == 0.35"), filename="boxplots-0.35h",
            labels={"y":"delay [s]", "x":"tdd dl ul ratio", "color":"tdd period", "fill":"tdd period"},
            aesthetics=p9.aes(y="delay__mean", ymax="delay__95%", ymin="delay__5%",middle="delay__50%", lower="delay__25%", upper="delay__75%", x="factor(tdd_config__tdd_dl_ul_ratio)", color="factor(tdd_config__tdd_dl_ul_tx_period)", fill="factor(tdd_config__tdd_dl_ul_tx_period)", linetype="direction"),
            )




# TODO: take cli flag: either parse the generall eval or work on a per packet level


if __name__ == "__main__":
    plot_per_setup()
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

