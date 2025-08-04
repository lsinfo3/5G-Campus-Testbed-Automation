import plotnine as p9
from plotnine.data import diamonds
from plotninesettings import PLOT_W, PLOT_H, LINE_SIZE, POINT_SIZE, WIDTH, ERRWIDTH, COLORS, COLORS_TC, PLOTCOLORS, GLOBAL_THEME, COLOR_MAP, COLOR_MAP_EXTRACTOR, \
        brighten, darken
import pandas as pd
import numpy as np
import scipy.stats
import natsort
import copy

from itertools import combinations


# df = diamonds
# print(df)



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








# df = pd.read_csv("/mnt/ext1/5g-masterarbeit-daten/main_measurement_qam64256/all_runs.csv.gz")
df = pd.read_csv("/mnt/ext1/5g-masterarbeit-daten/main_measurement/all_runs.csv.gz")
# print(df)
# print(df.columns)
# print(len(df))

df.rename(columns={
"gnb_version__type": "gnb_type",
"gnb_version__version":"gnb_version",
"gnb_version__uhd_version":"uhd_version",
"traffic_config__traffic_type" : "traffic_type",
"traffic_config__iat" : "iat",
"traffic_config__size" : "size",
"tdd_config__tdd_dl_ul_ratio" : "tdd_ratio",
"tdd_config__tdd_dl_ul_tx_period" : "tdd_period",
    }, inplace=True)
df.loc[:,"traffic_type_config"] = "" + df["traffic_type"].astype(str) + "__" + df["direction"].astype(str) + \
        "__" + df["iat"].astype(str) + "__" + df["size"].astype(str)



df["direction"] = df["direction"].apply(lambda x : x.upper())
df["tdd_ratio"] = df["tdd_ratio"].astype(str) + ":1"
df["tdd_period"] = df["tdd_period"].astype(str) + " slots"


id = "identifier"
metrics = ["throughput__mean", "delay__mean"]

params_base_iperf = [
"direction",
"gnb_type",
"tdd_ratio",
"tdd_period",
"gnb_version",
# "uhd_version",
        ]


params_base_ping = [
"direction",
"size",
"iat",
"gnb_type",
"tdd_period",
# "uhd_version",
        ]


df_means_dict_base = {
        "throughput":[],"throughput_l":[],"throughput_u":[],
        "delay":[],"delay_l":[],"delay_u":[],
        "param_1":[], "value_1":[],
        "param_2":[], "value_2":[]
        }

for interaction_plot_metric, interaction_plot_params in [
        ("throughput",params_base_iperf[:3]), ("throughput",params_base_iperf[:4]),("throughput",params_base_iperf[:5]),
        ("delay",params_base_ping[:3]), ("delay",params_base_ping[:4]), ("delay",params_base_ping[:5]),
        ]:
    df_means_dict = copy.deepcopy(df_means_dict_base)
    df_copy = df.copy(deep=True)
    if interaction_plot_metric == "throughput":
        df_copy = df_copy.drop(df_copy[ df_copy["traffic_type"] == "scapyudpping" ].index)
    else:
        df_copy = df_copy.drop(df_copy[ df_copy["traffic_type"] == "iperfthroughput" ].index)
    df_copy.drop(df_copy[ df_copy["failed_run"] == True ].index, inplace=True )
    df_copy.reset_index(drop=True, inplace=True)
    df_copy = df_copy.drop(columns=[ c for c in df.columns if c not in interaction_plot_params and c != id and c not in metrics ])
    df_copy.reset_index(drop=True, inplace=True)
    print(df_copy)
    print(df_copy.columns)

    for p in interaction_plot_params:
        df_copy[p] = pd.Categorical(df_copy[p], ordered=True, categories=natsort.natsorted(df_copy[p].unique()) )
    # df_melted = df_copy.melt(id_vars=[id]+metrics)
    # print(df_melted)

    # df_melted = df_melted.merge(df_melted, how='outer', on="identifier")
    # if interaction_plot_metric == "throughput":
    #     df_melted.rename( { "throughput__mean_x":"throughput" }, inplace=True )
    #     df_melted.drop(columns=["throughput__mean_y"],inplace=True)
    # else:
    #     df_melted.rename( { "delay__mean_x":"throughput" }, inplace=True )
    #     df_melted.drop(columns=["delay__mean_y"],inplace=True)
    # print(df_melted)
    # print(df_melted.columns)
    # print(df_melted["variable_x"].value_counts())
    # print(df_melted["value_x"].value_counts())
    # print("df_melted\n")


    for p1 in interaction_plot_params:
        for u1 in df_copy[p1].unique():
            for p2 in interaction_plot_params:
                if p2 == p1:
                    continue
                for u2 in df[p2].unique():
                    t_m = df_copy.loc[ (df_copy[p1]==u1) & (df_copy[p2]==u2), "throughput__mean"].mean()
                    t_ci = df_copy.loc[ (df_copy[p1]==u1) & (df_copy[p2]==u2), "throughput__mean"].agg(mean_confidence_interval(0.95))
                    d_m = df_copy.loc[ (df_copy[p1]==u1) & (df_copy[p2]==u2), "delay__mean"].mean()
                    d_ci = df_copy.loc[ (df_copy[p1]==u1) & (df_copy[p2]==u2), "delay__mean"].agg(mean_confidence_interval(0.95))
                    print(f"[({p1}={u1})({p2}={u2})]:{t_m}")
                    df_means_dict["throughput"].append(t_m)
                    df_means_dict["throughput_l"].append(t_m-t_ci)
                    df_means_dict["throughput_u"].append(t_m+t_ci)
                    df_means_dict["delay"].append(d_m)
                    df_means_dict["delay_l"].append(d_m-d_ci)
                    df_means_dict["delay_u"].append(d_m+d_ci)
                    df_means_dict["param_1"].append(p1)
                    df_means_dict["param_2"].append(p2)
                    df_means_dict["value_1"].append(u1)
                    df_means_dict["value_2"].append(u2)

    df_interactions = pd.DataFrame(df_means_dict)
    for c in ["param_1", "param_2", "value_1", "value_2"]:
        df_interactions[c] = pd.Categorical(df_interactions[c], ordered=True, categories=natsort.natsorted(df_interactions[c].unique()) )
    print(df_interactions)
    print(df_interactions.dtypes)
    # df_interactions.dropna(inplace=True)
    df_interactions.drop(df_interactions[ df_interactions[interaction_plot_metric].isna() ].index, inplace=True )
    print(df_interactions)
    print(df_interactions.columns)
    print(df_interactions["param_1"].value_counts())
    print(df_interactions["value_1"].value_counts())
    print("df_interactions\n")

    print(df)
    print("DF BASE *2")


    max_params = max([df[p].nunique() for p in interaction_plot_params])
    assert(max_params < 5)
    c = COLOR_MAP_EXTRACTOR(max_params)
    assign_colors = {}
    # print(df_copy["tdd_ratio"].unique())
    # print( pd.Series(pd.Categorical(df_copy["tdd_ratio"].unique(), ordered=True)) )
    # print(natsort.natsorted(df_copy["tdd_ratio"].unique()))
    for p in interaction_plot_params:
        for i,u in enumerate(  natsort.natsorted(df_copy[p].unique())  ):
            assign_colors[u] = c[i]


    if interaction_plot_metric == "throughput":
        plot = (p9.ggplot(df_interactions)
                + p9.facet_grid("param_1", cols="param_2", scales="free_x")
                + p9.aes(x="value_2",y="throughput/1000000",ymin="throughput_l/1000000",ymax="throughput_u/1000000",
                         group="value_1", color="value_1")
                + p9.labs(x="",y="throughput [Mbps]", color="")
                + p9.geom_line(size=LINE_SIZE)
                + p9.geom_errorbar()
                + p9.geom_point(size=POINT_SIZE*1.5)
                + p9.scale_color_manual(values=assign_colors, limits=list(assign_colors.keys()))
                + GLOBAL_THEME()
                )
        plot.save(f"main_interactions_{len(interaction_plot_params)}_iperf_{interaction_plot_metric}.pdf", width=PLOT_W, height=PLOT_H)
        plot.save(f"main_interactions_{len(interaction_plot_params)}_iperf_{interaction_plot_metric}.jpg", width=PLOT_W, height=PLOT_H)
    elif interaction_plot_metric == "delay":
        print(df_interactions)
        print(df_interactions[ df_interactions["param_1"] == "iat" ].value_counts())
        plot = (p9.ggplot(df_interactions)
                + p9.facet_grid("param_1", cols="param_2", scales="free_x")
                + p9.aes(x="value_2",y="delay",ymin="delay_l",ymax="delay_u", group="value_1", color="value_1")
                + p9.geom_line(size=LINE_SIZE)
                + p9.labs(x="",y="delay [s]", color="")
                + p9.geom_errorbar()
                + p9.geom_point(size=POINT_SIZE*1.5)
                + p9.scale_color_manual(values=assign_colors, limits=list(assign_colors.keys()))
                + GLOBAL_THEME()
                )
        plot.save(f"main_interactions_{len(interaction_plot_params)}_scapy_{interaction_plot_metric}.pdf", width=PLOT_W, height=PLOT_H)
        plot.save(f"main_interactions_{len(interaction_plot_params)}_scapy_{interaction_plot_metric}.jpg", width=PLOT_W, height=PLOT_H)
    else:
        raise ValueError()













