import pandas as pd
from pandas.core.api import DataFrame
import numpy as np
import scipy.stats
import os
import sys
import hashlib
import glob

import re

import random

from plotnine.ggplot import ggplot
from plotnine import labs, aes, \
        facet_grid, facet_wrap, \
        geom_line, geom_point, geom_bar, geom_errorbar, geom_boxplot, stat_boxplot, geom_col, geom_segment, \
        theme, theme_light, theme_dark, theme_gray, element_text, element_line, element_rect, element_blank, guide_legend, guide_colorbar, \
        scale_x_log10, scale_y_log10, scale_x_continuous, scale_y_continuous, scale_y_discrete, scale_x_discrete, coord_cartesian, coord_trans, \
        scale_color_manual, scale_fill_manual, \
        ylim, xlim, position_dodge

from plotninesettings import PLOT_W, PLOT_H, LINE_SIZE, POINT_SIZE, COLORS, COLORS_TC, PLOTCOLORS, GLOBAL_THEME, brighten, darken

projectdir = "."

def mean_confidence_interval(data, confidence=0.95):
    """
    Get mean and the lower and upper limit for the confidence interval
    """
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    ci = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, ci


def data_import_per_file(f_name:str, summarize=True):
    if m:= re.match(r".*SIZE_(\w+)__CORES_(\w+)__RATE_(\w+)", os.path.basename(f_name)):
        SIZE = f"{m.group(1)}B"
        CORES = int(m.group(2))
        RATE = str(m.group(3))

        # df_tmp = pd.read_csv(f_name,sep=",",usecols=["TS"],dtype={'TS':np.int64},on_bad_lines='warn')
        df_tmp = pd.read_csv(f_name,sep=",",usecols=["TS"],on_bad_lines='warn',low_memory=False)
        print(df_tmp.value_counts())
        if os.path.isfile(f"{f_name}_CORRECTION"):
            # INFO: this is a measurement of the switch, so correction is needed
            df_corr = pd.read_csv(f"{f_name}_CORRECTION",sep=",",usecols=["TS"],on_bad_lines='warn',low_memory=False)
            correction,_ = mean_confidence_interval(df_corr["TS"].to_numpy())
            df_tmp["TS"] = df_tmp["TS"] - correction
            # FIXME:
            df_tmp["TS"] = -df_tmp["TS"]
        else :
            # INFO: this is a measurement without a switch, so we asume mean = correction
            m,_ = mean_confidence_interval(df_tmp["TS"].to_numpy())
            df_tmp["TS"] = df_tmp["TS"]-m

        # mean = df_tmp.agg({"TS":"mean"}).item()
        # mean = int(mean)
        print("\n")
        mean, ci = mean_confidence_interval(df_tmp["TS"].to_numpy())
        if summarize:
            d = {
                    'MEAN':[mean],'MEAN_ci':[ci],
                    'MAX':[df_tmp["TS"].max().item()],
                    'MIN':[df_tmp["TS"].min().item()],
                    'VAR':[np.var(df_tmp["TS"].to_numpy())],
                    "SIZE":[SIZE],"CORES":[CORES],"RATE":[RATE],
                    "PKTS":[len(df_tmp)],
                    }
            return pd.DataFrame(d)
        else:
            df_tmp["MEAN"]=mean
            df_tmp["MEAN_ci"]=ci
            df_tmp["SIZE"]=SIZE
            df_tmp["CORES"]=CORES
            df_tmp["RATE"]=RATE
            return df_tmp
    raise ValueError(f"Filename doen't match re: {f_name}")

def data_import(dataroot):
    dataroot = os.path.expanduser(dataroot)
    df_parts = []
    for f in glob.glob(f"{dataroot}/*G.csv"):
        print(f)
        df_parts.append(data_import_per_file(f))
    else:
        df = pd.concat(df_parts, ignore_index=True)
    return df


def handle_import(dataroot)-> pd.DataFrame:
    """
    read from csv if available, else read all pcaps
    """
    summaryfile = f"summary_{hashlib.md5(dataroot.encode()).hexdigest()}.csv"
    if os.path.isfile(summaryfile):
        print("reading from file")
        datadf = pd.read_csv(summaryfile, index_col=[0])
        print(datadf)
    else:
        datadf = data_import(dataroot)
        print(datadf)
        print(datadf.dtypes)
        print()
        datadf.to_csv(summaryfile)
        datadf = pd.read_csv(summaryfile, index_col=[0])
        print(datadf)
        print(datadf.dtypes)
    return datadf

def box_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            # + facet_wrap("param", scales="free_x",nrow=1)
            # + aes(group="param")
            + aes(y='TS', x='SIZE')
            + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE)
            + geom_boxplot(outlier_shape="x", size=LINE_SIZE, outlier_size=LINE_SIZE,color=COLORS[0],fill=brighten(COLORS[0],4.0))
            # improve readability by adding , -> 1,000
            # + scale_x_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + scale_y_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + labs(
                title="pkt delay",
                x="frame size [B]",
                y="time [ns]",
                # color="tc:"
                )
            + GLOBAL_THEME(smallh=True)
            # + theme_light()
            # + scale_color_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def some_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            # + facet_wrap("param", scales="free_x",nrow=1)
            # + aes(group="param")
            + geom_line(aes(y='TS', x='SIZE'), data=df, size=LINE_SIZE)
            + geom_errorbar()
            # improve readability by adding , -> 1,000
            + scale_x_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + scale_y_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + labs(
                title="pkt delay normalized",
                x="frame size[B]",
                y="time[ns]",
                # color="tc:"
                )
            # + GLOBAL_THEME(smallh=True)
            + theme_light()
            + scale_color_manual(COLORS_TC, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def ts_mean_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + aes(y='MEAN', ymin="MIN", ymax="MAX", x='SIZE')
            + geom_errorbar(size=LINE_SIZE,color=COLORS[0])
            + geom_point(size=LINE_SIZE*4,color=COLORS[0])
            # + coord_cartesian()
            + labs(
                title="pkt delay normalized",
                x="frame size [B]",
                y="time [ns]",
                # color="tc:"
                )
            + GLOBAL_THEME()
            # + scale_color_manual(COLORS_TC, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def switch_success_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            # +aes(ymin=90)
            + aes(y='PKTS/100000', x='SIZE')
            # + geom_errorbar(size=DEFAULT_LINE_SIZE,color=COLORS[0])
            + geom_col(width=0.7/2,color=None,fill=COLORS[0])
            # + geom_segment(aes(y=1,yend='PKTS/100000', x='SIZE', xend='SIZE'), size=LINE_SIZE*24,color=COLORS[0])
            # + geom_point(aes(y='PKTS/100000', x='SIZE'), size=DEFAULT_LINE_SIZE*2,color=COLORS[0])
            + geom_errorbar(aes(ymin='(PKTS-PKTS_ci)/100000',ymax='(PKTS+PKTS_ci)/100000', x='SIZE', xend='SIZE'), width=0.3/2, size=LINE_SIZE, linetype="dashed",color=darken(COLORS[0]))
            # + geom_point(size=DEFAULT_LINE_SIZE*2,color=COLORS[0])
            # + geom_errorbar(size=DEFAULT_LINE_SIZE,color=COLORS[0])
            # + coord_cartesian(ylim=(1.94,2))
            + coord_cartesian(ylim=(90,100))
            # + scale_y_continuous(trans='log2')
            # + scale_y_continuous(trans='log2',breaks=[1,3.5,6,8.5,11], labels=['90','92.5','95','97.5','100'])

            # + scale_y_log10()

            # + scale_y_continuous(trans="log10")
            # + ylim(0.9,1.0)
            # + coord_trans(y="log10", ylim=(10e89,10e100))
            # + coord_trans(y="log10")
            + labs(
                title="pkt loss",
                x="frame size [B]",
                y="success [%]",
                # fill="rate",
                # color="tc:"
                )
            + GLOBAL_THEME()
            # + theme_light()
            # + scale_color_manual(COLORS_TC, drop=False)
            # + scale_fill_manual(COLORS_TC, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def switch_success_plot_interference(df:DataFrame, filename:str):# {{{
    def rename(x):
        if x['CORES'] == 3:
            return "STANDALONE"
        elif x['CORES'] == 10:
            return "SIMULTANEOUS"
        else:
            return "ERROR"
    df["CORES"] = df.apply(rename,axis=1)
    print(df)
    plot = (ggplot(df)
            + aes(y="PKTS/100000",ymin='(PKTS-4*PKTS_ci)/100000',ymax='(PKTS+4*PKTS_ci)/100000', x='SIZE', fill="CORES",color="CORES")
            # + geom_segment(aes(y=1,yend='PKTS/100000', x='SIZE', xend='SIZE'), size=LINE_SIZE*24,color=COLORS[0])
            # + geom_errorbar(aes(ymin='(PKTS-PKTS_ci)/100000',ymax='(PKTS+PKTS_ci)/100000', x='SIZE', xend='SIZE'), width=0.3, size=LINE_SIZE, linetype="dashed",color=darken(COLORS[0]))
            + geom_col(width=0.7,color=None,position=position_dodge(width=0.7),size=0)
            # + geom_segment(aes(y=1,yend='PKTS/100000', x='SIZE', xend='SIZE'), size=LINE_SIZE*24,color=COLORS[0])
            # + geom_point(aes(y='PKTS/100000', x='SIZE'), size=DEFAULT_LINE_SIZE*2,color=COLORS[0])
            + geom_errorbar(width=0.3, size=LINE_SIZE, linetype="dashed", position=position_dodge(width=0.7))
            # + geom_errorbar(position=position_dodge(width=0.7))

            + coord_cartesian(ylim=(80,100))
            + labs(
                title="pkt loss",
                x="frame size [B]",
                y="success [%]",
                fill="",
                color=""
                )
            + GLOBAL_THEME()
            # + theme_light()
            + scale_color_manual([darken(c) for c in COLORS], drop=False)
            + scale_fill_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def ts_success_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + aes(y='PKTS/100000', x='SIZE', fill="RATE", group="RATE")
            # + geom_errorbar(size=DEFAULT_LINE_SIZE,color=COLORS[0])

            # + geom_point(aes(y='PKTS/100000', x='SIZE'), size=DEFAULT_LINE_SIZE*2,color=COLORS[0], position=position_dodge(width=DEFAULT_LINE_SIZE/4))
            # + geom_line(aes(y='PKTS/100000', x='SIZE'), size=DEFAULT_LINE_SIZE*1,color=COLORS[0])
            + geom_col(width=0.7,color=None,position="dodge")
            # + geom_segment(aes(y=0,yend='PKTS/100000', x='SIZE', xend="SIZE", fill="RATE", color="RATE", group="RATE"),size=LINE_SIZE*24,position=position_dodge(width=LINE_SIZE/1.22))
            # + geom_segment(aes(y=0,yend='PKTS/100000', x='SIZE', xend="SIZE", fill="RATE", color="RATE", group="RATE"),size=LINE_SIZE*24,position=position_dodge(width=LINE_SIZE/1.22))
            + coord_cartesian(ylim=(90,100))
            # + ylim(90,100)
            + labs(
                title="pkt loss",
                x="frame size [B]",
                y="success [%]",
                fill="rate",
                # color="tc:"
                )
            + GLOBAL_THEME()
            + scale_color_manual(COLORS, drop=False)
            + scale_fill_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def ts_success_plot_cores(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + aes(y='PKTS/100000', x='CORES')
            # + geom_errorbar(size=DEFAULT_LINE_SIZE,color=COLORS[0])
            + geom_col(width=0.7,color=None,position="dodge",fill=COLORS[0])
            + coord_cartesian(ylim=(0.0,100.0))
            + labs(
                title="pkt loss per core",
                x="number of lcores",
                y="success [%]",
                # fill="rate",
                # color="tc:"
                )
            + GLOBAL_THEME()
            # + scale_color_manual(COLORS_TC, drop=False)
            # + scale_fill_manual(COLORS_TC, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def ts_success_plot_cores_err(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + aes(y='PKTS/100000', x='CORES')
            + geom_col(width=0.7,color=None,position="dodge",fill=COLORS[0])
            + geom_errorbar(aes(ymin="(PKTS - PKTS_ci)/100000",ymax="(PKTS + PKTS_ci)/100000"),width=0.3,size=LINE_SIZE,color=darken(COLORS[0]),linetype="dashed")
            + coord_cartesian(ylim=(0.0,100.0))
            + scale_x_continuous(trans="identity",breaks=[2,4,6,8,10], labels=["2","4","6","8","10"])
            + labs(
                title="pkt loss per core",
                x="number of lcores",
                y="success [%]",
                # fill="rate",
                # color="tc:"
                )
            + GLOBAL_THEME()
            # + scale_color_manual(COLORS_TC, drop=False)
            # + scale_fill_manual(COLORS_TC, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)# }}}
    plot.save(f"{filename}.svg",width = PLOT_W, height= PLOT_H, verbose=False)# }}}



if __name__ == "__main__":
    df = handle_import("~/Documents/I3-Praktikum-DPDK/data/antrittsvortrag/TS/")
    # print(df)
    # some_plot(df.query("CORES == 6"), "test")
    ts_mean_plot(df.query("CORES == 6 and RATE == '10G'"), "ts_mean")
    ts_success_plot(df.query("CORES == 6"), "ts_success")
    ts_success_plot_cores(df.query("RATE == '10G' and SIZE == '64B'"), "ts_success_cores")
    # box_plot(df, "test")


    TS_CORES = "/home/lks/Documents/I3-Praktikum-DPDK/data/antrittsvortrag/TS_CORES/"
    df_parts = []
    for f in glob.glob(f"{TS_CORES}/*.csv"):
        if m:= re.match(r".*SIZE_(\w+)__CORES_(\w+)__RATE_(\w+)", os.path.basename(f)):
            SIZE = f"{m.group(1)}B"
            CORES = int(m.group(2))
            RATE = str(m.group(3))
            d = pd.read_csv(f)
            m,ci = mean_confidence_interval(d["PKTS"].to_numpy())
            dd = {
            "SIZE":[SIZE],
            "CORES":[CORES],
            "RATE":[RATE],
            "PKTS_ci":[ci],
            "PKTS":[m]
                    }
            df_parts.append(pd.DataFrame(dd))
    df_full = pd.concat(df_parts, ignore_index=True)
    print(df_full)
    ts_success_plot_cores_err(df_full.query("RATE == '10G' and SIZE == '64B'"), "ts_success_cores_err")

    SWITCH_PATH="/home/lks/Documents/I3-Praktikum-DPDK/data/antrittsvortrag/SWITCH_DELAY/"
    df = handle_import(SWITCH_PATH)
    ts_mean_plot(df.query("CORES == 3 and RATE == '10G'"), "switch_mean")

    df_parts = []
    for f in glob.glob(f"{SWITCH_PATH}/*G.csv"):
        df_parts.append(data_import_per_file(f,summarize=False).sample(10000))
    df_full = pd.concat(df_parts, ignore_index=True)
    box_plot(df_full, "switch_delay")

    SWITCH_SUCCESS="/home/lks/Documents/I3-Praktikum-DPDK/data/antrittsvortrag/SWITCH_SUCCESS/"
    df_parts = []
    for f in glob.glob(f"{SWITCH_SUCCESS}/*G.csv"):
        if m:= re.match(r".*SIZE_(\w+)__CORES_(\w+)__RATE_(\w+)", os.path.basename(f)):
            SIZE = f"{m.group(1)}B"
            CORES = int(m.group(2))
            RATE = str(m.group(3))
            d = pd.read_csv(f)
            m,ci = mean_confidence_interval(d["PKTS"].to_numpy())
            dd = {
            "SIZE":[SIZE],
            "CORES":[CORES],
            "RATE":[RATE],
            "PKTS_ci":[ci],
            "PKTS":[m]
                    }
            df_parts.append(pd.DataFrame(dd))
    df_full = pd.concat(df_parts, ignore_index=True)
    print("---")
    print(df_full)
    print("---")
    df_full["PKTS_succ"] = df_full["PKTS"]/100000
    print(df_full)
    # 10 Cores means we had the switch and timestamper running simoultaneously => interference
    switch_success_plot(df_full.query("CORES == 3 and RATE == '10G'"), "switch_pkts")
    switch_success_plot_interference(df_full.query("SIZE =='64B' and RATE == '10G'"), "switch_pkts_interference")

    pass













