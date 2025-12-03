import pandas as pd
# from pptx import Presentation
from pandas.core.api import DataFrame
import numpy as np
import scipy.stats
import os
import sys
import hashlib
from datetime import datetime, timedelta

import re
import glob


from mizani.formatters import date_format
from mizani.breaks import date_breaks

from plotnine.ggplot import ggplot
from plotnine import labs, guides, guide_legend, aes, \
        facet_grid, facet_wrap, \
        geom_line, geom_point, geom_bar, geom_errorbar, geom_col, geom_segment, geom_boxplot, stat_boxplot, geom_vline, geom_hline, geom_violin, geom_histogram, geom_smooth, stat_smooth, \
        theme, theme_light, theme_dark, theme_gray, element_text, element_line, element_rect, element_blank, guide_legend, guide_colorbar, \
        scale_x_log10, scale_y_log10, scale_x_continuous, scale_y_continuous, scale_y_discrete, scale_x_discrete, coord_cartesian, coord_trans, \
        scale_color_cmap, scale_color_gradient, scale_color_distiller, scale_color_manual, scale_fill_manual, scale_linetype_manual, scale_x_datetime, scale_y_datetime, \
        position_dodge, position_dodge2, \
        ylim, xlim

# INFO: geom_smooth with non linear fitting: https://stackoverflow.com/questions/63344621/fit-non-linear-curve-using-plotnine

from plotninesettings import PLOT_W, PLOT_H, LINE_SIZE, POINT_SIZE, WIDTH, ERRWIDTH, COLORS, COLORS_TC, PLOTCOLORS, GLOBAL_THEME, COLOR_MAP, COLOR_MAP_EXTRACTOR, \
        brighten, darken






EXTENSIONS=[".pdf", ".jpg"]

# TODO:
# FIGURES_PATH
# DATA_PATH
# df validation function

projectdir = "."


"""
Use only log-scale or limits not both!
"""

def mean_confidence_interval(data, confidence=0.95):
    """
    Get mean and the lower and upper limit for the confidence interval
    """
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, h


def box_plot(df:DataFrame, filename:str):# {{{
    print("Box plot")
    print(df)
    plot = (ggplot(df)
            + aes(y='Y*1000', x='param_D',  fill="param_C", color="param_C")
            + stat_boxplot(geom='errorbar',size=LINE_SIZE,width=WIDTH,position=position_dodge(width=WIDTH+LINE_SIZE*0.033))
            + geom_boxplot(outlier_shape="x", size=LINE_SIZE,width=WIDTH, outlier_size=LINE_SIZE,position=position_dodge(width=WIDTH+LINE_SIZE*0.033))
            # INFO: boxplots can also be precomputed like and plotted like this:
            # + geom_boxplot(aes(x='factor(hour)', ymin='min', lower="q25", middle="median", upper="q75", ymax="max"), stat = "identity")
            + geom_hline(yintercept=[3500],color=COLORS[-1],linetype="dotted", size=LINE_SIZE)
            + scale_y_log10(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + guides(fill=guide_legend(title="Abc",reverse=True),color=guide_legend(title="Abc",reverse=True))
            + labs(
                title="Box plots with horizontal line and reversed legend",
                x="frame size [B]",
                y="time [ns]",
                # color="tc:"
                )
            + GLOBAL_THEME()
            + scale_color_manual(COLORS, drop=False)
            + scale_fill_manual([brighten(c,6) for c in COLORS], drop=False)
            )
    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = PLOT_W, height= PLOT_H, verbose=False, dpi=300)# }}}

def box_plot_manual(df:DataFrame, filename:str,
                     aesthetics=aes(x='param_D', ymin="ymin",lower="lower",middle="middle",upper="upper",ymax="ymax",fill="param_C", color="param_C"),
                     labels = { "title":"title", "x":"horse power", "y":"miles per galon", "color": "gears" },
                     facets = {},
                     outliers = False,
                     limits = { "xlim":[None,None], "ylim":[None,None] , "cartesian":True},
                     colors = COLOR_MAP_EXTRACTOR(5),
                     # colors_mapping: dict = none
                     smooth: bool = False):# {{{
    for k in ["x", "ymin", "lower", "middle", "upper", "ymax"]:
        if aesthetics.get(k) == None:
            raise ValueError(f"Aesthetics is missing definition of '{k}'!")

    plot = (ggplot(df)
            + aesthetics
            # + stat_boxplot(geom='errorbar',size=LINE_SIZE,width=WIDTH,position=position_dodge(width=WIDTH+LINE_SIZE*0.033), stat = "identity")
            + geom_errorbar(size=LINE_SIZE,width=WIDTH,position=position_dodge(width=WIDTH+LINE_SIZE*0.033))
            + geom_boxplot(outlier_shape="x" if outliers else None, size=LINE_SIZE,width=WIDTH, outlier_size=LINE_SIZE,position=position_dodge(width=WIDTH+LINE_SIZE*0.033),stat = "identity")
            # TODO: + guides(fill=guide_legend(title="Abc",reverse=True),color=guide_legend(title="Abc",reverse=True))
            + labs(**labels
                )
            + GLOBAL_THEME()
            + scale_color_manual(colors, drop=False)
            + scale_fill_manual([brighten(c,6) for c in colors], drop=False)
            )
    # TODO: limits! coord_cartesian!
    if limits.get("xlim") and limits.get("xlim") != [None,None] and limits.get("ylim") and limits.get("ylim") != [None,None]:
        plot = plot + coord_cartesian(ylim=limits["ylim"], xlim=limits["xlim"])
    elif limits["ylim"] != [None,None]:
        plot = plot + coord_cartesian(ylim=limits["ylim"])
    elif limits["xlim"] != [None,None]:
        plot = plot + coord_cartesian(xlim=limits["xlim"])

    if "facet" in facets.keys():
        plot = plot + facets["facet"]

    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = PLOT_W, height= PLOT_H, verbose=False, dpi=300)# }}}

def simple_line_plot(df:DataFrame, filename:str,
                     aesthetics=aes(y='mpg', x='hp', color="gear"),
                     labels = { "title":"title", "x":"horse power", "y":"miles per galon", "color": "gears" },
                     lines = True,
                     bars = False,
                     points = True,
                     facets = {},
                     scale = "continuous",
                     ratio = "4:3",
                     size = None,
                     errorbars = False,
                     limits = { "xlim":[None,None], "ylim":[None,None] , "cartesian":True},
                     colors = COLOR_MAP_EXTRACTOR(5),
                     # colors_mapping: dict = none
                     smooth: bool = False,
                     add_to_plot: list = []     # TODO: add to other function declarations
                     ):# {{{

    # if df[aesthetics.get("color")].nunique() > 5:
        # TODO: logger!
        # colors = COLOR_MAP_EXTRACTOR(df[aesthetics.get("color")].nunique())
        # raise ValueError("Require color mapping")

    lim_xlim = None if "xlim" not in limits.keys() or limits["xlim"] == [None,None] else limits["xlim"]
    lim_ylim = None if "ylim" not in limits.keys() or limits["ylim"] == [None,None] else limits["ylim"]


    plot = (ggplot(df)
            + aesthetics
            + scale_color_manual(colors, drop=True)
            + scale_fill_manual(colors, drop=True)
            )


    if lines:
        plot = plot + geom_line(size=LINE_SIZE)
    if bars:
        bar_width = 0.8
        plot = plot + geom_col(size=LINE_SIZE/1.2, position=position_dodge2(), color="#000", width=bar_width) + scale_fill_manual(COLORS, drop=False)
    if points:
        plot = plot + geom_point(size=POINT_SIZE*2)

    if errorbars:
        if aesthetics.get("ymin") == None or aesthetics.get("ymax") == None:
            raise ValueError("ymin and ymax must be defined when using errorbars!")
        if bars:
            plot = plot + geom_errorbar(size=LINE_SIZE/2, width=WIDTH/3,color="#000", linetype="solid", position=position_dodge(bar_width))
        else:
            # thin black lines
            # plot = plot + geom_errorbar(size=LINE_SIZE/2,width=WIDTH/10, color="#000000", linetype="solid")
            # thick lines
            # plot = plot + geom_errorbar(size=LINE_SIZE/2,width=WIDTH*5, linetype="solid")
            plot = plot + geom_errorbar(size=LINE_SIZE/2,width=WIDTH, linetype="solid")


    plot = (plot
            # + geom_errorbar(aes(ymin='Y_MEAN-Y_MEAN_ci',ymax='Y_MEAN+Y_MEAN_ci'),size=LINE_SIZE/2, linetype="dashed")
            # + scale_y_continuous(trans="log2",breaks=[2,2.5,6,8.5,11], labels=['2','2.5','6','8.5','11'])
            # + coord_cartesian(xlim=lim_xlim,ylim=lim_ylim)
            + labs(**labels
                )
            + GLOBAL_THEME()
            # + scale_fill_manual(colors, drop=False)
            # + scale_color_cmap("managua")
            # + scale_color_cmap("cividis")
            # + scale_color_gradient(high="#c44601", low="#054fb9")
            )

    # + geom_errorbar(size=LINE_SIZE,width=WIDTH,position=position_dodge(width=WIDTH+LINE_SIZE*0.033))
    # plot = plot + facet_wrap("gears")

    for e in add_to_plot:
        plot = plot + e


    if smooth:
        plot = plot + geom_smooth(method="lm")


    if limits.get("cartesian") == False and limits.get("xlim") != None and limits.get("xlim") != [None,None]:
        print("Setting xlim")
        plot = plot + xlim(limits["xlim"])
    if limits.get("cartesian") == False and limits.get("ylim") != None and limits.get("ylim") != [None,None]:
        print("Setting ylim")
        plot = plot + ylim(limits["ylim"])
    if limits.get("cartesian") == True and limits.get("ylim") != None and limits.get("ylim") != [None,None]:
        plot = plot + coord_cartesian(ylim=limits.get("ylim"))

    if scale == "log10":
        plot = plot + scale_y_log10()
    elif scale == "log2":
        plot = plot + scale_y_continuous(trans="log2")

    if "facet" in facets.keys():
        plot = plot + facets["facet"]

    plot_width = PLOT_W
    plot_height = PLOT_H
    if ratio == "16:9":
        plot_height = plot_height * (4/3) / (16/9)
    elif ratio == "16:10":
        plot_height = plot_height * (4/3) / (16/10)
    elif ratio == "16:11":
        plot_height = plot_height * (4/3) / (16/11)
    if size:
        plot_width, plot_height = size
    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = plot_width, height= plot_height, verbose=False, dpi=450)# }}}
    print(f"Writing {filename}.{EXTENSIONS[0]}")

def bar_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + facet_wrap("param_C", scales="free_x", nrow=1)
            + aes(y='Y_MEAN', x='param_D', fill="param_C", color="param_C", group="param_C",width=WIDTH)
            + geom_col(position='dodge', size=0.0)
            + geom_errorbar(aes(ymin="Y_MEAN-Y_MEAN_ci", ymax="Y_MEAN+Y_MEAN_ci",width=ERRWIDTH),position=position_dodge(width=WIDTH))
            + coord_cartesian(ylim=(0.9,8))
            + labs(
                title="Packetloss",
                x="rate[kbps]",
                y="packetloss[%]",
                # color="tc:"
                )
            + GLOBAL_THEME(smallh=True)
            + scale_fill_manual(COLORS, drop=False)
            + scale_color_manual([darken(c) for c in COLORS], drop=False)
            )
    # TODO: put save routine in custom function
    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = PLOT_W, height= PLOT_H, verbose=False, dpi=300)# }}}

def ecdf_plot(df: DataFrame, filename: str):#{{{
    plot = (ggplot(df)
            + aes(x="X_MEAN")
            + geom_line(stat="ecdf",color=COLORS[0])
            + coord_cartesian(expand=False)
            + labs(
                title="ECDF",
                y="",
                )
            + GLOBAL_THEME(smallh=True)
            )
    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = PLOT_W, height= PLOT_H, verbose=False)# }}}

def date_plot(df:DataFrame, filename:str):# {{{
    plot = (ggplot(df)
            + aes(y='Y', x='T', color="param_C", group="param_C")
            + geom_line(size=LINE_SIZE)
            + geom_point(size=POINT_SIZE)
            + scale_x_datetime(labels = date_format("%a %m-%d %H:%M:%S"), breaks = date_breaks("12 hours"))
            + labs(
                title="Packetloss",
                x="timestamp",
                y="packetloss[%]",
                color="tc:"
                )
            + GLOBAL_THEME() + theme(axis_text=element_text(angle=90))
            + scale_color_manual(COLORS, drop=False)
            )
    for e in EXTENSIONS:
        plot.save(f"{filename}{e}",width = PLOT_W, height= PLOT_H, verbose=False, dpi=300)# }}}

if __name__ == "__main__":
    d = {
            "X":[0,1,2,3,4,5],
            "Y":[1,2,2,4,5,6],
            "param_C":["Y", "N", "Y", "N", "Y", "N"],
            "param_D":["A", "A", "B", "B", "C", "C"],
            "T":[datetime.now() + timedelta(hours=15*i) for i in range(6)],
    }
    d_df = pd.DataFrame(d)

    df = handle_import(".",summarize=False,reload=True)
    print(df)
    box_plot(df.query("param_D == 'B' or param_D == 'C'"), "box")
    box_plot_manual(df.query("param_D == 'B' or param_D == 'C'"), "box_manual")


    df = handle_import(".",summarize=True,reload=True)
    print(df)
    bar_plot(df, "bar")
    ecdf_plot(df, "ecdf")
    date_plot(d_df, "date")
    pass













