import pandas as pd
import numpy as np


from plotnine.ggplot import ggplot
from plotnine import labs, aes, \
        facet_grid, facet_wrap, \
        geom_line, geom_point, geom_bar, geom_errorbar, geom_boxplot, stat_boxplot, geom_col, geom_segment, geom_violin, \
        theme, theme_light, theme_dark, theme_gray, element_text, element_line, element_rect, element_blank, guide_legend, guide_colorbar, \
        scale_x_log10, scale_y_log10, scale_x_continuous, scale_y_continuous, scale_y_discrete, scale_x_discrete, coord_cartesian, coord_trans, \
        scale_color_manual, scale_fill_manual, scale_linetype_manual, \
        ylim, xlim, position_dodge, \
        stage

from plotninesettings import PLOT_W, PLOT_H, LINE_SIZE, POINT_SIZE, COLORS, COLORS_TC, PLOTCOLORS, GLOBAL_THEME, brighten, darken






# df1 = pd.read_csv("../ansible/dumps/tcpdump_ue.csv")
df1 = pd.read_csv("/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/c80e840c/c80e840c__4/tcpdump_ue.csv")
df1["location"] = "ue"
df1["type"] = df1.apply(lambda x: "request" if x["SourceIPInner"] != "10.45.0.1" else "response" , axis=1)
df1.sort_values(by=["SeqNum", "type"], ignore_index=True, inplace=True)
df1["delay"] = np.nan
df1["IAT"] = np.nan
indexer = lambda d:d["type"] == "request"
df1.loc[indexer,"IAT"] = df1.loc[indexer,"Timestamp"] - df1.loc[indexer,"Timestamp"].shift(1)
indexer = lambda d:d["type"] == "response"
df1.loc[indexer,"IAT"] = df1.loc[indexer,"Timestamp"] - df1.loc[indexer,"Timestamp"].shift(1)

# df2 = pd.read_csv("../ansible/dumps/tcpdump_gnb.csv")
df2 = pd.read_csv("/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/dumps/c80e840c/c80e840c__4/tcpdump_gnb.csv")
df2["location"] = "gnb"
df2["type"] = df2.apply(lambda x: "request" if x["SourceIPInner"] != "10.45.0.1" else "response" , axis=1)
df2.sort_values(by=["SeqNum", "type"], ignore_index=True, inplace=True)
df2["delay"] = np.nan
df2["IAT"] = np.nan
indexer = lambda d:d["type"] == "request"
df2.loc[indexer,"IAT"] = df2.loc[indexer,"Timestamp"] - df2.loc[indexer,"Timestamp"].shift(1)
indexer = lambda d:d["type"] == "response"
df2.loc[indexer,"IAT"] = df2.loc[indexer,"Timestamp"] - df2.loc[indexer,"Timestamp"].shift(1)
df2_max_seq = df2["SeqNum"].max()
df2= df2[df2["SeqNum"] < df2_max_seq]
print(f"Max: {df2_max_seq}")

df1= df1[df1["SeqNum"] < df2_max_seq]
print(df1)
print(df2)

for idx, row in df1.iterrows():
    if row["type"] == "request":
        delay = df2.iloc[idx]["Timestamp"] - row["Timestamp"]
    elif row["type"] == "response":
        delay = row["Timestamp"] - df2.iloc[idx]["Timestamp"]
    else:
        raise ValueError
    df1.loc[idx, "delay"] = delay

df1["direction"] = df1["type"].apply(lambda x : "Ul" if x == "request" else "Dl")

df = pd.concat([df1, df2])
print(df)




def box_plot(df, filename:str):
    plot = (ggplot(df)
            # + facet_wrap("param", scales="free_x",nrow=1)
            # + aes(group="param")
            + aes(y='delay', x='type')
            + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE)
            + geom_boxplot(outlier_shape="x", size=LINE_SIZE, outlier_size=LINE_SIZE,color=COLORS[0],fill=brighten(COLORS[0],4.0))
            # improve readability by adding , -> 1,000
            # + scale_x_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            # + scale_y_continuous(labels=lambda lst: [f"{int(y):,}" for y in lst])
            + labs(
                title="delay",
                x="ICMP ping type",
                y="delay [s]",
                # color="tc:"
                )
            + GLOBAL_THEME(smallh=True)
            # + theme_light()
            # + scale_color_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)

def violin_box_combined_plot(df, filename:str):
    q01 = min([df.loc[lambda d: d["type"] == "request","delay"].quantile(0.01), df.loc[lambda d: d["type"] == "response","delay"].quantile(0.01)     ])
    q99 = max([df.loc[lambda d: d["type"] == "request","delay"].quantile(0.99), df.loc[lambda d: d["type"] == "response","delay"].quantile(0.99)     ])
    print(f"Min/max: {q01}/{q99}")
    shift = 0.05
    plot = (ggplot(df)
            + aes(y='delay', x=stage('type', after_scale='x-shift'), linetype="direction")
            # + geom_violin(size=LINE_SIZE, color=COLORS[0], fill=brighten(COLORS[0],4.0), draw_quantiles=[0.05,0.25,0.5,0.75,0.95])
            + geom_violin(size=LINE_SIZE/1.3, style="left", width=1.1 , color=COLORS[1], fill=brighten(COLORS[1],4))
            + geom_violin(aes(x=stage('type', after_scale='x+shift')), size=LINE_SIZE/1.3, style="right", width=1.1, color=COLORS[1], fill=brighten(COLORS[1],4) )
            + geom_boxplot(aes(x='type'), outlier_shape=None, size=LINE_SIZE, width=0.05, outlier_size=LINE_SIZE, position="identity", color=COLORS[1], fill=brighten(COLORS[1],4))
            # + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE, linetype="dashed")
            + ylim(q01,q99)
            + labs(
                title="delay",
                x="ICMP ping type",
                y="delay [s]",
                )
            + GLOBAL_THEME(smallh=True)
            # + scale_color_manual(COLORS, drop=False)
            # + scale_fill_manual([brighten(c,4) for c in COLORS], drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)

def violin_plot(df, filename:str):
    shift = 0.5
    plot = (ggplot(df)
            + aes(y='delay', x=stage('type', after_scale='x-shift'), color="type", fill="type")
            # + geom_violin(size=LINE_SIZE, color=COLORS[0], fill=brighten(COLORS[0],4.0), draw_quantiles=[0.05,0.25,0.5,0.75,0.95])
            + geom_violin(size=LINE_SIZE/1.3, style="right" )
            # + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE, linetype="dashed")
            + labs(
                title="delay",
                x="ICMP ping type",
                y="delay [s]",
                )
            + GLOBAL_THEME(smallh=True)
            + scale_color_manual(COLORS, drop=False)
            + scale_fill_manual([brighten(c,4) for c in COLORS], drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)


def line_plot(df, filename:str):
    shift = 0.5
    plot = (ggplot(df.head(100))
            # + aes(y='delay', x="Timestamp", color="type", fill="type")
            + aes(y='delay', x="SeqNum", color="direction", fill="direction")
            # + geom_violin(size=LINE_SIZE, color=COLORS[0], fill=brighten(COLORS[0],4.0), draw_quantiles=[0.05,0.25,0.5,0.75,0.95])
            + geom_line(size=LINE_SIZE)
            + geom_point(size=POINT_SIZE)
            # + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE, linetype="dashed")
            + labs(
                title="delay",
                x="packet number",
                y="delay [s]",
                )
            + GLOBAL_THEME(smallh=True)
            + scale_color_manual(COLORS, drop=False)
            + scale_fill_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)

def lineiat_plot(df, filename:str):
    shift = 0.5
    plot = (ggplot(df.head(100))
            + aes(y='IAT', x="SeqNum", color="direction", fill="direction")
            # + geom_violin(size=LINE_SIZE, color=COLORS[0], fill=brighten(COLORS[0],4.0), draw_quantiles=[0.05,0.25,0.5,0.75,0.95])
            + geom_line(size=LINE_SIZE)
            + geom_point(size=POINT_SIZE)
            # + stat_boxplot(geom='errorbar',color=COLORS[0],size=LINE_SIZE, linetype="dashed")
            + labs(
                title="delay",
                x="packet number",
                y="IAT [s]",
                )
            + GLOBAL_THEME(smallh=True)
            + scale_color_manual(COLORS, drop=False)
            + scale_fill_manual(COLORS, drop=False)
            )
    plot.save(f"{filename}.pdf",width = PLOT_W, height= PLOT_H, verbose=False)
    plot.save(f"{filename}.png",width = PLOT_W, height= PLOT_H, verbose=False)



box_plot(df1, filename="../ansible/dumps/delay_boxplot")
violin_box_combined_plot(df1, filename="../ansible/dumps/delay_violinboxplot")
violin_plot(df1, filename="../ansible/dumps/delay_violinplot")
line_plot(df1, filename="../ansible/dumps/delay_lineplot")
lineiat_plot(df1, filename="../ansible/dumps/delay_iatlineplot")
