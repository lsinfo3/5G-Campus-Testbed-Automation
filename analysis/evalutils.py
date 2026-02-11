import numpy as np
import pandas as pd
import scipy.stats
import natsort

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
    elif x == "v2.3.0":
        return "OAI 2.3.0"
    else:
        return x


def relabel_gnb_versions(x):
    if x == "release_24_04":
        return "v24.04"
    elif x == "release_24_10":
        return "v24.10"
    elif x == "v2.1.0":
        return "v2.1.0"
    elif x == "v2.2.0":
        return "v2.2.0"
    elif x == "v2.3.0":
        return "v2.3.0"
    else:
        return x


def add_labels(df: pd.DataFrame):
    if "gnb_version__version" in df.columns:
        df["gnb_version_label"] = df["gnb_version__version"].apply(labeler_gnb_version)
        df["gnb_version__version"] = df["gnb_version__version"].apply(relabel_gnb_versions)
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
        df['tdd_label'] = pd.Categorical(df['tdd_label'], ordered=True, categories= natsort.natsorted(df['tdd_label'].unique()))
    if "rx_gain" in df.columns:
        df['rx_gain_lbl'] = df['rx_gain'].apply(lambda x: "NoChange" if not x else f"{x}")
    if "tx_gain" in df.columns:
        df['tx_gain_lbl'] = df['tx_gain'].apply(lambda x: "NoChange" if not x else f"{x}")
    if "gnb_bandwidth" in df.columns:
        df['gnb_bandwidth_label'] = df['gnb_bandwidth'].apply(lambda x: f"{x} MHz")
    if "direction" in df.columns:
        df['dir_lbl'] = df['direction'].apply(lambda x: "downlink" if x.casefold() == 'dl' else 'uplink' )
    return df

def relabel_failue_value_vars(x):
    if x == "failed_run__agg__mean":
        return "Complete run"
    if x == "ue_power_failed__agg__mean":
        return "UE power"
    if x == "sdr_power_failed__agg__mean":
        return "SDR power"
    if x == "ue_channelmetrics_failed__agg__mean":
        return "UE channel"
    if x == "gnb_channelmetrics_failed__agg__mean":
        return "gNB channel"
