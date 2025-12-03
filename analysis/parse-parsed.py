import pandas as pd
import scipy.stats
import numpy as np
import argparse
import os
from allcolumns import agg_percentiles, agg_mean, agg_min, agg_max, all_columns, all_msm_columns, columns_to_group_by


def percentile(n):
    def percentile_(x):
        return x.quantile(n)
    # percentile_.__name__ = 'percentile_{:02.0f}'.format(n*100)
    percentile_.__name__ = f'percent_{int(n*100):02d}'
    return percentile_


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


""" Built dictionary of columns and corresponding aggregate functions """
def build_agg_dictionary() -> dict:
    d = {}
    for k in agg_mean:
        if k in d.keys():
            d[k] += ["mean"]
        else:
            d[k] = ["mean"]

    for k in agg_min:
        if k in d.keys():
            d[k] += ["min"]
        else:
            d[k] = ["min"]
    for k in agg_max:
        if k in d.keys():
            d[k] += ["max"]
        else:
            d[k] = ["max"]

    percentiles = [0.05, 0.25, 0.75, 0.95]
    percentiles = [percentile(p) for p in percentiles]
    for k in agg_percentiles:
        if k in d.keys():
            d[k] += percentiles
        else:
            d[k] = percentiles

    for k in agg_percentiles:
        if k in d.keys():
            d[k] += [mean_confidence_interval(0.95)]
        else:
            d[k] = [mean_confidence_interval(0.95)]

    return d


def show_columns_with_differences(df : pd.DataFrame):
    # columns_to_group_by = list( set(all_columns).difference(set(all_msm_columns)).difference(set(["identifier"])) )
    for c in columns_to_group_by:
        if not c in df.columns:
            continue
        param_values = list(df[c].unique())
        if len(param_values) <= 1:
            continue
        print(f"{c}:{param_values}")



def main(dir):
    # df = pd.read_parquet("/home/lks/DocSync/Uni/5G-Masterarbeit/ansible/antenna-gain/all_runs.parquet")
    assert(os.path.isdir(dir))
    assert(os.path.isfile(f"{dir}/all_runs.parquet"))
    df = pd.read_parquet(f"{dir}/all_runs.parquet")

    defaults = {
        'jammer':                                False,
        'dockerization':                         False,
        'performance_tuning':                    False,
        'distance_floor':                        0.2,
        'distance_nearest_wall':                 0.2,
        'location':                              "A202"
    }
    for k,v in defaults.items():
        if k not in df.columns:
            df[k] = v

    try :
        assert(set(df.columns) == set(all_columns))
    except AssertionError as ae:
        print(set(df.columns).symmetric_difference(set(all_columns)))
        raise ae



    columns_to_group_by = list( set(all_columns).difference(set(all_msm_columns)).difference(set(["run", "identifier"])) )
    print("---")
    print(columns_to_group_by)
    print("---")
    for c in columns_to_group_by:
        print(f"{c}: {df[c].nunique()}")
    print("---")


    # Not needed, coverd by agg failed_runs_series = df.groupby(columns_to_group_by, dropna=False).agg({"failed_run":[("failed_runs",lambda x : sum(x==True))]}).reset_index(drop=True)["failed_run"]["failed_runs"]
    dfg = df.groupby(columns_to_group_by, dropna=False).agg(build_agg_dictionary())
    dfg.columns = list(map(lambda x: '__agg__'.join(filter(None,x)), dfg.columns.values))
    dfg.reset_index(inplace=True)

    dfg['ue_power__agg__ci_95_l'] = dfg['ue_power__agg__mean'] - np.abs(dfg["ue_power__agg__ci_95"])
    dfg['ue_power__agg__ci_95_u'] = dfg['ue_power__agg__mean'] + np.abs(dfg["ue_power__agg__ci_95"])
    dfg['sdr_power__agg__ci_95_l'] = dfg['sdr_power__agg__mean'] - np.abs(dfg["sdr_power__agg__ci_95"])
    dfg['sdr_power__agg__ci_95_u'] = dfg['sdr_power__agg__mean'] + np.abs(dfg["sdr_power__agg__ci_95"])

    dfg['throughput__mean__agg__ci_95_l'] = dfg['throughput__mean__agg__mean'] - np.abs(dfg["throughput__mean__agg__ci_95"])
    dfg['throughput__mean__agg__ci_95_u'] = dfg['throughput__mean__agg__mean'] + np.abs(dfg["throughput__mean__agg__ci_95"])

    dfg['throughputin__mean__agg__ci_95_l'] = dfg['throughputin__mean__agg__mean'] - np.abs(dfg["throughputin__mean__agg__ci_95"])
    dfg['throughputin__mean__agg__ci_95_u'] = dfg['throughputin__mean__agg__mean'] + np.abs(dfg["throughputin__mean__agg__ci_95"])

    dfg['delay__mean__agg__ci_95_l'] = dfg['delay__mean__agg__mean'] - np.abs(dfg["delay__mean__agg__ci_95"])
    dfg['delay__mean__agg__ci_95_u'] = dfg['delay__mean__agg__mean'] + np.abs(dfg["delay__mean__agg__ci_95"])

    dfg['sent_pkts__agg__ci_95_l'] = dfg['sent_pkts__agg__mean'] - np.abs(dfg["sent_pkts__agg__ci_95"])
    dfg['sent_pkts__agg__ci_95_u'] = dfg['sent_pkts__agg__mean'] + np.abs(dfg["sent_pkts__agg__ci_95"])

    dfg.to_csv(f"{dir}/all_runs_groupby_agg.csv")
    dfg.to_parquet(f"{dir}/all_runs_groupby_agg.parquet")
    print(dfg)

    print("Different values:")
    show_columns_with_differences(df)

    print("\n\nDifferent values aggregated:")
    show_columns_with_differences(dfg)

    print("\n\nFinal columns:")
    print( list(dfg.columns) )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Evaluate packet recordings in csvs",
        description="Scan given dir and"
            )
    parser.add_argument("filename")
    args = parser.parse_args()
    ansible_dump = args.filename
    main(ansible_dump)





