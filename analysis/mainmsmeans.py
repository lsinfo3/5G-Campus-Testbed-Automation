import pandas as pd
import numpy as np
import scipy.stats
import statsmodels.api as sm
from statsmodels.formula.api import ols
from itertools import combinations, product
import natsort

pd.options.display.float_format = "{:,.6f}".format
pd.set_option('display.max_rows', 200)
# pd.set_option('display.precision', 2)

ANOVA_TYPE=3



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
print(df)
print(df.columns)
print(len(df))

# model = ols('value ~ C(A) + C(Aa) +C(B) + C(A):C(B) + C(Aa):C(B) + C(A):C(Aa) + C(A):C(Aa):C(B)', data=df).fit()
# anova_result = sm.stats.anova_lm(model, type=1)
# anova_result.loc[:,".p"] = anova_result.loc[:,"PR(>F)"].apply(lambda x : f"{x:.6f}")
# anova_result.loc[:,"SST/SS"] = anova_result.loc[:,"sum_sq"]/  anova_result.loc[:,"sum_sq"].sum()
# print(anova_result)

df.rename(columns={
"gnb_version__type": "gnb_type",
"gnb_version__version":"gnb_version",
"gnb_version__uhd_version":"uhd_version",
"traffic_config__traffic_type" : "traffic_type",
"tdd_config__tdd_dl_ul_ratio" : "tdd_ratio",
"tdd_config__tdd_dl_ul_tx_period" : "tdd_period",
    }, inplace=True)
# df.loc[:,"traffic_type_config"] = "" + df["traffic_type"].astype(str) + "__" + df["traffic_config__direction"].astype(str) + \
#         "__" + df["traffic_config__iat"].astype(str) + "__" + df["traffic_config__size"].astype(str)
df.loc[:,"traffic_type_config"] = "" + df["traffic_type"].astype(str) + "__" + df["direction"].astype(str) + \
        "__" + df["traffic_config__iat"].astype(str) + "__" + df["traffic_config__size"].astype(str)
df.loc[:,"throughput__mean"] = df.loc[:,"throughput__mean"] / 1000000


# df['tdd_ratio'] = df["tdd_ratio"].astype(str) + ":1"
# df['tdd_ratio'] = pd.Categorical(df['tdd_ratio'], ordered=True, categories= natsort.natsorted(df['tdd_ratio'].unique()))
df['tdd_ratio'] = pd.Categorical(df['tdd_ratio'], ordered=True, categories=[1, 2, 4] )

factors = [
# "direction",
"gnb_type",
"gnb_version",
"uhd_version",
# "traffic_type",
# "traffic_type_config",
"tdd_ratio",
"tdd_period",
        ]
# print(df[factors])
# print(df.dtypes)
# print(df["traffic_type_config"].value_counts())


""" Returns string in this format C(1):C(2) + C(1):C(3) + C(2):C(3) """
def build_factor_combinations(factors: list, length: int):
    factor_combinations = combinations(factors, length)
    factor_combinations = [ ":".join([f"C({f})" for f in factorlist]) for factorlist in factor_combinations ]
    return " + ".join(factor_combinations)

# factors_combine_1 = " + ".join([f"C({f})" for f in factors])
# factors_combine_2 = " + ".join([f"C({f1}):C({f2})" for f1,f2 in combinations(factors, 2)])
# factors_combine_3 = " + ".join([f"C({f1}):C({f2}):C({f3})" for f1,f2,f3 in combinations(factors, 3)])
# factors_combine_4 = " + ".join([f"C({f1}):C({f2}):C({f3}):C({f4})" for f1,f2,f3,f4 in combinations(factors, 4)])
# factors_combine_5 = " + ".join([f"C({f1}):C({f2}):C({f3}):C({f4}):C({f5})" for f1,f2,f3,f4,f5 in combinations(factors, 5)])
# factors_combine_6 = " + ".join([f"C({f1}):C({f2}):C({f3}):C({f4}):C({f5}):C({f6})" for f1,f2,f3,f4,f5,f6 in combinations(factors, 6)])
# factors_combine_7 = " + ".join([f"C({f1}):C({f2}):C({f3}):C({f4}):C({f5}):C({f6}):C({f7})" for f1,f2,f3,f4,f5,f6,f7 in combinations(factors, 7)])


metrics = ["throughput__mean", "delay__mean"]
collected_data =pd.DataFrame(None, pd.MultiIndex.from_product([["tt"],["A"],["b"]], names=["Traffic","Factor", "Values"]),["throughput__mean"]).dropna()
for traffic_type in df["traffic_type"].unique():
    for metric in metrics:

        if traffic_type == 'scapyudpping':
            q_factors = factors + ["traffic_config__size", "traffic_config__iat"]
        elif traffic_type == 'iperfthroughput':
            q_factors = factors + ["direction"]
        else:
            raise ValueError("Unknown data query")

        for factor in q_factors:
            for unique_value in df[ df["traffic_type"] == traffic_type ][factor].unique():
                mean = df[ (df["traffic_type"] == traffic_type) & (df[factor] == unique_value) ][metric].mean()
                collected_data.loc[(traffic_type,factor,unique_value),metric] = mean
                if pd.isna(mean):
                    ci = np.nan
                else:
                    ci = df[ (df["traffic_type"] == traffic_type) & (df[factor] == unique_value) ][metric].agg(mean_confidence_interval(0.95))
                collected_data.loc[(traffic_type,factor,unique_value),metric+"_ci"] = ci

print(collected_data)


metrics = "throughput__mean"
cnsm_multiindex = pd.MultiIndex.from_product([["A"],["b"]], names=["gNB", "Ratio"])
# cnsm_multiindex.sortlevel("Ratio", ascending=True)
cnsm_like_table =pd.DataFrame(None, cnsm_multiindex ,["Dl","Ul"]).dropna()
df_q = df.loc[ (df["traffic_type"] == 'iperfthroughput') & (df["uhd_version"] == "UHD-3.15.LTS") & ( (df["gnb_version"] == "release_24_04")|(df["gnb_version"] == "v2.1.0") ) ]
cnsm_factors = ["direction", "gnb_type", "tdd_ratio"]
for gnb in df_q["gnb_type"].unique():
    # for ratio in df_q["tdd_ratio"].unique():
    for ratio in [1,2,4]:
        for direction in df_q["direction"].unique():
            mean = df_q.loc[ (df["gnb_type"] == gnb) & (df["tdd_ratio"] == ratio) & (df["direction"] == direction), metrics].mean()
            ci = df_q.loc[ (df["gnb_type"] == gnb) & (df["tdd_ratio"] == ratio) & (df["direction"] == direction),metrics].agg(mean_confidence_interval(0.95))
            cnsm_like_table.loc[(gnb,ratio),direction] = f"{mean:.2f}±{ci:.2f}"

# cnsm_like_table.sort_index(level=[1],inplace=True)
print("\nSame dimensions and versions as in cnsm paper")
print(cnsm_like_table)


metrics = "throughput__mean"
cnsm_multiindex = pd.MultiIndex.from_product([["A"],["b"]], names=["gNB", "Ratio"])
# cnsm_multiindex.sortlevel("Ratio", ascending=True)
cnsm_like_table =pd.DataFrame(None, cnsm_multiindex ,["Dl","Ul"]).dropna()
df_q = df.loc[ (df["traffic_type"] == 'iperfthroughput') ]
cnsm_factors = ["direction", "gnb_type", "tdd_ratio"]
for gnb in df_q["gnb_type"].unique():
    # for ratio in df_q["tdd_ratio"].unique():
    for ratio in [1,2,4]:
        for direction in df_q["direction"].unique():
            mean = df_q.loc[ (df["gnb_type"] == gnb) & (df["tdd_ratio"] == ratio) & (df["direction"] == direction), metrics].mean()
            ci = df_q.loc[ (df["gnb_type"] == gnb) & (df["tdd_ratio"] == ratio) & (df["direction"] == direction),metrics].agg(mean_confidence_interval(0.95))
            cnsm_like_table.loc[(gnb,ratio),direction] = f"{mean:.2f}±{ci:.2f}"

# cnsm_like_table.sort_index(level=[1],inplace=True)
print("\n\nSame dimensions as in cnsm paper, but agg. over gnb/uhd versions")
print(cnsm_like_table)
