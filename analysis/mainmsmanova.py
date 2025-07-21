import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
from itertools import combinations, product

pd.options.display.float_format = "{:,.6f}".format
pd.set_option('display.max_rows', 200)
# pd.set_option('display.precision', 2)

ANOVA_TYPE=3







df = pd.read_csv("/mnt/ext1/5g-masterarbeit-daten/main_measurement_qam64256/all_runs.csv.gz")
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


for anova_metric, data_query in product(["throughput__mean", "delay__mean"], ["traffic_type=='scapyudpping'", "traffic_type=='iperfthroughput'"]):
    print(f"ANOVA: {anova_metric} for {data_query}")
    try:
        # model = ols(f'{anova_metric} ~ {factors_combine_1} + {factors_combine_2} + {factors_combine_3} + '\
        #         f'{factors_combine_4} + {factors_combine_5} + {factors_combine_6}', data=df.query(data_query)).fit()
        if data_query == "traffic_type=='scapyudpping'":
            q_factors = factors + ["traffic_config__size", "traffic_config__iat"]
        elif data_query == "traffic_type=='iperfthroughput'":
            q_factors = factors + ["direction"]
        else:
            raise ValueError("Unknown data query")

        # Create string of this format: "metric ~ C(1) + C(2) + C(3) + C(1):C(2) + C(1):C(3) + C(2):C(3)"
        model_creation_string = f'{anova_metric} ~ {" + ".join([build_factor_combinations(q_factors,i+1) for i in range(len(q_factors))])}'

        model = ols(model_creation_string, data=df.query(data_query)).fit()
        anova_result = sm.stats.anova_lm(model, type=ANOVA_TYPE)
        anova_result.loc[:,"SS/SST"] = anova_result.loc[:,"sum_sq"]/  anova_result.loc[:,"sum_sq"].sum()
        anova_result.sort_values(by=['SS/SST'] , ascending=False, inplace=True)
        print(anova_result[ (anova_result["PR(>F)"] < 0.05) & (anova_result["SS/SST"] > 0.01) | (anova_result.index == "Residual") ])
    except Exception as e:
        print(e)
        pass

