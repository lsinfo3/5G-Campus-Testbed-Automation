import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols











sample_data_a1 = {
        "A":    [1,1,1,1,1,1],
        "B":    [32,32,64,64,128,128],
        "run":  [1,2,1,2,1,2],
        "value":[0.25,.28,.21,.19,.15,.11],
        }
sample_data_a2 = {
        "A":    [2,2,2,2,2,2],
        "B":    [32,32,64,64,128,128],
        "run":  [1,2,1,2,1,2],
        "value":[.52,.48,.45,.49,.36,.30],
        }
sample_data_a3 = {
        "A":    [3,3,3,3,3,3],
        "B":    [32,32,64,64,128,128],
        "run":  [1,2,1,2,1,2],
        "value":[.81,.76,.66,.59,.50,.61],
        }
sample_data_a4 = {
        "A":    [4,4,4,4,4,4],
        "B":    [32,32,64,64,128,128],
        "run":  [1,2,1,2,1,2],
        "value":[1.5,1.61,1.45,1.32,.70,.68],
        }

df = pd.concat([pd.DataFrame(d) for d in [sample_data_a1, sample_data_a2, sample_data_a3, sample_data_a4]]).reset_index(drop=True)
df.loc[:,"Aa"] = df["A"] < 3
print(df)


model = ols('value ~ C(A) + C(Aa) +C(B) + C(A):C(B) + C(Aa):C(B) + C(A):C(Aa) + C(A):C(Aa):C(B)', data=df).fit()
anova_result = sm.stats.anova_lm(model, type=1)
anova_result.loc[:,".p"] = anova_result.loc[:,"PR(>F)"].apply(lambda x : f"{x:.6f}")
anova_result.loc[:,"SST/SS"] = anova_result.loc[:,"sum_sq"]/  anova_result.loc[:,"sum_sq"].sum()
print(anova_result)
