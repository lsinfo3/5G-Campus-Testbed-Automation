# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
import datetime
import time
import matplotlib.pyplot as plt
import numpy as np
print("test")


# %% [markdown]
#
# # Header
# test,
# more tests
# - a
# - b
# - cc
#
#

# %%

print(datetime.datetime.now())

xpoints = np.array([float(i) for i in range(10)])
ypoints = np.array([float(i) for i in range(10)]) + np.random.rand(10) *2

plt.plot(xpoints, ypoints)
plt.show()



# %%

print("new test")
print("new test")
print("new test")
print("new test")
