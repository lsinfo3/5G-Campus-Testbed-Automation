# ---
# jupyter:
#   jupytext:
#     formats: py:percent
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
import matplotlib.pyplot as plt
import numpy as np


xpoints = np.array([i for i in range(10)])
ypoints = np.array([i for i in range(10)]) + np.random.rand(10)

plt.plot(xpoints, ypoints)
plt.show()

# %%

print("Test")
print("Test")
print("Test")
