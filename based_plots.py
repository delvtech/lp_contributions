import pandas as pd
import numpy as np


def gini_coefficient(x):
    """Compute Gini coefficient of array of values"""
    diffsum = 0
    for i, xi in enumerate(x[:-1], 1):
        diffsum += np.sum(np.abs(xi - x[i:]))
    return diffsum / (len(x) ** 2 * np.mean(x)) * 100


def plotit(ax, distrib, field, normalize=False):
    """Plot gini coefficient"""
    xvals = np.arange(1, len(distrib[field]) + 1)
    if normalize:
        xvals = xvals / max(xvals)
    p = ax.plot(
        xvals,
        distrib[field].sort_values(ascending=True).cumsum() / sum(distrib[field]),
    )
    ax.plot(xvals, xvals / max(xvals))
    gini_coeff = gini_coefficient(np.array(distrib[field].values))
    title = f"gini = {gini_coeff:,.0f}"
    ax.set_title(title)
    return p[0], gini_coeff, title
