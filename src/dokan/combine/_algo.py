import numpy as np
import math

# import matplotlib.pyplot as plt


def is_outlier_MAD(points, thresh=3.5):
    median = np.nanmedian(points)
    abs_diff = np.abs(points - median)

    mad = np.nanmedian(abs_diff)
    if mad == 0.0:
        modified_z_score = 0.0 * abs_diff
    else:
        modified_z_score = 0.6745 * abs_diff / mad

    return modified_z_score > thresh


def is_outlier_doubleMAD(points, thresh=3.5, qplt=False):
    # > maybe even want to swith to the "Harrell-Davis quantile estimator" in the future?

    median = np.nanmedian(points)
    abs_diff = np.abs(points - median)
    left_mad = np.nanmedian(abs_diff[points <= median])
    right_mad = np.nanmedian(abs_diff[points >= median])
    if left_mad == 0.0 or right_mad == 0.0:
        # means all entries are a constant (propbably zero)
        modified_z_score = 0.0 * abs_diff
    else:
        y_mad = np.zeros(len(points))
        y_mad[points < median] = left_mad
        y_mad[points > median] = right_mad
        y_mad[points == median] = 0.5 * (left_mad + right_mad)
        modified_z_score = 0.6745 * abs_diff / y_mad

    # if qplt:
    #     print(">>> doubleMAD [ {} | {} | {} ]".format(left_mad,median,right_mad))
    #     nbins = 50
    #     range = [ min(points), max(points) ]
    #     plt.hist(points, range=range, bins=nbins, log=True, label='all')
    #     plt.hist(points[modified_z_score > thresh], range=range, bins=nbins, log=True, label='trim')
    #     plt.show()

    return modified_z_score > thresh


def is_outlier_dynMAD(points, thresh=3.5, dsigma=0.6745, qplt=False):
    frac = math.erf(dsigma / math.sqrt(2.0))
    # print(">>> dsigma = {}; frac = {}".format(dsigma, frac))
    low, med, upp = np.nanquantile(points, [0.5 * (1.0 - frac), 0.5, 0.5 * (1.0 + frac)])
    # , method="normal_unbiased")
    if low == med or upp == med or med == np.nan:
        # means all entries are a constant (propbably zero)
        modified_z_score = np.zeros(len(points))
    else:
        y_mad = np.zeros(len(points))
        y_mad[points < med] = abs(med - low)  # left_mad
        y_mad[points > med] = abs(upp - med)  # right_mad
        y_mad[points == med] = 0.5 * abs(upp - low)
        modified_z_score = dsigma * np.abs(points - med) / y_mad

    # if qplt:
    #     print(">>> dyn MAD [ {} | {} | {} ]".format(left_mad,median,right_mad))
    #     nbins = 50
    #     range = [ min(points), max(points) ]
    #     plt.hist(points, range=range, bins=nbins, log=True, label='all')
    #     plt.hist(points[modified_z_score > thresh], range=range, bins=nbins, log=True, label='trim')
    #     plt.show()

    return modified_z_score > thresh


def is_outlier_IQR(points, thresh=1.5):
    median = np.nanmedian(points)
    lower_quartile = np.nanpercentile(points, 25.0)
    upper_quartile = np.nanpercentile(points, 75.0)

    # the inter-quartile-range
    iqr = upper_quartile - lower_quartile

    # fences
    lower_fence = lower_quartile - thresh * iqr
    upper_fence = upper_quartile + thresh * iqr

    # return (points < lower_fence) | (points > upper_fence)
    return np.array(
        [(pt < lower_fence) | (pt > upper_fence) if np.isfinite(pt) else False for pt in points]
    )
