import numpy as np
import scipy.stats as scp

def interval(a):
    a = np.array(a)
    X = np.average(a)
    S = np.sqrt(np.average(a * a) - X * X)
    n = len(a)
    z = scp.t(n - 1).ppf(0.95)
    delta = (S * z) / np.sqrt(n - 1)
    return (X - delta, X + delta)