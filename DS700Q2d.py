# Fit distribution to histogram (?) and estimate Pr(>max)

import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

distIn = np.loadtxt("q2a.txt")
dist = []
for i in range(len(distIn)):
    dist.append([distIn[i][0]]*int(distIn[i][1]))

distF = [item for sublist in dist for item in sublist]
x = np.linspace(0,max(dist)[0],1000)

h = plt.hist(distF, bins=range(len(set(distF))),normed=True)
plt.show()

# dist_names = ['norm', 'expon', 'lognorm']
dist_names = ['levy','loggamma','weibull_min']

for dist_name in dist_names:
    distro = getattr(scipy.stats, dist_name)
    param = distro.fit(distF)
    pdf_fitted = distro.pdf(x, *param[:-2], loc=param[-2], scale=param[-1]) * len(distF)
    plt.plot(pdf_fitted, label=dist_name)
    prMax = distro.cdf(max(dist)[0], *param[:-2], loc=param[-2], scale=param[-1])
    print dist_name + ": " + str(prMax)

plt.legend(loc='upper right')
plt.show()

# Poisson
mu = sum(distF)/len(distF)
vals = sorted(list(set(distF)))
plt.plot(scipy.stats.poisson.pmf(vals,mu))
plt.show()

scipy.stats.poisson.cdf(max(dist)[0],mu)

# # Zipf

# plt.plot(scipy.stats.zipf.pmf(vals,mu))
# plt.show()

# scipy.stats.zipf.cdf(max(dist)[0],mu)
