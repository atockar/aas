import numpy as np

degs = np.loadtxt('degrees.txt')

print "Length: " + str(degs.size)
print "Mean: " + str(degs.mean())
print "Median: " + str(np.median(degs))

degs0 = np.concatenate([np.zeros(45993),degs])

print "Length (with zeros): " + str(degs0.size)
print "Mean (with zeros): " + str(degs0.mean())
print "Median (with zeros): " + str(np.median(degs0))

print "Probably go with the first answer as got this wrong last time"