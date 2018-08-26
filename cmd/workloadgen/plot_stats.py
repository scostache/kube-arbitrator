import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import errno
import operator

# Some fake data:

def parse_queue(s):
  aux = s.split("=")
  return int(aux[1])


dirn = sys.argv[1]
file = dirn + "/" + "delay.cvs"

delaydata = np.genfromtxt(file, delimiter=' ')

y = delaydata[:,1]

mean = np.mean(y)
std = np.std(y)
sum = np.sum(y)

print "Total delay=",sum," mean=",mean," std=",std

resdir=dirn+"/figures"
statdir=dirn+"/stat"

try:
    os.makedirs(resdir)
except OSError as exc: # Guard against race condition
    if exc.errno != errno.EEXIST:
        raise
try:
    os.makedirs(statdir)
except OSError as exc: # Guard against race condition
    if exc.errno != errno.EEXIST:
        raise

f = open("%s/delay_stats.txt" % statdir, "w")
f.write("Total delay=%s mean=%s std=%s\n" % (sum, mean, std))
f.close()

sorted_delaydata = np.sort(y)  # Or data.sort(), if data can be modified

# Cumulative counts:
plt.step(sorted_delaydata, np.arange(sorted_delaydata.size))  # From 0 to the number of data points-1
plt.xlabel('Jobs')
plt.ylabel('Delay CDF')
#plt.show()
plt.savefig('%s/delay_cdf.png' % resdir)

plt.clf()

#plot delay points
plt.plot(delaydata[:,0], delaydata[:,1], label="Delay distribution")
plt.xlabel('Job ID')
plt.ylabel('Delay (seconds)')
#plt.show()
plt.savefig('%s/delay_distribution.png' % resdir)

plt.clf()

# plot allocation
file = dirn + "alloc.cvs"
allocdata = np.genfromtxt(file, delimiter=' ')
y = allocdata[:,1]
plt.plot(allocdata[:,1], label="Allocation variation in the cluster")
plt.xlabel('Time Step')
plt.ylabel('Allocated Resource Units')
plt.savefig('%s/allocation_variation.png' % resdir)
#plt.show()

plt.clf()

file = dirn + "queues_numbers.cvs"
# plot Queue variation over time
queue = np.genfromtxt(file, delimiter=' ')
plt.plot(queue[:,1], label="Queue variation in the cluster")
plt.xlabel('Time Step')
plt.ylabel('Number of Queued Jobs')
plt.savefig('%s/queue_variation.png' % resdir)

# plot job request statistics
plt.clf()

file = dirn + "stats.txt"
all = np.genfromtxt(file, delimiter=' ')
runtime = all[:,5]
request = all[:,15]
sr = np.sort(runtime)
rr = np.sort(request)

plt.step(sr, np.arange(sr.size), label="Runtime")  # From 0 to the number of data points-1
plt.step(rr, np.arange(rr.size), label="Size")  # From 0 to the number of data points-1

plt.xlabel('Jobs')
plt.ylabel('CDF')
plt.savefig('%s/cdf_job_requests.png' % resdir)

