from mrjob.job import MRJob
from kmeans import MRKMeans
from math import sqrt
import sys
import os

random_centroids = "rand_centroids"
CENTROIDS_FILE = "centroids"

def get_c(job, runner):
    c = []
    for key, value in job.parse_output(runner.cat_output()):
        c.append(key)
    return c
def get_first_c(fname):
    f = open(fname, 'r')
    centroids = []
    for line in f.read().split('\n'):
        if line:
            x, y = line.split(', ')
            centroids.append([float(x), float(y)])
    f.close()
    return centroids
def write_c(centroids):
    f = open(CENTROIDS_FILE, "w")
    centroids.sort()
    for c in centroids:
        k, cx, cy = c.split(', ')
        # print c
        f.write("%s, %s\n" % (cx, cy))
    f.close()
def get_dist(v1, v2):
    return sqrt((v2[0] - v1[0]) * (v2[0] - v1[0]) + (v2[1] - v1[1]) * (v2[1] - v1[1]))
def diff(cs1, cs2):
    max_dist = 0.0
    for i in range(3):
        dist = get_dist(cs1[i], cs2[i])
        if dist > max_dist:
            max_dist = dist
    return max_dist
if __name__ == '__main__':
    args = sys.argv[1:]
    old_c = get_first_c(random_centroids)
    i = 1
    while True:
        print("\nIteration #"+str(i))
        mr_job = MRKMeans(args) #
        print("start runner..")
        with mr_job.make_runner() as runner:
            runner.run()
            centroids = get_c(mr_job, runner)
        write_c(centroids)
        n_c = get_first_c(CENTROIDS_FILE)
        print("old_c\t"+str(old_c))
        print("new_c\t"+str(n_c))
        max_d = diff(n_c, old_c)
        print("max dist = "+str(max_d))
        if max_d < 0.01:
            break
        else:
            old_c = n_c
            i = i + 1
