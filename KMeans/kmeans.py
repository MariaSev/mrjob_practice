from mrjob.job import MRJob
from math import sqrt
import os

CENTROIDS_FILE = "/home/masha/KMeans/centroids" 
CENTROIDS_NUMBER = 3

class MRKMeans(MRJob):
	SORT_VALUES = True

	def get_dist(self, v1, v2):
		return sqrt((v2[0] - v1[0]) * (v2[0] - v1[0]) + (v2[1] - v1[1]) * (v2[1] - v1[1]))

	def get_centroids_from_file(self):
		f = open(CENTROIDS_FILE, 'r')
		centroids = []
		for line in f.read().split('\n'):
			if line:
				x, y = line.split(', ')
				centroids.append([float(x), float(y)])
		f.close()
		return centroids
 
	def mapper(self, _, lines):
		"""
        Calculates the nearest centroid for each point 
        Out: number of cluster (= number of nearest centroid), point
		"""
		centroids=self.get_centroids_from_file()
		for l in lines.split('\n'):
			x, y = l.split(', ')
			point = [float(x), float(y)]
			min_dist = 100000000.0
			classe = 0
            # iterate over the centroids (Here we know that we are doing a 3means)
			for i in range(CENTROIDS_NUMBER):
				dist = self.get_dist(point, centroids[i])
				if dist < min_dist:
					min_dist = dist
					classe = i
			yield classe, point
	def reducer(self, k, v):
		"""
        Calculates the new centroid for each claster
        Out: "N, coord_x, coord_y", None
		"""
		count = 0
		moy_x = moy_y = 0.0
		for t in v:
			count += 1
			moy_x += t[0]
			moy_y += t[1]
		yield str(k) + ", " + str(moy_x / count) + ",  " + str(moy_y / count), None

if __name__ == '__main__':
	MRKMeans.run()


