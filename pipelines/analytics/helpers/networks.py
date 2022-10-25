import numpy as np
import networkx as nx
from sknetwork.clustering import Louvain
from tqdm import tqdm
import logging

class Networks():
    def compute_biadjacency(self, G, top_nodes, bottom_nodes):
        """Computes the biadjacency matrix for a BiPartite network.
        Returns the biadjacency the top_node maping and the bottom_nodes mapping.
        return biadjacency, top_id_map, bottom_id_map
        """
        logging.info("Computing biadjacency...")
        bottom_id_map = {}
        for i in range(len(bottom_nodes)):
            bottom_id_map[bottom_nodes[i]] = i

        top_id_map = {}
        for i in range(len(top_nodes)):
            top_id_map[top_nodes[i]] = i

        biadjacency = np.zeros(
            (len(top_nodes), len(bottom_nodes)), dtype=np.float32)
        for top_node in tqdm(top_nodes):
            js = [bottom_id_map[edge[1]]
                  for edge in G.edges(top_node) if edge[1] in bottom_id_map]
            i = top_id_map[top_node]
            for j in js:
                biadjacency[i][j] += 1
        return biadjacency, top_id_map, bottom_id_map

    def compute_projection(self, biadjacency, threshold, axis=0):
        """Optimized projection computation for a biadjacency matrix.
        This method is optimized for memory and CPU time but returns no logging info and may seem stuck.
        Returns the adjacency matrix of the projection."""
        logging.info("Computing projection...")
        if axis == 0:
            projection = np.matmul(biadjacency, biadjacency.T)
        elif axis == 1:
            projection = np.matmul(biadjacency.T, biadjacency)
        else:
            raise(ValueError("axis must be 0 or 1"))
        for i in range(len(projection)):
            projection[i][i] = 0
        projection = projection * (projection > threshold)
        return projection.astype(bool).astype(np.int8)

    def get_partitions(self, adjacency, id_map):
        """Computes the louvain partitions from an adjacency matrix"""
        logging.info("Computing partitions...")
        louvain = Louvain()
        labels = louvain.fit_transform(adjacency)
        id_map_r = {}
        for node_id in id_map:
            id_map_r[id_map[node_id]] = node_id
        partitions = {}
        for i in range(len(labels)):
            partitions[id_map_r[i]] = labels[i]
        return partitions, set(labels)
