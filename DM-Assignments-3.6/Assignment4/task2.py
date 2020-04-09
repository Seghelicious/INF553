import copy

from pyspark import SparkConf, SparkContext
import time
import sys
from collections import defaultdict


def write_to_file_betweenness(output_file, output):
    f = open(output_file, 'w')
    for i in output:
        f.write("(\'" + str(i[0][0] + "\', \'" + str(i[0][1]) + "\'), " + str(i[1]) + "\n"))
    f.close()


def write_to_file_community(output_file, output):
    f = open(output_file, 'w')
    for i in output:
        f.write(str(i).strip('[').strip(']') + '\n')
    f.close()


def set_dict_value(dictionary, key, value):
    dictionary[key] = value


def bfs(root, original_graph):
    parent = defaultdict(list)
    depth = defaultdict(int)
    shortest_path_count = defaultdict(float)
    credit = defaultdict(float)
    queue = []
    bfs_queue = []

    set_dict_value(parent, root, None)
    set_dict_value(depth, root, 0)
    set_dict_value(shortest_path_count, root, 1)
    bfs_queue.append(root)

    for c in original_graph[root]:
        set_dict_value(parent, c, [root])
        set_dict_value(depth, c, 1)
        set_dict_value(shortest_path_count, c, 1)
        queue.append(c)
        bfs_queue.append(c)

    while queue:
        node = queue.pop(0)
        set_dict_value(credit, node, 1)
        paths = 0
        for p in parent[node]:
            paths = paths + shortest_path_count[p]
        set_dict_value(shortest_path_count, node, paths)
        for neighbour in original_graph[node]:
            if neighbour not in bfs_queue:
                set_dict_value(parent, neighbour, [node])
                set_dict_value(depth, neighbour, depth[node] + 1)
                queue.append(neighbour)
                bfs_queue.append(neighbour)
            else:
                # case when one node has multiple parents
                if depth[neighbour] == depth[node] + 1:
                    parent[neighbour].append(node)


    bfs_queue.reverse()
    for child in bfs_queue[:-1]:
        for parnt in parent[child]:
            score = credit[child] * (shortest_path_count[parnt] / shortest_path_count[child])
            credit[parnt] += score
            yield (tuple(sorted([child, parnt])), score)


def get_communities(vertices):
    visited_nodes = []
    communities = []
    queue = []
    for vertex in vertices:
        if vertex not in visited_nodes:
            visited = [vertex]
            queue.append(vertex)
            while queue:
                node = queue.pop(0)
                for neighbour in original_graph[node]:
                    if neighbour not in visited:
                        visited.append(neighbour)
                        queue.append(neighbour)
            visited.sort()
            visited_nodes.extend(visited)
            communities.append(visited)
    return communities


def get_modularity(communities):
    modularity = 0.0
    denominator = 2 * m
    for community in communities:
        for i in community:
            for j in community:
                expected = (len(original_graph[i]) * len(original_graph[i])) / denominator
                if j in original_graph[i]:
                    actual = 1
                else:
                    actual = 0
                modularity += (actual - expected)
    return modularity / denominator


def cut_graph(edges):
    for edge in edges:
        original_graph[edge[0]].remove(edge[1])
        original_graph[edge[1]].remove(edge[0])


start_time = time.time()

# time /home/local/spark/latest/bin/spark-submit task2.py $ASNLIB/publicdata/power_input.txt task2-betweenness.csv task2-community.csv
input_file = sys.argv[1]
betweenness_output_file = sys.argv[2]
community_output_file = sys.argv[3]
# input_file = 'dataset/power_input.txt'
# betweenness_output_file = 'output/task2-betweenness.csv'
# community_output_file = 'output/task2-community.csv'

conf = SparkConf().setAppName("INF553").setMaster('local[*]')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

modular_community = []
best_modularity = 0
input_data = sc.textFile(input_file)
input_data = input_data.map(lambda x: x.split(" ")).persist()
m = input_data.count()  # total edges

# Finding Betweenness
original_graph = input_data.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collectAsMap()
vertices = sorted(original_graph.keys())
betweenness = sc.parallelize(vertices).flatMap(lambda x: bfs(x, original_graph)).reduceByKey(lambda x, y: x+y)\
    .map(lambda x: (x[0], x[1]/2)).sortBy(lambda x: (-x[1], x[0][0], x[0][1])).collect()
write_to_file_betweenness(betweenness_output_file, betweenness)

# Finding Communities
rem_edges = m
while rem_edges > 0:
    communities = get_communities(vertices)
    modularity = get_modularity(communities)
    if modularity > best_modularity:
        best_modularity = modularity
        modular_community = copy.deepcopy(communities)
    min_cut = sc.parallelize(vertices).flatMap(lambda x: bfs(x, original_graph)).reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[1]/2, [x[0]])).reduceByKey(lambda x, y: x+y).sortBy(lambda x: (-x[0])).map(lambda x: x[1]).first()
    cut_graph(min_cut)
    rem_edges -= len(min_cut)
comunities = sc.parallelize(modular_community).sortBy(lambda x: (len(x), x)).collect()
write_to_file_community(community_output_file, comunities)

print("Duration : ", time.time() - start_time)
