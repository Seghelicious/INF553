import math
import sys
from sklearn.cluster import KMeans
import numpy as np
import time


# Dictonary struct : 0 - points, 1 - len, 2 - SUMi, 3 - SUM2, 4 - SD, 5 - Centroid


def get_point_clusterid_map(ds_statistics, cs_statistics, rs):
    point_clusterid_map = {}
    for key in ds_statistics:
        for point in ds_statistics[key][0]:
            point_clusterid_map[point] = key
    for key in cs_statistics:
        for point in cs_statistics[key][0]:
            point_clusterid_map[point] = -1
    for point in rs:
        point_clusterid_map[point] = -1
    return point_clusterid_map


def output_intermediate(fout, load_instance):
    ds_points_count = 0
    cs_clusters_count = 0
    cs_points_count = 0
    for key in ds_summary.keys():
        ds_points_count += ds_summary[key][1]
    for key in cs_summary.keys():
        cs_clusters_count += 1
        cs_points_count += cs_summary[key][1]
    rs_points_count = len(RS_points)
    print("Round " + str(load_instance + 1) + ": " + str(ds_points_count) + "," + str(cs_clusters_count) + "," + str(
        cs_points_count) + "," + str(rs_points_count))
    fout.write(
        "Round " + str(load_instance + 1) + ": " + str(ds_points_count) + "," + str(cs_clusters_count) + "," + str(
            cs_points_count) + "," + str(rs_points_count) + "\n")


def output_cluster_info(fout, ds_summary, cs_summary, rs):
    fout.write("\nThe clustering results: ")
    point_clusterid_map = get_point_clusterid_map(ds_summary, cs_summary, rs)
    for point in sorted(point_clusterid_map.keys(), key=int):
        fout.write("\n" + str(point) + "," + str(point_clusterid_map[point]))
    print(len(point_clusterid_map))


def generate_ds_summary(key, point_indices, points_array):
    ds_summary[key] = {}
    ds_summary[key][0] = []
    for i in point_indices:
        ds_summary[key][0].append(ctr_index_map[i])
    ds_summary[key][1] = len(ds_summary[key][0])
    ds_summary[key][2] = np.sum(points_array[point_indices, :].astype(np.float), axis=0)
    ds_summary[key][3] = np.sum((points_array[point_indices, :].astype(np.float)) ** 2, axis=0)
    ds_summary[key][4] = np.sqrt((ds_summary[key][3][:] / ds_summary[key][1]) - (
            np.square(ds_summary[key][2][:]) / (ds_summary[key][1] ** 2)))
    ds_summary[key][5] = ds_summary[key][2] / ds_summary[key][1]


def generate_cs_summary(key, point_indices, points_array):
    cs_summary[key] = {}
    cs_summary[key][0] = []
    for i in point_indices:
        pointid = list(RS.keys())[list(RS.values()).index(RS_points[i])]
        cs_summary[key][0].append(pointid)
    cs_summary[key][1] = len(cs_summary[key][0])
    cs_summary[key][2] = np.sum(points_array[point_indices, :].astype(np.float), axis=0)
    cs_summary[key][3] = np.sum((points_array[point_indices, :].astype(np.float)) ** 2, axis=0)
    cs_summary[key][4] = np.sqrt((cs_summary[key][3][:] / cs_summary[key][1]) - (
            np.square(cs_summary[key][2][:]) / (cs_summary[key][1] ** 2)))
    cs_summary[key][5] = cs_summary[key][2] / cs_summary[key][1]


def update_ds_summary(pointid, newpoint, cluster_key):
    ds_summary[cluster_key][0].append(pointid)
    ds_summary[cluster_key][1] = ds_summary[cluster_key][1] + 1
    for i in range(0, d):
        ds_summary[cluster_key][2][i] += newpoint[i]
        ds_summary[cluster_key][3][i] += newpoint[i] ** 2
    ds_summary[cluster_key][4] = np.sqrt((ds_summary[cluster_key][3][:] / ds_summary[cluster_key][1]) - (
            np.square(ds_summary[cluster_key][2][:]) / (ds_summary[cluster_key][1] ** 2)))
    ds_summary[cluster_key][5] = ds_summary[cluster_key][2] / ds_summary[cluster_key][1]


def update_cs_summary(pointid, newpoint, cluster_key):
    cs_summary[cluster_key][0].append(pointid)
    cs_summary[cluster_key][1] = cs_summary[cluster_key][1] + 1
    for i in range(0, d):
        cs_summary[cluster_key][2][i] += newpoint[i]
        cs_summary[cluster_key][3][i] += newpoint[i] ** 2
    cs_summary[cluster_key][4] = np.sqrt((cs_summary[cluster_key][3][:] / cs_summary[cluster_key][1]) - (
            np.square(cs_summary[cluster_key][2][:]) / (cs_summary[cluster_key][1] ** 2)))
    cs_summary[cluster_key][5] = cs_summary[cluster_key][2] / cs_summary[cluster_key][1]


def merge_cs_clusters(key1, key2):
    cs_summary[key1][0].extend(cs_summary[key2][0])
    cs_summary[key1][1] = cs_summary[key1][1] + cs_summary[key2][1]
    for i in range(0, d):
        cs_summary[key1][2][i] += cs_summary[key2][2][i]
        cs_summary[key1][3][i] += cs_summary[key2][3][i]
    cs_summary[key1][4] = np.sqrt((cs_summary[key1][3][:] / cs_summary[key1][1]) - (
            np.square(cs_summary[key1][2][:]) / (cs_summary[key1][1] ** 2)))
    cs_summary[key1][5] = cs_summary[key1][2] / cs_summary[key1][1]


def merge_cs_with_ds(cs_key, ds_key):
    ds_summary[ds_key][0].extend(cs_summary[cs_key][0])
    ds_summary[ds_key][1] = ds_summary[ds_key][1] + cs_summary[cs_key][1]
    for i in range(0, d):
        ds_summary[ds_key][2][i] += cs_summary[cs_key][2][i]
        ds_summary[ds_key][3][i] += cs_summary[cs_key][3][i]
    ds_summary[ds_key][4] = np.sqrt((ds_summary[ds_key][3][:] / ds_summary[ds_key][1]) - (
            np.square(ds_summary[ds_key][2][:]) / (ds_summary[ds_key][1] ** 2)))
    ds_summary[ds_key][5] = ds_summary[ds_key][2] / ds_summary[ds_key][1]


def get_cluster_dict(clusters):
    cluster_dict = {}
    ctr = 0
    for clusterid in clusters:
        if clusterid in cluster_dict:
            cluster_dict[clusterid].append(ctr)
        else:
            cluster_dict[clusterid] = [ctr]
        ctr = ctr + 1
    return cluster_dict


def get_closest_cluster_id(summary):
    closest_cluster_md = threshold_distance
    closest_clusterid = -1
    for key in summary.keys():
        std_dev = summary[key][4].astype(np.float)
        centroid = summary[key][5].astype(np.float)
        mahalanobis_distance = 0
        for dim in range(0, d):
            mahalanobis_distance += ((point[dim] - centroid[dim]) / std_dev[dim]) ** 2
        mahalanobis_distance = np.sqrt(mahalanobis_distance)

        if mahalanobis_distance < closest_cluster_md:
            closest_cluster_md = mahalanobis_distance
            closest_clusterid = key
    return closest_clusterid


def get_closest_cluster(summary1, summary2):
    keys1 = summary1.keys()
    keys2 = summary2.keys()
    closest = {}
    for x in keys1:
        closest_cluster_md = threshold_distance
        closest_clusterid = x
        for y in keys2:
            if x != y:
                stddev1 = summary1[x][4]
                stddev2 = summary2[y][4]
                centroid1 = summary1[x][5]
                centroid2 = summary2[y][5]
                md1 = 0
                md2 = 0
                for dim in range(0, d):
                    md1 += ((centroid1[dim] - centroid2[dim]) / stddev2[dim]) ** 2
                    md2 += ((centroid2[dim] - centroid1[dim]) / stddev1[dim]) ** 2
                md1 = np.sqrt(md1)
                md2 = np.sqrt(md2)
                mahalanobis_distance = min(md1, md2)
                if mahalanobis_distance < closest_cluster_md:
                    closest_cluster_md = mahalanobis_distance
                    closest_clusterid = y
        closest[x] = closest_clusterid
    return closest


start = time.time()

data_file = 'dataset/hw6_clustering.txt'
num_clusters = 10
out_file = 'output/output.csv'

# data_file = sys.argv[1]
# num_clusters = int(sys.argv[2])
# out_file = sys.argv[3]

fout = open(out_file, "w")
file = open(data_file, "r")
data = np.array(file.readlines())
file.close()
final_round = 4

# Step 1. Load 20% of the data randomly.

one_fifth = int(len(data) * 20 / 100)
initial_sample = np.random.choice(a=data, size=one_fifth, replace=False)
ctr_index_map = {}  # ctr -> pointid
index_point_map = {}  # pointid -> point
point_index_map = {}  # point -> pointid
initial_data = []

DS_ctr = 0
for l in initial_sample:
    line = l.replace("\n", "").split(",")
    index = line[0]
    point = line[2:]
    initial_data.append(point)
    ctr_index_map[DS_ctr] = index
    index_point_map[index] = point
    point_index_map[str(point)] = index
    DS_ctr = DS_ctr + 1

d = len(initial_data[0])
threshold_distance = 2 * math.sqrt(d)
points_array = np.array(initial_data)

# Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters) on the data in memory using the Euclidean distance as the similarity measurement.

kmeans = KMeans(n_clusters=5 * num_clusters, random_state=0)
clusters_extra = kmeans.fit_predict(points_array)
clusters = {}

ctr = 0
for clusterid in clusters_extra:
    point = initial_data[ctr]
    if clusterid in clusters:
        clusters[clusterid].append(point)
    else:
        clusters[clusterid] = [point]
    ctr = ctr + 1

RS = {}  # index <-> point
ds_summary = {}
cs_summary = {}

# Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).

ctr = 0
for key in clusters.keys():
    if len(clusters[key]) == 1:
        point = clusters[key][0]
        pos = initial_data.index(point)
        RS[ctr_index_map[pos]] = point  # setting RS
        initial_data.remove(point)
        for l in range(pos, len(ctr_index_map) - 1):
            ctr_index_map[l] = ctr_index_map[l + 1]
        ctr = ctr + 1

# Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.

points_array_without_RS = np.array(initial_data)
kmeans = KMeans(n_clusters=num_clusters, random_state=0)
clusters = get_cluster_dict(kmeans.fit_predict(points_array_without_RS))

# Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).

for key in clusters.keys():
    generate_ds_summary(key, clusters[key], points_array_without_RS)

# The initialization of DS has finished, so far, you have K numbers of DS clusters (from Step 5) and some numbers of RS (from Step 3).

# Step 6. Run K-Means on the points in the RS with a large K to generate CS (clusters with more than one points) and RS (clusters with only one point).
RS_points = []
for key in RS.keys():
    RS_points.append(RS[key])

rs_points_array = np.array(RS_points)
kmeans = KMeans(n_clusters=int(len(RS_points) / 2 + 1), random_state=0)
cs_clusters = get_cluster_dict(kmeans.fit_predict(rs_points_array))

for key in cs_clusters.keys():
    if len(cs_clusters[key]) > 1:
        generate_cs_summary(key, cs_clusters[key], rs_points_array)

for key in cs_clusters.keys():
    if len(cs_clusters[key]) > 1:
        for i in cs_clusters[key]:
            point_to_remove = list(RS.keys())[list(RS.values()).index(RS_points[i])]
            del RS[point_to_remove]

RS_points = []
for key in RS.keys():
    RS_points.append(RS[key])

fout.write("The intermediate results:\n")
output_intermediate(fout, 0)

# Step 7. Load another 20% of the data randomly.

for load_instance in range(1, 5):
    new_data = []
    if load_instance == final_round:
        new_data = np.random.choice(a=data, size=len(data) - one_fifth * 4, replace=False)
    else:
        new_data = np.random.choice(a=data, size=one_fifth, replace=False)

    new_points = []
    last_ctr = DS_ctr
    for l in new_data:
        line = l.replace("\n", "").split(",")
        index = line[0]
        point = line[2:]
        new_points.append(point)
        ctr_index_map[DS_ctr] = index
        index_point_map[index] = point
        point_index_map[str(point)] = index
        DS_ctr = DS_ctr + 1

    new_points_array = np.array(new_points)

    # Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign them to the nearest DS clusters if the distance is < 2√𝑑.
    # Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and assign the points to the nearest CS clusters if the distance is < 2√𝑑
    # Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.

    ctr = 0
    for l in new_points_array:
        point = l.astype(np.float)
        index = ctr_index_map[last_ctr + ctr]
        closest_clusterid = get_closest_cluster_id(ds_summary)

        if closest_clusterid > -1:
            # Step 8
            update_ds_summary(index, point, closest_clusterid)
        else:
            closest_clusterid = get_closest_cluster_id(cs_summary)

            if closest_clusterid > -1:
                # Step 9
                update_cs_summary(index, point, closest_clusterid)
            else:
                # Step 10
                RS[index] = list(l)
                RS_points.append(list(l))
        ctr = ctr + 1


    # Step 11. Run K-Means on the RS with a large K to generate CS (clusters with more than one points) and RS (clusters with only one point).

    new_points_array = np.array(RS_points)
    kmeans = KMeans(n_clusters=int(len(RS_points) / 2 + 1), random_state=0)

    cs_clusters = get_cluster_dict(kmeans.fit_predict(new_points_array))

    for key in cs_clusters.keys():
        if len(cs_clusters[key]) > 1:
            k = 0
            if key in cs_summary.keys():
                while k in cs_summary:
                    k = k + 1
            else:
                k = key

            generate_cs_summary(k, cs_clusters[key], new_points_array)

    for key in cs_clusters.keys():
        if len(cs_clusters[key]) > 1:
            for i in cs_clusters[key]:
                # point_to_remove = list(RS.keys())[list(RS.values()).index(RS_points[i])]
                # del RS[point_to_remove]
                point_to_remove = point_index_map[str(RS_points[i])]
                if point_to_remove in RS.keys():
                    del RS[point_to_remove]

    RS_points = []
    for key in RS.keys():
        RS_points.append(RS[key])

    # Step 12. Merge CS clusters that have a Mahalanobis Distance < 2√𝑑.

    cs_keys = cs_summary.keys()
    closest_cluster_map = get_closest_cluster(cs_summary, cs_summary)

    for cs_key in closest_cluster_map.keys():
        if cs_key != closest_cluster_map[cs_key] and closest_cluster_map[cs_key] in cs_summary.keys() and cs_key in cs_summary.keys():
            merge_cs_clusters(cs_key, closest_cluster_map[cs_key])
            del cs_summary[closest_cluster_map[cs_key]]

    # If this is the last run , merge CS clusters with DS clusters that have a Mahalanobis Distance < 2√𝑑.

    if load_instance == final_round:
        closest_cluster_map = get_closest_cluster(cs_summary, ds_summary)
        for cs_key in closest_cluster_map.keys():
            if closest_cluster_map[cs_key] in ds_summary.keys() and cs_key in cs_summary.keys():
                merge_cs_with_ds(cs_key, closest_cluster_map[cs_key])
                del cs_summary[cs_key]

    output_intermediate(fout, load_instance)

output_cluster_info(fout, ds_summary, cs_summary, RS)

fout.close()

end = time.time()
print("Duration: " + str(end - start))

# Round 1: 64457,2,4,1
# Round 2: 128910,5,12,2
# Round 3: 193356,6,27,3
# Round 4: 257800,5,46,2
# Round 5: 322256,3,54,2
# 216686
# Duration: 71.13698983192444
