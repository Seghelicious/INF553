import math
import sys
from sklearn.cluster import KMeans
from sklearn.metrics import normalized_mutual_info_score
import numpy as np
import time


def output_intermediate(fout, load_instance):
    ds_points_count = 0
    cs_clusters_count = 0
    cs_points_count = 0
    for key in discard_set.keys():
        ds_points_count += discard_set[key][1]
    for key in compression_set.keys():
        cs_clusters_count += 1
        cs_points_count += compression_set[key][1]
    rs_points_count = len(retained_set_points)
    fout.write(
        "Round " + str(load_instance + 1) + ": " + str(ds_points_count) + "," + str(cs_clusters_count) + "," + str(
            cs_points_count) + "," + str(rs_points_count) + "\n")


def get_point_clusterid_map(ds_summary, cs_summary, rs):
    point_clusterid_map = {}
    # ds_points_count = 0
    # cs_points_count = 0
    # rs_points_count = 0
    for key in ds_summary:
        for point in ds_summary[key][0]:
            point_clusterid_map[point] = key
            # ds_points_count += 1
    for key in cs_summary:
        for point in cs_summary[key][0]:
            point_clusterid_map[point] = -1
            # cs_points_count += 1
    for point in rs:
        point_clusterid_map[point] = -1
        # rs_points_count += 1
    # print("DS count : " + str(ds_points_count) + " CS count : " + str(cs_points_count) + " RS count : " + str(rs_points_count))
    # print("Percentage of discard points after last round : " + str(ds_points_count * 100 / (ds_points_count + cs_points_count + rs_points_count)))
    return point_clusterid_map


def output_cluster_info(fout, ds_summary, cs_summary, rs):
    fout.write("\nThe clustering results: ")
    point_clusterid_map = get_point_clusterid_map(ds_summary, cs_summary, rs)
    for point in sorted(point_clusterid_map.keys(), key=int):
        # clustered_data.append([point, point_clusterid_map[point]])
        fout.write("\n" + str(point) + "," + str(point_clusterid_map[point]))


# Dictonary struct : 0 - points, 1 - len, 2 - SUMi, 3 - SUM2, 4 - SD, 5 - Centroid

def create_discard_set(key, point_indices, points_array):
    discard_set[key] = {}
    discard_set[key][0] = []
    for i in point_indices:
        discard_set[key][0].append(pctr_index_map[i])
    discard_set[key][1] = len(discard_set[key][0])
    discard_set[key][2] = np.sum(points_array[point_indices, :].astype(np.float), axis=0)
    discard_set[key][3] = np.sum((points_array[point_indices, :].astype(np.float)) ** 2, axis=0)
    discard_set[key][4] = np.sqrt((discard_set[key][3][:] / discard_set[key][1]) - (
            np.square(discard_set[key][2][:]) / (discard_set[key][1] ** 2)))
    discard_set[key][5] = discard_set[key][2] / discard_set[key][1]


def create_compression_set(key, point_indices, points_array):
    compression_set[key] = {}
    compression_set[key][0] = []
    for i in point_indices:
        pointid = list(retained_set_dict.keys())[list(retained_set_dict.values()).index(retained_set_points[i])]
        compression_set[key][0].append(pointid)
    compression_set[key][1] = len(compression_set[key][0])
    compression_set[key][2] = np.sum(points_array[point_indices, :].astype(np.float), axis=0)
    compression_set[key][3] = np.sum((points_array[point_indices, :].astype(np.float)) ** 2, axis=0)
    compression_set[key][4] = np.sqrt((compression_set[key][3][:] / compression_set[key][1]) - (
            np.square(compression_set[key][2][:]) / (compression_set[key][1] ** 2)))
    compression_set[key][5] = compression_set[key][2] / compression_set[key][1]


def update_summary(summary, index, newpoint, cluster_key):
    summary[cluster_key][0].append(index)
    summary[cluster_key][1] = summary[cluster_key][1] + 1
    for i in range(0, d):
        summary[cluster_key][2][i] += newpoint[i]
        summary[cluster_key][3][i] += newpoint[i] ** 2
    summary[cluster_key][4] = np.sqrt((summary[cluster_key][3][:] / summary[cluster_key][1]) - (
            np.square(summary[cluster_key][2][:]) / (summary[cluster_key][1] ** 2)))
    summary[cluster_key][5] = summary[cluster_key][2] / summary[cluster_key][1]


def merge_cs_clusters(cs1_key, cs2_key):
    compression_set[cs1_key][0].extend(compression_set[cs2_key][0])
    compression_set[cs1_key][1] = compression_set[cs1_key][1] + compression_set[cs2_key][1]
    for i in range(0, d):
        compression_set[cs1_key][2][i] += compression_set[cs2_key][2][i]
        compression_set[cs1_key][3][i] += compression_set[cs2_key][3][i]
    compression_set[cs1_key][4] = np.sqrt((compression_set[cs1_key][3][:] / compression_set[cs1_key][1]) - (
            np.square(compression_set[cs1_key][2][:]) / (compression_set[cs1_key][1] ** 2)))
    compression_set[cs1_key][5] = compression_set[cs1_key][2] / compression_set[cs1_key][1]


def merge_cs_with_ds(cs_key, ds_key):
    discard_set[ds_key][0].extend(compression_set[cs_key][0])
    discard_set[ds_key][1] = discard_set[ds_key][1] + compression_set[cs_key][1]
    for i in range(0, d):
        discard_set[ds_key][2][i] += compression_set[cs_key][2][i]
        discard_set[ds_key][3][i] += compression_set[cs_key][3][i]
    discard_set[ds_key][4] = np.sqrt((discard_set[ds_key][3][:] / discard_set[ds_key][1]) - (
            np.square(discard_set[ds_key][2][:]) / (discard_set[ds_key][1] ** 2)))
    discard_set[ds_key][5] = discard_set[ds_key][2] / discard_set[ds_key][1]


def get_cluster_dict(clusters):
    cluster_dict = {}
    for i in range(len(clusters)):
        clusterid = clusters[i]
        if clusterid in cluster_dict:
            cluster_dict[clusterid].append(i)
        else:
            cluster_dict[clusterid] = [i]
    return cluster_dict


def get_nearest_cluster_id(point, summary):
    nearest_cluster_md = threshold_distance
    nearest_clusterid = -1
    for key in summary.keys():
        std_dev = summary[key][4].astype(np.float)
        centroid = summary[key][5].astype(np.float)
        mahalanobis_distance = 0
        for dim in range(0, d):
            mahalanobis_distance += ((point[dim] - centroid[dim]) / std_dev[dim]) ** 2
        mahalanobis_distance = np.sqrt(mahalanobis_distance)
        if mahalanobis_distance < nearest_cluster_md:
            nearest_cluster_md = mahalanobis_distance
            nearest_clusterid = key
    return nearest_clusterid


def get_nearest_cluster_dict(summary1, summary2):
    cluster1_keys = summary1.keys()
    cluster2_keys = summary2.keys()
    nearest_cluster_id_map = {}
    for key1 in cluster1_keys:
        nearest_cluster_md = threshold_distance
        nearest_clusterid = key1
        for key2 in cluster2_keys:
            if key1 != key2:
                stddev1 = summary1[key1][4]
                centroid1 = summary1[key1][5]
                stddev2 = summary2[key2][4]
                centroid2 = summary2[key2][5]
                md1 = 0
                md2 = 0
                for dim in range(0, d):
                    if stddev2[dim] != 0 and stddev1[dim] != 0:
                        md1 += ((centroid1[dim] - centroid2[dim]) / stddev2[dim]) ** 2
                        md2 += ((centroid2[dim] - centroid1[dim]) / stddev1[dim]) ** 2
                mahalanobis_distance = min(np.sqrt(md1), np.sqrt(md2))
                if mahalanobis_distance < nearest_cluster_md:
                    nearest_cluster_md = mahalanobis_distance
                    nearest_clusterid = key2
        nearest_cluster_id_map[key1] = nearest_clusterid
    return nearest_cluster_id_map


# time /home/local/spark/latest/bin/spark-submit task.py $ASNLIB/publicdata/hw6_clustering.txt 10 output.csv

start_time = time.time()

# input_file = 'dataset/hw6_clustering.txt'
# num_clusters = 10
# output_file = 'output/output.csv'

input_file = sys.argv[1]
num_clusters = int(sys.argv[2])
output_file = sys.argv[3]

# ground_truth = np.loadtxt(input_file, delimiter=",")
# clustered_data = []
fin = open(input_file, "r")
data = np.array(fin.readlines())
fin.close()
fout = open(output_file, "w")

# Step 1. Load 20% of the data.

one_fifth = int(len(data) * 20 / 100)

start_index = 0
end_index = one_fifth
initial_sample = data[start_index:end_index]

pctr_index_map = {}
point_index_map = {}
first_load = []

point_ctr = 0
for line in initial_sample:
    line = line.split(",")
    index = line[0]
    point = line[2:]
    first_load.append(point)
    pctr_index_map[point_ctr] = index
    point_index_map[str(point)] = index
    point_ctr = point_ctr + 1

d = len(first_load[0])
threshold_distance = 2 * math.sqrt(d)
points_array = np.array(first_load)

# Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters) on the data in memory using the Euclidean distance as the similarity measurement.

kmeans = KMeans(n_clusters=5 * num_clusters, random_state=0)
clusters_extra = kmeans.fit_predict(points_array)
clusters = {}

for i in range(len(clusters_extra)):
    point = first_load[i]
    clusterid = clusters_extra[i]
    if clusterid in clusters:
        clusters[clusterid].append(point)
    else:
        clusters[clusterid] = [point]

# Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).

retained_set_dict = {}
for key in clusters.keys():
    if len(clusters[key]) == 1:
        point = clusters[key][0]
        pos = first_load.index(point)
        retained_set_dict[pctr_index_map[pos]] = point
        first_load.remove(point)
        for l in range(pos, len(pctr_index_map) - 1):
            pctr_index_map[l] = pctr_index_map[l + 1]

# Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.

points_array_without_RS = np.array(first_load)
kmeans = KMeans(n_clusters=num_clusters, random_state=0)
clusters = get_cluster_dict(kmeans.fit_predict(points_array_without_RS))

# Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).

discard_set = {}
for key in clusters.keys():
    create_discard_set(key, clusters[key], points_array_without_RS)

# The initialization of DS has finished, so far, you have K numbers of DS clusters (from Step 5) and some numbers of RS (from Step 3).

# Step 6. Run K-Means on the points in the RS with a large K to generate CS (clusters with more than one points) and RS (clusters with only one point).

retained_set_points = []
for key in retained_set_dict.keys():
    retained_set_points.append(retained_set_dict[key])

rs_points_array = np.array(retained_set_points)
kmeans = KMeans(n_clusters=int(len(retained_set_points) / 2 + 1), random_state=0)
cs_clusters = get_cluster_dict(kmeans.fit_predict(rs_points_array))

compression_set = {}
for key in cs_clusters.keys():
    if len(cs_clusters[key]) > 1:
        create_compression_set(key, cs_clusters[key], rs_points_array)

for key in cs_clusters.keys():
    if len(cs_clusters[key]) > 1:
        for i in cs_clusters[key]:
            point_to_remove = list(retained_set_dict.keys())[list(retained_set_dict.values()).index(retained_set_points[i])]
            del retained_set_dict[point_to_remove]

retained_set_points = []
for key in retained_set_dict.keys():
    retained_set_points.append(retained_set_dict[key])

fout.write("The intermediate results:\n")
output_intermediate(fout, 0)

# Step 7. Load another 20% of the data.
final_round = 4
for num_round in range(1, 5):
    start_index = end_index
    new_data = []
    if num_round == final_round:
        end_index = len(data)
        new_data = data[start_index:end_index]
    else:
        end_index = start_index + one_fifth
        new_data = data[start_index:end_index]

    points = []
    last_ctr = point_ctr
    for line in new_data:
        line = line.split(",")
        index = line[0]
        point = line[2:]
        points.append(point)
        pctr_index_map[point_ctr] = index
        point_index_map[str(point)] = index
        point_ctr = point_ctr + 1

    new_points_array = np.array(points)

    # Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign them to the nearest DS clusters if the distance is < 2√𝑑.
    # Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and assign the points to the nearest CS clusters if the distance is < 2√𝑑
    # Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.

    for i in range(len(new_points_array)):
        x = new_points_array[i]
        point = x.astype(np.float)
        index = pctr_index_map[last_ctr + i]
        closest_clusterid = get_nearest_cluster_id(point, discard_set)

        if closest_clusterid > -1:
            # Step 8
            update_summary(discard_set, index, point, closest_clusterid)
        else:
            closest_clusterid = get_nearest_cluster_id(point, compression_set)
            if closest_clusterid > -1:
                # Step 9
                update_summary(compression_set, index, point, closest_clusterid)
            else:
                # Step 10
                retained_set_dict[index] = list(x)
                retained_set_points.append(list(x))

    # Step 11. Run K-Means on the RS with a large K to generate CS (clusters with more than one points) and RS (clusters with only one point).

    new_points_array = np.array(retained_set_points)
    kmeans = KMeans(n_clusters=int(len(retained_set_points) / 2 + 1), random_state=0)

    cs_clusters = get_cluster_dict(kmeans.fit_predict(new_points_array))

    for key in cs_clusters.keys():
        if len(cs_clusters[key]) > 1:
            k = 0
            if key in compression_set.keys():
                while k in compression_set:
                    k = k + 1
            else:
                k = key
            create_compression_set(k, cs_clusters[key], new_points_array)

    for key in cs_clusters.keys():
        if len(cs_clusters[key]) > 1:
            for i in cs_clusters[key]:
                point_to_remove = point_index_map[str(retained_set_points[i])]
                if point_to_remove in retained_set_dict.keys():
                    del retained_set_dict[point_to_remove]

    retained_set_points = []
    for key in retained_set_dict.keys():
        retained_set_points.append(retained_set_dict[key])

    # Step 12. Merge CS clusters that have a Mahalanobis Distance < 2√𝑑.

    cs_keys = compression_set.keys()
    closest_cluster_map = get_nearest_cluster_dict(compression_set, compression_set)

    for cs_key in closest_cluster_map.keys():
        if cs_key != closest_cluster_map[cs_key] and closest_cluster_map[cs_key] in compression_set.keys() and cs_key in compression_set.keys():
            merge_cs_clusters(cs_key, closest_cluster_map[cs_key])
            del compression_set[closest_cluster_map[cs_key]]

    # If this is the last run , merge CS clusters with DS clusters that have a Mahalanobis Distance < 2√𝑑.

    if num_round == final_round:
        closest_cluster_map = get_nearest_cluster_dict(compression_set, discard_set)
        for cs_key in closest_cluster_map.keys():
            if closest_cluster_map[cs_key] in discard_set.keys() and cs_key in compression_set.keys():
                merge_cs_with_ds(cs_key, closest_cluster_map[cs_key])
                del compression_set[cs_key]

    output_intermediate(fout, num_round)

output_cluster_info(fout, discard_set, compression_set, retained_set_dict)
fout.close()

# clustered_data = np.array(clustered_data)
# score = normalized_mutual_info_score(ground_truth[:, 1], clustered_data[:, 1])
# print("Normalized Score: ", score)

print("Duration : ", time.time() - start_time)
