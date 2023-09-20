from shapely.geometry import LineString as shapLS
from shapely.ops import unary_union
from shapely import MultiPoint
from geojson import LineString as geoLS
import pandas as pd
import numpy as np
import math
import geopandas as gpd
import geopy.distance
import pyproj
import shapely.geometry as geom
import osmnx as ox
import networkx as nx
import keplergl as kgl
import datetime as dt
import time
import datetime
import random

def extract_sample_network(north, east, south, west, sample_size,seed=None):
    """
    Extracts a sample of road networks within a bounding box and returns a dataframe of routes, assuming a route choice of shortest path.
    
    Parameters:
    -----------
    north : float
        The northernmost coordinate of the bounding box.
    south : float
        The southernmost coordinate of the bounding box.
    east : float
        The easternmost coordinate of the bounding box.
    west : float
        The westernmost coordinate of the bounding box.
    sample_size : int
        The number of routes to sample.
    seed : int, optional
        Seed value for the random number generator.
    
    Returns:
    --------
    df : pandas.DataFrame
        A dataframe with two columns: 'coordinates', which contains a list of coordinates for each route, and 'length_km', which contains the length of each route in kilometers.
    """
    
    # Set the seed value for the random number generator
    random.seed(seed)

    # Download the street network within the bounding box
    G = ox.graph_from_bbox(north, south, east, west, network_type='drive')

    # Get a list of all nodes in the graph
    all_nodes = list(G.nodes())

    # Initialize the list of routes
    routes = []

    # Extract a random sample of routes
    while len(routes) < sample_size:
        # Choose a random origin node
        origin = random.choice(all_nodes)

        # Find all nodes that are reachable from the origin
        reachable_nodes = nx.descendants(G, origin) # type: ignore

        if len(reachable_nodes) == 0:
            continue

        # Choose a random destination node from the reachable nodes
        destination = random.choice(list(reachable_nodes))

        # Check if a route exists
        try:
            route = nx.shortest_path(G, origin, destination, weight='travel_time')
            routes.append(route)
        except nx.NetworkXNoPath: # type: ignore
            continue

    # Extract the lat, lon values from every node
    coords = []
    lengths_km = []
    for route in routes:
        route_coords = []
        length_m = 0
        for i in range(len(route)-1):
            node1 = G.nodes[route[i]]
            node2 = G.nodes[route[i+1]]
            coords1 = (node1['x'], node1['y'])
            coords2 = (node2['x'], node2['y'])
            length_m += geopy.distance.distance(coords1, coords2).m
            route_coords.append([node1['x'], node1['y']])
        coords.append(route_coords)
        lengths_km.append(length_m/1000)

    df = pd.DataFrame({'coordinates': coords, 'length_km': lengths_km})

    return df


def trace_transform(df, speed, distance_delta):
    df_list = []
    
    # Partition initialization
    current_date = datetime.datetime.now().date()
    partition_scheme = [
        (0.1, datetime.datetime.combine(current_date, datetime.time(6, 0)), datetime.timedelta(hours=2)),  # 6 AM to 8 AM
        (0.2, datetime.datetime.combine(current_date, datetime.time(8, 0)), datetime.timedelta(hours=2)),  # 8 AM to 10 AM
        (0.3, datetime.datetime.combine(current_date, datetime.time(10, 0)), datetime.timedelta(hours=4)), # 10 AM to 2 PM
        (0.2, datetime.datetime.combine(current_date, datetime.time(14, 0)), datetime.timedelta(hours=2)), # 2 PM to 4 PM
        (0.2, datetime.datetime.combine(current_date, datetime.time(16, 0)), datetime.timedelta(hours=2)), # 4 PM to 6 PM
    ]
    
    total_rows = len(df)
    current_partition_index = 0
    rows_processed_in_partition = 0

    for index, row in df.iterrows():
        coordinates = row['coordinates']
        length_km = row['length_km']

        if len(coordinates) <= 1:
            print(f"Skipping row {index}: Not enough points to form a line.")
            continue
        
        try:
            # Assuming you have shapLS function defined elsewhere to generate the line from coordinates
            line = shapLS(coordinates)
            distances = np.arange(0, line.length, distance_delta)
            points = MultiPoint([line.interpolate(distance) for distance in distances])
            lat_lon_values = [[p.x, p.y] for p in points.geoms]

            # partition_length in km
            total_points = len(lat_lon_values)
            partition_length = (length_km / total_points)

            fraction, start_time, duration = partition_scheme[current_partition_index]
            partition_rows = int(fraction * total_rows)
            delay_per_row = duration / partition_rows

            # Calculate the start timestamp for the current row
            start_timestamp = start_time + rows_processed_in_partition * delay_per_row
            start_timestamp = start_timestamp.timestamp()

            row_timestamps = [int(start_timestamp)]
            for i in range(1, total_points):  # Starting from 1 because we already have the start timestamp
                time_delta = (partition_length / speed) * 3600
                next_timestamp = row_timestamps[-1] + time_delta
                row_timestamps.append(int(next_timestamp))

            rows_processed_in_partition += 1

            # Move to next partition if necessary
            if rows_processed_in_partition >= partition_rows:
                current_partition_index += 1
                rows_processed_in_partition = 0

            # Append the new row as a new dataframe to the list
            df_list.append(pd.DataFrame({'coordinates': [lat_lon_values], 'length_km': [length_km], 'timestamps_list': [row_timestamps]}))
        
        except Exception as e:
            print(f"Error processing row {index}: {str(e)}")
            continue

    # Concatenate all the dataframes in the list into a single dataframe
    new_df = pd.concat(df_list, ignore_index=True)

    return new_df
