import pandas as pd
import numpy as np
import math
import geopandas as gpd
import geopy.distance
import pyproj
import shapely.geometry as geom
from shapely.geometry import Point, LineString
import osmnx as ox
import networkx as nx
import keplergl as kgl
import datetime as dt
import time
import random
from datetime import datetime, timedelta
from geojson import LineString
from keplergl import KeplerGl

#Entfernen unnötiger Spalten
def clean_columns(data,add_col=[], inplace=True):
    drop_columns = ['vehicle_type','accuracy','propulsion_types','SID','timestamp','_headers.eventType'] + add_col
    for header in data.keys():
        if header in drop_columns:
            data.drop([header],axis=1,inplace=inplace)


def get_origin_target(data):
    data['origin'] = data['coordinates'].apply(lambda x: x[0])
    data['target'] = data['coordinates'].apply(lambda x: x[-1])


#Abstand zwischen zwei Koordinaten-Punkten
def geo_distance(point1, point2):
    R = 6378.137 # Radius of earth in KM
    dLat = point2[1] * math.pi / 180 - point1[1] * math.pi / 180
    dLon = point2[0] * math.pi / 180 - point1[0] * math.pi / 180
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(point1[1] * math.pi / 180) * math.cos(point2[1] * math.pi / 180) * math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d * 1000 # meters



#Liegen Start- und/oder Zielpunkt eines Trips in der Nähe eines POI
def point_of_interest(list=[],point=[],radius=200):
    d_0 = geo_distance(list[0],point)
    d_1 = geo_distance(list[-1],point)
    if d_0 <= radius:
        if d_1 <= radius:
            return 'both'
        else:
            return 'origin'
    elif d_1 <= radius:
        return 'target'
    else:
        return 'null'



def points_of_interest(data,points=[[]],radius=200):
    poi = []
    points = pd.Series(points)
    for i in range(len(data)):
        list = data['coordinates'][i]
        d_0 = points.apply(lambda x: geo_distance(list[0],x))
        d_1 = points.apply(lambda x: geo_distance(list[-1],x))
        if (d_0 <= radius).any():
            if (d_1 <= radius).any():
                poi.append('both')
            else:
                poi.append('origin')
        elif (d_1 <= radius).any():
            poi.append('target')
        else:
            poi.append('null')
    return poi



def old_points_of_interest(list=[],points_df=[],radius=200):
    d_0 = points_df.apply(lambda x: geo_distance(list[0],x))
    d_1 = points_df.apply(lambda x: geo_distance(list[-1],x))
    if (d_0 <= radius).any():
        if (d_1 <= radius).any():
            return 'both'
        else:
            return 'origin'
    elif (d_1 <= radius).any():
        return 'target'
    else:
        return 'null'



#Wartezeit vor der Benutzung
def wait_list(data):
    waited = pd.DataFrame(['null'] * len(data), columns=['waited'])
    cc = 0
    for vehicle in data['vehicle_id'].unique():
        triplist = data[data['vehicle_id'] == vehicle]
        for i in range(0,len(triplist)):
            if i == 0:
                cc += 1
            else:
                waited['waited'].iloc[cc] = triplist['start_time'].iloc[i] - triplist['end_time'].iloc[i-1]
                cc += 1

    data['waited'] = waited



def trip_layer(data):
    geo_list = pd.DataFrame(np.zeros((len(data)),dtype=object),columns=['geo_json'])
    geo_no_time = pd.DataFrame(np.zeros((len(data)),dtype=object),columns=['geo_json'])
                            
    for i in range(0,len(data)):                       
        z_list = [0] * len(data['timestamps_list'][i])
        list0 = data['coordinates'][i]
        list1 = np.insert(list0,2,z_list,axis=1)
        list2 = np.insert(list1,3,data['timestamps_list'][i],axis=1)
        
        geo_list.iloc[i] = [LineString(list2.tolist())]
        geo_no_time.iloc[i] = [LineString(list1.tolist())]

    map_0 = KeplerGl(height=800, data={'Scooters': geo_list}, config=trip_layer_config.config)
    return map_0


def trip_list(data):
    trips = pd.DataFrame([])
    for k in range(0,len(data)):
        coord = pd.DataFrame(data['coordinates'][k],columns=['trip_lng','trip_lat'])
        timest = pd.DataFrame(data['timestamps_list'][k],columns=['time'])
        route = pd.DataFrame(np.zeros((len(coord),1)),columns=['route_id'])
        v = pd.DataFrame(np.zeros((len(coord),1)),columns=['velocity'])
        for i in range(1,len(coord)-1):
            dt = (timest.loc[i+1]-timest.loc[i]) / 1000 # seconds
            route.iloc[i] = data['trip_id'][k]
            ds = geo_distance(coord.loc[i],coord.loc[i+1])
            if dt[0] == 0:
                v.iloc[i] = 0
            else:
                v.iloc[i] = ((ds)/(dt))*3.6 # km/h
        trip = pd.concat([route,coord,timest,v],axis=1)
        trips = pd.concat([trips,trip],axis=0)
        
    #trips['time'] = trips['time'].apply(lambda x: pd.to_datetime(x, unit='ms', origin='unix'))
    return trips


def velocity_layer(data):
    #trips = trip_list(data)
    map_1 = KeplerGl(height=800, data={'Trips': data}, config=velocity_layer_config.config)
    return map_1

def extract_sample_network(north, east, south, west, sample_size):
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
    
    Returns:
    --------
    df : pandas.DataFrame
        A dataframe with two columns: 'coordinates', which contains a list of coordinates for each route, and 'length_km', which contains the length of each route in kilometers.
    """
    
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
            coords1 = (node1['y'], node1['x'])
            coords2 = (node2['y'], node2['x'])
            length_m += geopy.distance.distance(coords1, coords2).m
            route_coords.append([node1['y'], node1['x']])
        coords.append(route_coords)
        lengths_km.append(length_m/1000)

    routes = pd.DataFrame({'coordinates': coords, 'length_km': lengths_km})

    return routes