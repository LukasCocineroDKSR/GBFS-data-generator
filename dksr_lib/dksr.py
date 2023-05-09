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
import random

from datetime import datetime, timedelta
from keplergl import KeplerGl
from geojson import LineString as geoLS
from shapely.geometry import LineString as shapLS
from shapely.ops import unary_union
from shapely import MultiPoint

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

    map_0 = KeplerGl(height=800, data={'Scooters': geo_list})#, config=trip_layer_config.config)
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

