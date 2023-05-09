from shapely.geometry import LineString as shapLS
from shapely.ops import unary_union
from shapely import MultiPoint
from dksr_lib.dksr import *

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
    """
    Takes a pandas dataframe with columns 'coordinates' and 'length_km', and returns a new dataframe with three columns: 'coordinates', 'length_km', and 'timestamps_list'.
    Each row in the new dataframe contains a list of coordinates, a length in kilometers, and a list of Unix timestamps indicating the time at which each point was reached, assuming a constant speed between points.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        A dataframe with two columns: 'coordinates', which contains a list of coordinate pairs for each route, and 'length_km', which contains the length of each route in kilometers.
    speed : float
        The average speed of movement, in kilometers per hour.
    distance_delta : float
        The distance between consecutive points, in kilometers.
    
    Returns:
    --------
    new_df : pandas.DataFrame
        A dataframe with three columns: 'coordinates', 'length_km', and 'timestamps_list', where 'coordinates' contains a list of coordinates, 'length_km' contains the length of the route in kilometers, and 'timestamps_list' contains a list of Unix timestamps indicating the time at which each point was reached.
    """
    new_df = pd.DataFrame(columns=['coordinates', 'length_km', 'timestamps_list'])
    
    # iterate over each row in the input dataframe
    for index, row in df.iterrows():
        coordinates = row['coordinates']
        length_km = row['length_km']
        
        # create points by given distance_delta
        line = shapLS(coordinates)
        distances = np.arange(0, line.length, distance_delta)
        points = MultiPoint([line.interpolate(distance) for distance in distances])
        lat_lon_values = [[p.x, p.y] for p in points.geoms]

        # partition_length in km   
        total_points = len(lat_lon_values)
        partition_length = (length_km / total_points)

        # time distance in seconds
        time_delta = (partition_length / speed) * 3600

        row_timestamps = []
        
        for i in range(total_points):
            if i == 0:
                # set initial timestamp to current time
                row_timestamps.append(int(time.time()))
            else:
                # calculate the time delta from the previous point
                time_delta = (partition_length / speed) * 3600
                # add the time delta to the previous timestamp
                prev_timestamp = row_timestamps[-1]
                next_timestamp = prev_timestamp + time_delta
                # append the new timestamp to the list
                row_timestamps.append(int(next_timestamp))
        
        # append the new row to the output dataframe
        new_df = new_df.append({'coordinates': lat_lon_values, 'length_km': length_km, 'timestamps_list': row_timestamps}, ignore_index=True)

    return new_df