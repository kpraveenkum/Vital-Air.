"""
grid_generator.py - Generate grid points for ML heatmap interpolation
"""

import math
import random
from typing import List, Tuple, Dict, Any

# ========== STATE BOUNDARIES ==========
STATE_BOUNDS = {
    'delhi': {
        'name': 'Delhi NCR',
        'lat_min': 28.4, 'lat_max': 28.9,
        'lon_min': 76.8, 'lon_max': 77.3,
        'center': {'lat': 28.6139, 'lon': 77.2090},
        'area_km2': 5500,
        'grid_density': {
            'low': 50,      # 2,500 points (every ~1.1km)
            'medium': 100,  # 10,000 points (every ~550m)
            'high': 200,    # 40,000 points (every ~275m)
        }
    },
    'maharashtra': {
        'name': 'Maharashtra',
        'lat_min': 15.6, 'lat_max': 22.0,
        'lon_min': 72.6, 'lon_max': 80.9,
        'center': {'lat': 19.0760, 'lon': 72.8777},
        'area_km2': 307713,
        'grid_density': {
            'low': 50,      # 2,500 points (every ~14km)
            'medium': 100,  # 10,000 points (every ~7km)
            'high': 150,    # 22,500 points (every ~4.7km)
        }
    }
}

# ========== MAJOR CITIES FOR ENHANCED RESOLUTION ==========
CITY_HOTSPOTS = {
    'delhi': [
        {'name': 'New Delhi', 'lat': 28.6139, 'lon': 77.2090, 'weight': 2.0, 'aqi_factor': 1.3},
        {'name': 'Anand Vihar', 'lat': 28.6468, 'lon': 77.3164, 'weight': 2.5, 'aqi_factor': 1.5},
        {'name': 'ITO', 'lat': 28.6298, 'lon': 77.2423, 'weight': 2.2, 'aqi_factor': 1.4},
        {'name': 'RK Puram', 'lat': 28.5633, 'lon': 77.1769, 'weight': 1.8, 'aqi_factor': 1.2},
        {'name': 'Dwarka', 'lat': 28.5704, 'lon': 77.0653, 'weight': 1.5, 'aqi_factor': 1.1},
        {'name': 'Rohini', 'lat': 28.7344, 'lon': 77.0895, 'weight': 1.5, 'aqi_factor': 1.1},
        {'name': 'Noida', 'lat': 28.5355, 'lon': 77.3910, 'weight': 2.0, 'aqi_factor': 1.3},
        {'name': 'Gurgaon', 'lat': 28.4595, 'lon': 77.0266, 'weight': 1.8, 'aqi_factor': 1.2},
    ],
    'maharashtra': [
        {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777, 'weight': 2.5, 'aqi_factor': 1.3},
        {'name': 'Pune', 'lat': 18.5204, 'lon': 73.8567, 'weight': 2.0, 'aqi_factor': 1.2},
        {'name': 'Nagpur', 'lat': 21.1458, 'lon': 79.0882, 'weight': 1.8, 'aqi_factor': 1.1},
        {'name': 'Nashik', 'lat': 19.9975, 'lon': 73.7898, 'weight': 1.5, 'aqi_factor': 1.0},
        {'name': 'Aurangabad', 'lat': 19.8762, 'lon': 75.3433, 'weight': 1.3, 'aqi_factor': 1.0},
    ]
}

def generate_grid_points(state: str = 'delhi', density: str = 'medium') -> List[Tuple[float, float]]:
    """
    Generate a uniform grid of points for a state
    """
    if state not in STATE_BOUNDS:
        print(f"⚠️ Unknown state: {state}, defaulting to delhi")
        state = 'delhi'
    
    bounds = STATE_BOUNDS[state]
    steps = bounds['grid_density'].get(density, bounds['grid_density']['medium'])
    
    grid_points = []
    lat_step = (bounds['lat_max'] - bounds['lat_min']) / steps
    lon_step = (bounds['lon_max'] - bounds['lon_min']) / steps
    
    for i in range(steps):
        lat = bounds['lat_min'] + i * lat_step
        for j in range(steps):
            lon = bounds['lon_min'] + j * lon_step
            grid_points.append((round(lat, 4), round(lon, 4)))
    
    return grid_points

def generate_adaptive_grid(state: str = 'delhi', 
                          base_density: str = 'medium', 
                          enhance_cities: bool = True) -> List[Tuple[float, float]]:
    """
    Generate adaptive grid with higher density around major cities
    """
    # Start with base grid
    grid_points = generate_grid_points(state, base_density)
    
    if not enhance_cities or state not in CITY_HOTSPOTS:
        return grid_points
    
    bounds = STATE_BOUNDS[state]
    cities = CITY_HOTSPOTS[state]
    
    # Add extra points around each city based on its weight
    for city in cities:
        # Higher weight = more points
        extra_points = int(150 * city['weight'])
        
        for _ in range(extra_points):
            # Random offset within ~3km radius
            lat_offset = (random.random() - 0.5) * 0.05
            lon_offset = (random.random() - 0.5) * 0.05
            
            new_lat = city['lat'] + lat_offset
            new_lon = city['lon'] + lon_offset
            
            # Ensure within state bounds
            if (bounds['lat_min'] <= new_lat <= bounds['lat_max'] and
                bounds['lon_min'] <= new_lon <= bounds['lon_max']):
                grid_points.append((round(new_lat, 4), round(new_lon, 4)))
    
    return grid_points

def generate_weighted_grid_points(state: str = 'delhi', 
                                 density: str = 'medium',
                                 sensor_locations: List[Dict[str, Any]] = None) -> List[Tuple[float, float]]:
    """
    Generate grid points with weights based on sensor density
    """
    # Start with adaptive grid
    grid_points = generate_adaptive_grid(state, density)
    
    if not sensor_locations:
        return grid_points
    
    bounds = STATE_BOUNDS[state]
    
    # Count sensors per area to identify clusters
    sensor_grid = {}
    for sensor in sensor_locations:
        lat_key = round(sensor['lat'], 1)
        lon_key = round(sensor['lon'], 1)
        key = (lat_key, lon_key)
        sensor_grid[key] = sensor_grid.get(key, 0) + 1
    
    # Add extra points in high-density sensor areas
    for (lat_key, lon_key), count in sensor_grid.items():
        if count >= 2:  # Area with multiple sensors
            # Add extra points proportional to sensor count
            extra_points = count * 20
            
            for _ in range(extra_points):
                lat = lat_key + (random.random() - 0.5) * 0.2
                lon = lon_key + (random.random() - 0.5) * 0.2
                
                if (bounds['lat_min'] <= lat <= bounds['lat_max'] and
                    bounds['lon_min'] <= lon <= bounds['lon_max']):
                    grid_points.append((round(lat, 4), round(lon, 4)))
    
    return list(set(grid_points))  # Remove duplicates

def get_grid_statistics(state: str, density: str = 'medium') -> Dict[str, Any]:
    """
    Get statistics about the generated grid
    """
    if state not in STATE_BOUNDS:
        return {"error": "Unknown state"}
    
    bounds = STATE_BOUNDS[state]
    steps = bounds['grid_density'].get(density, bounds['grid_density']['medium'])
    
    lat_range = bounds['lat_max'] - bounds['lat_min']
    lon_range = bounds['lon_max'] - bounds['lon_min']
    
    lat_step = lat_range / steps
    lon_step = lon_range / steps
    
    # Approximate distance in km
    lat_dist_km = lat_step * 111  # 1° ≈ 111 km
    lon_dist_km = lon_step * 111 * math.cos(math.radians((bounds['lat_min'] + bounds['lat_max']) / 2))
    
    return {
        'state': state,
        'density': density,
        'grid_points': steps * steps,
        'resolution_km': {
            'lat': round(lat_dist_km, 2),
            'lon': round(lon_dist_km, 2),
            'avg': round((lat_dist_km + lon_dist_km) / 2, 2)
        },
        'bounds': bounds
    }

def optimize_grid_for_lambda(state: str = 'delhi', max_points: int = 10000) -> List[Tuple[float, float]]:
    """
    Generate grid optimized for AWS Lambda (memory and time constraints)
    """
    bounds = STATE_BOUNDS[state]
    
    # Calculate required density to stay under max_points
    total_area_points = (bounds['lat_max'] - bounds['lat_min']) * (bounds['lon_max'] - bounds['lon_min'])
    target_density = math.sqrt(max_points / total_area_points)
    
    steps = int(target_density * 100)  # Scale appropriately
    
    grid_points = []
    lat_step = (bounds['lat_max'] - bounds['lat_min']) / steps
    lon_step = (bounds['lon_max'] - bounds['lon_min']) / steps
    
    for i in range(steps):
        lat = bounds['lat_min'] + i * lat_step
        for j in range(steps):
            lon = bounds['lon_min'] + j * lon_step
            grid_points.append((round(lat, 4), round(lon, 4)))
    
    return grid_points
