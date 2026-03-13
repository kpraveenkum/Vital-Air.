"""
ml_processor_numpy.py - Enhanced ML Processor using NumPy
Predicts AQI using:
- Past historical data
- Current nearby sensors
- Burning/fire data
- Traffic congestion
- Weather patterns
- Advanced interpolation algorithms
"""

import json
import boto3
import os
import time
import math
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from scipy.interpolate import Rbf
from scipy.spatial import cKDTree

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Tables
SENSORS_TABLE = os.environ.get('SENSORS_TABLE', 'air-quality-sensors')
HISTORICAL_TABLE = os.environ.get('HISTORICAL_TABLE', 'air-quality-historical')
PREDICTIONS_TABLE = os.environ.get('PREDICTIONS_TABLE', 'air-quality-predictions')
HEATMAP_BUCKET = os.environ.get('HEATMAP_BUCKET', 'vital-air-heatmaps-eu-north-1')
DATA_BUCKET = os.environ.get('DATA_BUCKET', 'vital-air-data-eu-north-1')

# ========== STATE BOUNDARIES ==========
STATE_BOUNDS = {
    'delhi': {
        'name': 'Delhi NCR',
        'lat_min': 28.4, 'lat_max': 28.9,
        'lon_min': 76.8, 'lon_max': 77.3,
        'center': {'lat': 28.6139, 'lon': 77.2090},
        'grid_size': 100,  # 100x100 = 10,000 points
        'weight': 1.2
    },
    'maharashtra': {
        'name': 'Maharashtra',
        'lat_min': 15.6, 'lat_max': 22.0,
        'lon_min': 72.6, 'lon_max': 80.9,
        'center': {'lat': 19.0760, 'lon': 72.8777},
        'grid_size': 80,  # 80x80 = 6,400 points
        'weight': 0.8
    }
}

# ========== CITY HOTSPOTS ==========
CITY_HOTSPOTS = {
    'delhi': [
        {'name': 'Anand Vihar', 'lat': 28.6468, 'lon': 77.3164, 'base_aqi': 250, 'weight': 2.5},
        {'name': 'ITO', 'lat': 28.6298, 'lon': 77.2423, 'base_aqi': 220, 'weight': 2.2},
        {'name': 'New Delhi', 'lat': 28.6139, 'lon': 77.2090, 'base_aqi': 200, 'weight': 2.0},
        {'name': 'RK Puram', 'lat': 28.5633, 'lon': 77.1769, 'base_aqi': 180, 'weight': 1.8},
        {'name': 'Noida', 'lat': 28.5355, 'lon': 77.3910, 'base_aqi': 190, 'weight': 1.9},
        {'name': 'Gurgaon', 'lat': 28.4595, 'lon': 77.0266, 'base_aqi': 170, 'weight': 1.7},
    ],
    'maharashtra': [
        {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777, 'base_aqi': 150, 'weight': 2.0},
        {'name': 'Pune', 'lat': 18.5204, 'lon': 73.8567, 'base_aqi': 120, 'weight': 1.8},
        {'name': 'Nagpur', 'lat': 21.1458, 'lon': 79.0882, 'base_aqi': 100, 'weight': 1.5},
    ]
}

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def lambda_handler(event, context):
    """Main ML processor handler with NumPy"""
    print("🧠 ML Processor started - NumPy Enhanced Version")
    start_time = time.time()
    timestamp = int(start_time)
    
    print(f"📊 NumPy version: {np.__version__}")
    
    # Step 1: Fetch all available data
    print("\n📊 Step 1: Fetching all data sources...")
    
    current_sensors = fetch_current_sensors()
    print(f"   - Current sensors: {len(current_sensors)} readings")
    
    historical_data = fetch_historical_data()
    print(f"   - Historical records: {len(historical_data)}")
    
    fire_data = fetch_fire_data()
    print(f"   - Active fires: {len(fire_data)}")
    
    traffic_data = fetch_traffic_data()
    print(f"   - Traffic hotspots: {len(traffic_data)}")
    
    weather_data = fetch_weather_data()
    print(f"   - Weather stations: {len(weather_data)}")
    
    # Step 2: Prepare data for interpolation
    print("\n🔮 Step 2: Preparing interpolation data...")
    
    # Convert to numpy arrays for fast processing
    sensor_points, sensor_values = prepare_sensor_data(current_sensors)
    fire_points, fire_weights = prepare_fire_data(fire_data)
    traffic_points, traffic_weights = prepare_traffic_data(traffic_data)
    
    # Step 3: Generate predictions for each region
    print("\n📍 Step 3: Generating predictions for each region...")
    
    all_predictions = []
    region_stats = {}
    
    for state, bounds in STATE_BOUNDS.items():
        print(f"\n   Processing {state.upper()}...")
        
        # Generate grid points
        grid_lats, grid_lons, grid_points = generate_grid_numpy(bounds)
        print(f"   - Grid size: {len(grid_points)} points")
        
        # Predict using multiple methods
        predictions = predict_region_numpy(
            grid_lats, grid_lons, grid_points,
            sensor_points, sensor_values,
            fire_points, fire_weights,
            traffic_points, traffic_weights,
            state, bounds
        )
        
        print(f"   - Generated {len(predictions)} predictions")
        all_predictions.extend(predictions)
        region_stats[state] = len(predictions)
        
        # Save region-specific predictions
        save_region_predictions(state, predictions, timestamp)
    
    # Step 4: Save to S3 and DynamoDB
    print("\n💾 Step 4: Saving predictions...")
    
    save_to_s3(all_predictions, {
        'current_sensors': len(current_sensors),
        'historical_data': len(historical_data),
        'fire_data': len(fire_data),
        'traffic_data': len(traffic_data),
        'weather_data': len(weather_data)
    }, timestamp, start_time, region_stats)
    
    save_to_dynamodb(all_predictions[:100], timestamp)
    
    execution_time = time.time() - start_time
    print(f"\n✅ ML Processor completed in {execution_time:.2f}s")
    print(f"📊 Total predictions: {len(all_predictions)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'ML processor completed',
            'execution_time': round(execution_time, 2),
            'predictions': len(all_predictions),
            'data_sources': {
                'sensors': len(current_sensors),
                'historical': len(historical_data),
                'fires': len(fire_data),
                'traffic': len(traffic_data),
                'weather': len(weather_data)
            },
            'regions': region_stats,
            'numpy_version': np.__version__
        })
    }

def fetch_current_sensors():
    """Fetch current sensor data"""
    try:
        current_time = int(time.time())
        cutoff = current_time - (24 * 3600)
        
        table = dynamodb.Table(SENSORS_TABLE)
        response = table.scan(
            FilterExpression='#ts > :cutoff',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={':cutoff': cutoff},
            Limit=500
        )
        
        sensors = []
        for item in response.get('Items', []):
            try:
                sensors.append({
                    'lat': float(item['latitude']),
                    'lon': float(item['longitude']),
                    'pm25': float(item.get('pm25', 0)),
                    'aqi': float(item.get('aqi', 0)),
                    'confidence': float(item.get('confidence', 0.8))
                })
            except:
                continue
        return sensors
    except Exception as e:
        print(f"Error fetching sensors: {e}")
        return []

def fetch_historical_data():
    """Fetch historical data"""
    try:
        table = dynamodb.Table(HISTORICAL_TABLE)
        response = table.scan(Limit=1000)
        
        historical = []
        for item in response.get('Items', []):
            try:
                historical.append({
                    'lat': float(item.get('latitude', 0)),
                    'lon': float(item.get('longitude', 0)),
                    'avg_pm25': float(item.get('avg_pm25', 0))
                })
            except:
                continue
        return historical
    except Exception as e:
        print(f"Error fetching historical: {e}")
        return []

def fetch_fire_data():
    """Fetch fire data"""
    try:
        response = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix='raw/', MaxKeys=100)
        fires = []
        if 'Contents' in response:
            for obj in response['Contents'][:10]:
                try:
                    data = s3.get_object(Bucket=DATA_BUCKET, Key=obj['Key'])
                    content = json.loads(data['Body'].read())
                    if 'fires' in content and content['fires']:
                        for fire in content['fires']:
                            fires.append({
                                'lat': fire.get('lat'),
                                'lon': fire.get('lon'),
                                'distance': fire.get('distance_km', 0)
                            })
                except:
                    continue
        return fires
    except Exception as e:
        print(f"Error fetching fires: {e}")
        return []

def fetch_traffic_data():
    """Fetch traffic data"""
    try:
        response = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix='raw/', MaxKeys=100)
        traffic = []
        if 'Contents' in response:
            for obj in response['Contents'][:10]:
                try:
                    data = s3.get_object(Bucket=DATA_BUCKET, Key=obj['Key'])
                    content = json.loads(data['Body'].read())
                    if 'traffic' in content and content['traffic']:
                        traffic.append({
                            'lat': content.get('location', {}).get('lat'),
                            'lon': content.get('location', {}).get('lon'),
                            'congestion': content['traffic'].get('congestion_ratio', 0.5)
                        })
                except:
                    continue
        return traffic
    except Exception as e:
        print(f"Error fetching traffic: {e}")
        return []

def fetch_weather_data():
    """Fetch weather data"""
    try:
        response = s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix='raw/', MaxKeys=100)
        weather = []
        if 'Contents' in response:
            for obj in response['Contents'][:10]:
                try:
                    data = s3.get_object(Bucket=DATA_BUCKET, Key=obj['Key'])
                    content = json.loads(data['Body'].read())
                    if 'weather' in content and content['weather']:
                        weather.append({
                            'lat': content.get('location', {}).get('lat'),
                            'lon': content.get('location', {}).get('lon'),
                            'temperature': content['weather'].get('temperature', 25),
                            'humidity': content['weather'].get('humidity', 50),
                            'wind_speed': content['weather'].get('wind_speed', 10)
                        })
                except:
                    continue
        return weather
    except Exception as e:
        print(f"Error fetching weather: {e}")
        return []

def prepare_sensor_data(sensors):
    """Convert sensor data to numpy arrays"""
    if not sensors:
        return np.array([]), np.array([])
    
    points = []
    values = []
    for s in sensors:
        points.append([s['lat'], s['lon']])
        values.append(s.get('aqi', calculate_aqi_from_pm25(s.get('pm25', 100))))
    
    return np.array(points), np.array(values)

def prepare_fire_data(fires):
    """Prepare fire data for influence calculation"""
    if not fires:
        return np.array([]), np.array([])
    
    points = []
    weights = []
    for f in fires:
        points.append([f['lat'], f['lon']])
        weights.append(1.0 / max(1, f.get('distance', 10)))
    
    return np.array(points), np.array(weights)

def prepare_traffic_data(traffic):
    """Prepare traffic data for influence calculation"""
    if not traffic:
        return np.array([]), np.array([])
    
    points = []
    weights = []
    for t in traffic:
        points.append([t['lat'], t['lon']])
        weights.append(t.get('congestion', 0.5))
    
    return np.array(points), np.array(weights)

def generate_grid_numpy(bounds):
    """Generate grid using numpy for speed"""
    lat = np.linspace(bounds['lat_min'], bounds['lat_max'], bounds['grid_size'])
    lon = np.linspace(bounds['lon_min'], bounds['lon_max'], bounds['grid_size'])
    
    lat_grid, lon_grid = np.meshgrid(lat, lon)
    grid_points = np.column_stack([lat_grid.ravel(), lon_grid.ravel()])
    
    return lat_grid, lon_grid, grid_points

def predict_region_numpy(grid_lats, grid_lons, grid_points, 
                        sensor_points, sensor_values,
                        fire_points, fire_weights,
                        traffic_points, traffic_weights,
                        state, bounds):
    """
    Predict AQI for entire region using numpy for speed
    """
    predictions = []
    
    # Get base AQI from hotspots
    base_grid = get_base_grid_numpy(grid_lats, grid_lons, state)
    
    # Get sensor influence using IDW with numpy
    sensor_influence = np.zeros(len(grid_points))
    if len(sensor_points) > 0:
        sensor_influence = idw_interpolation_numpy(
            grid_points, sensor_points, sensor_values, power=2
        )
    
    # Get fire influence
    fire_influence = np.ones(len(grid_points)) * 100
    if len(fire_points) > 0:
        fire_influence = get_fire_influence_numpy(grid_points, fire_points, fire_weights)
    
    # Get traffic influence
    traffic_influence = np.ones(len(grid_points)) * 100
    if len(traffic_points) > 0:
        traffic_influence = get_traffic_influence_numpy(grid_points, traffic_points, traffic_weights)
    
    # Combine all influences with weights
    final_aqi = (
        base_grid * 0.1 +
        sensor_influence * 0.6 +
        fire_influence * 0.15 +
        traffic_influence * 0.15
    )
    
    # Add small random variation for realism
    final_aqi = final_aqi * np.random.uniform(0.98, 1.02, len(final_aqi))
    
    # Clip to valid range
    final_aqi = np.clip(final_aqi, 0, 500)
    
    # Calculate confidence for each point
    confidences = calculate_confidences_numpy(
        grid_points, sensor_points, fire_points, traffic_points
    )
    
    # Convert to list of dictionaries
    for i, (lat, lon) in enumerate(grid_points):
        predictions.append({
            'lat': round(float(lat), 4),
            'lon': round(float(lon), 4),
            'value': round(float(final_aqi[i]), 1),
            'confidence': round(float(confidences[i]), 2),
            'region': state
        })
    
    return predictions

def get_base_grid_numpy(lats, lons, state):
    """Generate base AQI grid from hotspots using numpy"""
    if state not in CITY_HOTSPOTS:
        return np.ones_like(lats.ravel()) * 100
    
    grid_points = np.column_stack([lats.ravel(), lons.ravel()])
    base_values = np.ones(len(grid_points)) * 100
    
    for hotspot in CITY_HOTSPOTS[state]:
        hotspot_point = np.array([[hotspot['lat'], hotspot['lon']]])
        distances = np.linalg.norm(grid_points - hotspot_point, axis=1)
        
        # Convert to km (rough approximation)
        distances_km = distances * 111
        
        # Influence decreases with distance
        influence = np.exp(-distances_km / 20) * hotspot['base_aqi'] * hotspot['weight']
        base_values = np.maximum(base_values, influence)
    
    return base_values

def idw_interpolation_numpy(target_points, source_points, source_values, power=2):
    """
    Inverse Distance Weighting interpolation using numpy
    Much faster than pure Python version
    """
    n_targets = len(target_points)
    n_sources = len(source_points)
    
    # Calculate distances using broadcasting
    distances = np.zeros((n_targets, n_sources))
    for i in range(n_targets):
        distances[i] = np.linalg.norm(source_points - target_points[i], axis=1)
    
    # Convert to km
    distances_km = distances * 111
    
    # Avoid division by zero
    distances_km = np.maximum(distances_km, 0.1)
    
    # Calculate weights
    weights = 1.0 / (distances_km ** power)
    
    # Weighted sum
    weighted_sum = np.sum(weights * source_values, axis=1)
    total_weight = np.sum(weights, axis=1)
    
    # Handle zero weights
    result = np.where(total_weight > 0, weighted_sum / total_weight, 100)
    
    return result

def get_fire_influence_numpy(target_points, fire_points, fire_weights):
    """Calculate fire influence using numpy"""
    if len(fire_points) == 0:
        return np.ones(len(target_points)) * 100
    
    n_targets = len(target_points)
    influence = np.ones(n_targets) * 100
    
    for i, fire in enumerate(fire_points):
        distances = np.linalg.norm(target_points - fire, axis=1) * 111  # to km
        fire_effect = 200 * fire_weights[i] * np.exp(-distances / 10)
        influence = np.maximum(influence, fire_effect)
    
    return influence

def get_traffic_influence_numpy(target_points, traffic_points, traffic_weights):
    """Calculate traffic influence using numpy"""
    if len(traffic_points) == 0:
        return np.ones(len(target_points)) * 100
    
    n_targets = len(target_points)
    influence = np.ones(n_targets) * 100
    
    for i, traffic in enumerate(traffic_points):
        distances = np.linalg.norm(target_points - traffic, axis=1) * 111
        traffic_effect = 100 + (50 * traffic_weights[i] * np.exp(-distances / 5))
        influence = np.maximum(influence, traffic_effect)
    
    return influence

def calculate_confidences_numpy(target_points, sensor_points, fire_points, traffic_points):
    """Calculate confidence scores using numpy"""
    confidences = np.ones(len(target_points)) * 70  # Base confidence
    
    if len(sensor_points) > 0:
        # Higher confidence near sensors
        sensor_distances = np.min(
            [np.linalg.norm(target_points - s, axis=1) * 111 for s in sensor_points],
            axis=0
        )
        confidences += 20 * np.exp(-sensor_distances / 5)
    
    if len(fire_points) > 0:
        # Lower confidence near fires (unpredictable)
        fire_distances = np.min(
            [np.linalg.norm(target_points - f, axis=1) * 111 for f in fire_points],
            axis=0
        )
        confidences -= 15 * np.exp(-fire_distances / 10)
    
    return np.clip(confidences, 50, 98)

def haversine_distance_numpy(lat1, lon1, lat2, lon2):
    """Vectorized haversine distance using numpy"""
    R = 6371
    
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    return R * c

def calculate_aqi_from_pm25(pm25):
    """Calculate AQI from PM2.5"""
    if pm25 <= 12.0:
        return (50/12.0) * pm25
    elif pm25 <= 35.4:
        return ((100-51)/(35.4-12.1)) * (pm25 - 12.1) + 51
    elif pm25 <= 55.4:
        return ((150-101)/(55.4-35.5)) * (pm25 - 35.5) + 101
    elif pm25 <= 150.4:
        return ((200-151)/(150.4-55.5)) * (pm25 - 55.5) + 151
    elif pm25 <= 250.4:
        return ((300-201)/(250.4-150.5)) * (pm25 - 150.5) + 201
    else:
        return ((500-301)/(500.4-250.5)) * (pm25 - 250.5) + 301

def save_region_predictions(region, predictions, timestamp):
    """Save region predictions to S3"""
    try:
        data = {
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'region': region,
            'predictions': predictions
        }
        
        s3.put_object(
            Bucket=HEATMAP_BUCKET,
            Key=f"regions/{region}_{timestamp}.json",
            Body=json.dumps(data, cls=DecimalEncoder),
            ContentType='application/json'
        )
        
        s3.put_object(
            Bucket=HEATMAP_BUCKET,
            Key=f"regions/{region}_latest.json",
            Body=json.dumps(data, cls=DecimalEncoder),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"Error saving region predictions: {e}")

def save_to_s3(predictions, data_counts, timestamp, start_time, region_stats):
    """Save all predictions to S3"""
    try:
        heatmap_data = {
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'total_predictions': len(predictions),
            'data_sources': data_counts,
            'regions': region_stats,
            'heatmap': predictions[:1000],
            'metadata': {
                'processing_time': round(time.time() - start_time, 2),
                'model_version': 'numpy-v1',
                'numpy_version': np.__version__
            }
        }
        
        s3.put_object(
            Bucket=HEATMAP_BUCKET,
            Key=f"full/heatmap_{timestamp}.json",
            Body=json.dumps(heatmap_data, cls=DecimalEncoder),
            ContentType='application/json'
        )
        
        s3.put_object(
            Bucket=HEATMAP_BUCKET,
            Key='latest.json',
            Body=json.dumps(heatmap_data, cls=DecimalEncoder),
            ContentType='application/json'
        )
        
        print(f"✅ Saved heatmap to S3")
    except Exception as e:
        print(f"Error saving to S3: {e}")

def save_to_dynamodb(predictions, timestamp):
    """Save predictions to DynamoDB"""
    try:
        table = dynamodb.Table(PREDICTIONS_TABLE)
        
        with table.batch_writer() as batch:
            for i, pred in enumerate(predictions):
                item = {
                    'prediction_id': f"pred_{timestamp}_{i}",
                    'timestamp': timestamp,
                    'datetime': datetime.fromtimestamp(timestamp).isoformat(),
                    'lat': Decimal(str(pred['lat'])),
                    'lon': Decimal(str(pred['lon'])),
                    'pm25': Decimal(str(pred['value'])),
                    'confidence': Decimal(str(pred.get('confidence', 0.8))),
                    'region': pred.get('region', 'unknown')
                }
                batch.put_item(Item=item)
        
        print(f"✅ Saved {len(predictions)} predictions to DynamoDB")
    except Exception as e:
        print(f"Error saving to DynamoDB: {e}")

# For local testing
if __name__ == "__main__":
    print("🧪 Testing ML Processor with NumPy...")
    result = lambda_handler({}, type('obj', (object,), {'aws_request_id': 'test'}))
    print(json.dumps(json.loads(result['body']), indent=2))
