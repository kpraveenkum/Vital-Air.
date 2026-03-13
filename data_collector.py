"""
data_collector.py - Lambda function for data collection
Triggered by CloudWatch Events (every hour) to fetch and store data
"""

import json
import boto3
import os
import time
import math
import logging
from datetime import datetime
from decimal import Decimal
import urllib.request
import urllib.parse
from typing import Dict, List, Any, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# ========== ENVIRONMENT VARIABLES ==========
SENSORS_TABLE = os.environ.get('SENSORS_TABLE', 'air-quality-sensors')
HISTORICAL_TABLE = os.environ.get('HISTORICAL_TABLE', 'air-quality-historical')
DATA_BUCKET = os.environ.get('DATA_BUCKET', 'vital-air-data-eu-north-1')

# ========== API KEYS (from Lambda environment) ==========
TOMTOM_API_KEY = os.environ.get('TOMTOM_API_KEY', '')
NASA_FIRMS_API_KEY = os.environ.get('NASA_FIRMS_API_KEY', '')
OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY', '')
OPENAQ_API_KEY = os.environ.get('OPENAQ_API_KEY', '')
IQAIR_API_KEY = os.environ.get('IQAIR_API_KEY', '')

# ========== FREE API URLS ==========
OPEN_METEO_AQI_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
OPEN_METEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

# ========== LOCATIONS ==========
LOCATIONS = [
    # Delhi NCR
    {"name": "New Delhi", "lat": 28.6139, "lon": 77.2090, "state": "delhi"},
    {"name": "Anand Vihar", "lat": 28.6468, "lon": 77.3164, "state": "delhi"},
    {"name": "ITO", "lat": 28.6298, "lon": 77.2423, "state": "delhi"},
    {"name": "RK Puram", "lat": 28.5633, "lon": 77.1769, "state": "delhi"},
    {"name": "Dwarka", "lat": 28.5704, "lon": 77.0653, "state": "delhi"},
    {"name": "Rohini", "lat": 28.7344, "lon": 77.0895, "state": "delhi"},
    {"name": "Noida", "lat": 28.5355, "lon": 77.3910, "state": "delhi"},
    {"name": "Gurgaon", "lat": 28.4595, "lon": 77.0266, "state": "delhi"},
    
    # Maharashtra
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "state": "maharashtra"},
    {"name": "Pune", "lat": 18.5204, "lon": 73.8567, "state": "maharashtra"},
    {"name": "Nagpur", "lat": 21.1458, "lon": 79.0882, "state": "maharashtra"},
    {"name": "Nashik", "lat": 19.9975, "lon": 73.7898, "state": "maharashtra"},
]

# ========== HELPER FUNCTIONS ==========

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in km"""
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def calculate_aqi_from_pm25(pm25: float) -> int:
    """Calculate AQI from PM2.5"""
    if pm25 <= 12.0:
        return round((50/12.0) * pm25)
    elif pm25 <= 35.4:
        return round(((100-51)/(35.4-12.1)) * (pm25 - 12.1) + 51)
    elif pm25 <= 55.4:
        return round(((150-101)/(55.4-35.5)) * (pm25 - 35.5) + 101)
    elif pm25 <= 150.4:
        return round(((200-151)/(150.4-55.5)) * (pm25 - 55.5) + 151)
    elif pm25 <= 250.4:
        return round(((300-201)/(250.4-150.5)) * (pm25 - 150.5) + 201)
    else:
        return round(((500-301)/(500.4-250.5)) * (pm25 - 250.5) + 301)

def fetch_url(url: str, params: Dict = None) -> Optional[Dict]:
    """Fetch URL with parameters"""
    try:
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'VitalAir/1.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read()
            return json.loads(data)
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def fetch_openmeteo_aqi(lat: float, lon: float) -> Optional[Dict]:
    """Fetch air quality from Open-Meteo (FREE)"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["pm10", "pm2_5", "nitrogen_dioxide", "ozone", "carbon_monoxide"],
        "timeformat": "unixtime"
    }
    
    data = fetch_url(OPEN_METEO_AQI_URL, params)
    
    if data:
        current = data.get('current', {})
        pm25 = current.get('pm2_5')
        pm10 = current.get('pm10')
        no2 = current.get('nitrogen_dioxide')
        o3 = current.get('ozone')
        co_ugm3 = current.get('carbon_monoxide')
        co_ppm = round(co_ugm3 / 1150, 3) if co_ugm3 else None
        
        result = {
            'pm25': round(pm25, 1) if pm25 else None,
            'pm10': round(pm10, 1) if pm10 else None,
            'no2': round(no2, 1) if no2 else None,
            'o3': round(o3, 1) if o3 else None,
            'co': co_ppm,
            'source': 'openmeteo',
            'timestamp': current.get('time', int(datetime.now().timestamp()))
        }
        
        return result if pm25 else None
    
    return None

def fetch_openmeteo_weather(lat: float, lon: float) -> Optional[Dict]:
    """Fetch weather from Open-Meteo (FREE)"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", 
                   "wind_direction_10m", "pressure_msl"],
        "timeformat": "unixtime"
    }
    
    data = fetch_url(OPEN_METEO_WEATHER_URL, params)
    
    if data:
        current = data.get('current', {})
        return {
            'temperature': current.get('temperature_2m'),
            'humidity': current.get('relative_humidity_2m'),
            'wind_speed': current.get('wind_speed_10m'),
            'wind_direction': current.get('wind_direction_10m'),
            'pressure': current.get('pressure_msl'),
            'source': 'openmeteo'
        }
    
    return None

def fetch_tomtom_traffic(lat: float, lon: float) -> Optional[Dict]:
    """Fetch traffic from TomTom"""
    if not TOMTOM_API_KEY:
        return None
    
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {
        'key': TOMTOM_API_KEY,
        'point': f"{lat},{lon}",
        'unit': 'KMPH'
    }
    
    data = fetch_url(url, params)
    
    if data:
        flow_data = data.get('flowSegmentData', {})
        current_speed = flow_data.get('currentSpeed', 0)
        free_flow_speed = flow_data.get('freeFlowSpeed', 50)
        
        congestion = max(0, (free_flow_speed - current_speed) / free_flow_speed) if free_flow_speed > 0 else 0.5
        
        return {
            'congestion_ratio': round(congestion, 2),
            'current_speed': current_speed,
            'free_flow_speed': free_flow_speed,
            'source': 'tomtom'
        }
    
    return None

def fetch_nasa_fires(lat: float, lon: float, radius_km: int = 100) -> List[Dict]:
    """Fetch fire data from NASA FIRMS"""
    if not NASA_FIRMS_API_KEY:
        return []
    
    try:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{NASA_FIRMS_API_KEY}/VIIRS_SNPP_NRT/IND/1"
        
        req = urllib.request.Request(url, headers={'User-Agent': 'VitalAir/1.0'})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read().decode('utf-8')
            lines = data.strip().split('\n')
            
            if len(lines) < 2:
                return []
            
            header = lines[0].split(',')
            lat_idx = next((i for i, col in enumerate(header) if 'latitude' in col.lower()), -1)
            lon_idx = next((i for i, col in enumerate(header) if 'longitude' in col.lower()), -1)
            
            if lat_idx == -1 or lon_idx == -1:
                return []
            
            nearby_fires = []
            for line in lines[1:50]:
                values = line.split(',')
                if len(values) <= max(lat_idx, lon_idx):
                    continue
                
                try:
                    fire_lat = float(values[lat_idx])
                    fire_lon = float(values[lon_idx])
                    dist = haversine_distance(lat, lon, fire_lat, fire_lon)
                    
                    if dist <= radius_km:
                        nearby_fires.append({
                            'lat': fire_lat,
                            'lon': fire_lon,
                            'distance_km': round(dist, 1)
                        })
                except:
                    continue
            
            return nearby_fires
    except Exception as e:
        logger.error(f"NASA FIRMS error: {e}")
    
    return []

def store_sensor_data(location: Dict, aqi_data: Dict, weather_data: Dict, 
                     traffic_data: Optional[Dict], fires: List[Dict], timestamp: int) -> bool:
    """Store sensor data in DynamoDB and S3"""
    try:
        sensor_id = f"{location['name'].replace(' ', '_')}_{timestamp}"
        
        # Calculate AQI
        aqi = calculate_aqi_from_pm25(aqi_data.get('pm25', 0))
        
        # Prepare DynamoDB item
        item = {
            'sensor_id': sensor_id,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'location': location['name'],
            'state': location['state'],
            'latitude': Decimal(str(location['lat'])),
            'longitude': Decimal(str(location['lon'])),
            'pm25': Decimal(str(aqi_data.get('pm25', 0))) if aqi_data.get('pm25') else None,
            'pm10': Decimal(str(aqi_data.get('pm10', 0))) if aqi_data.get('pm10') else None,
            'no2': Decimal(str(aqi_data.get('no2', 0))) if aqi_data.get('no2') else None,
            'o3': Decimal(str(aqi_data.get('o3', 0))) if aqi_data.get('o3') else None,
            'co': Decimal(str(aqi_data.get('co', 0))) if aqi_data.get('co') else None,
            'aqi': Decimal(str(aqi)),
            'source': aqi_data.get('source', 'unknown')
        }
        
        # Add weather data
        if weather_data:
            if 'temperature' in weather_data and weather_data['temperature']:
                item['temperature'] = Decimal(str(weather_data['temperature']))
            if 'humidity' in weather_data and weather_data['humidity']:
                item['humidity'] = Decimal(str(weather_data['humidity']))
            if 'wind_speed' in weather_data and weather_data['wind_speed']:
                item['wind_speed'] = Decimal(str(weather_data['wind_speed']))
            if 'wind_direction' in weather_data and weather_data['wind_direction']:
                item['wind_direction'] = Decimal(str(weather_data['wind_direction']))
        
        # Add traffic data
        if traffic_data:
            item['congestion'] = Decimal(str(traffic_data['congestion_ratio']))
            item['traffic_speed'] = Decimal(str(traffic_data['current_speed']))
        
        # Add fire data
        if fires:
            item['nearby_fires'] = len(fires)
            item['fire_risk'] = 'high' if len(fires) > 3 else 'medium' if len(fires) > 0 else 'low'
        
        # Save to DynamoDB
        table = dynamodb.Table(SENSORS_TABLE)
        table.put_item(Item=item)
        
        # Also save to historical table
        hist_table = dynamodb.Table(HISTORICAL_TABLE)
        hist_item = item.copy()
        hist_item['sensor_id'] = f"hist_{sensor_id}"
        hist_table.put_item(Item=hist_item)
        
        # Save raw data to S3
        raw_data = {
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'location': location,
            'aqi': aqi_data,
            'weather': weather_data,
            'traffic': traffic_data,
            'fires': fires
        }
        
        s3.put_object(
            Bucket=DATA_BUCKET,
            Key=f"raw/{location['state']}/{location['name'].replace(' ', '_')}_{timestamp}.json",
            Body=json.dumps(raw_data, default=str),
            ContentType='application/json'
        )
        
        logger.info(f"✅ Stored data for {location['name']} - AQI: {aqi}")
        return True
        
    except Exception as e:
        logger.error(f"Error storing data for {location['name']}: {e}")
        return False

def lambda_handler(event, context):
    """Main Lambda handler"""
    logger.info("🌍 DATA COLLECTOR: Starting data collection")
    
    start_time = time.time()
    timestamp = int(start_time)
    execution_id = context.aws_request_id
    
    saved_count = 0
    failed_count = 0
    
    for location in LOCATIONS:
        logger.info(f"📍 Processing {location['name']}...")
        
        try:
            # Fetch AQI data
            aqi_data = fetch_openmeteo_aqi(location['lat'], location['lon'])
            
            # Fetch weather
            weather_data = fetch_openmeteo_weather(location['lat'], location['lon'])
            
            # Fetch traffic
            traffic_data = fetch_tomtom_traffic(location['lat'], location['lon'])
            
            # Fetch fires
            fires = fetch_nasa_fires(location['lat'], location['lon'])
            
            if aqi_data:
                success = store_sensor_data(
                    location, 
                    aqi_data, 
                    weather_data or {}, 
                    traffic_data, 
                    fires,
                    timestamp
                )
                
                if success:
                    saved_count += 1
                    logger.info(f"   ✅ Saved: {location['name']} - PM2.5: {aqi_data.get('pm25')}")
                else:
                    failed_count += 1
                    logger.error(f"   ❌ Failed to save {location['name']}")
            else:
                failed_count += 1
                logger.error(f"   ❌ No AQI data for {location['name']}")
                
        except Exception as e:
            failed_count += 1
            logger.error(f"   ❌ Error processing {location['name']}: {str(e)}")
    
    # Save summary to S3
    try:
        summary = {
            'execution_id': execution_id,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'total_locations': len(LOCATIONS),
            'saved_count': saved_count,
            'failed_count': failed_count
        }
        
        s3.put_object(
            Bucket=DATA_BUCKET,
            Key=f"summary/data_collector_{timestamp}.json",
            Body=json.dumps(summary, default=str),
            ContentType='application/json'
        )
        
        s3.put_object(
            Bucket=DATA_BUCKET,
            Key="summary/latest.json",
            Body=json.dumps(summary, default=str),
            ContentType='application/json'
        )
        
    except Exception as e:
        logger.error(f"Error saving summary: {e}")
    
    execution_time = time.time() - start_time
    logger.info(f"✅ Data collector completed in {execution_time:.2f}s - Saved: {saved_count}, Failed: {failed_count}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Data collection complete',
            'execution_id': execution_id,
            'timestamp': timestamp,
            'saved_count': saved_count,
            'failed_count': failed_count,
            'execution_time': round(execution_time, 2)
        })
    }
