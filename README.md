
## Vital Air

**Vital Air** is a hyper-local air quality intelligence platform that predicts pollution blind spots and visualizes dynamic risk zones using spatial interpolation and short-term forecasting.

---

## Problem Statement

City-wide AQI averages hide dangerous pollution hotspots. Sparse monitoring stations fail to warn commuters about hazardous air just a few kilometers away. There is a lack of real-time, location-specific intelligence answering a simple question:

With rising urban pollution and climate-driven anomalies, static AQI systems are no longer sufficient for real-time health decisions.

**“Is it safe to breathe here, right now?”**

---

## Solution Overview

Vital Air fills air-quality blind spots by estimating pollution levels between monitoring stations and predicting short-term PM2.5 trends. The system leverages cloud-based backend services and data-driven models to generate dynamic heatmaps and risk zones on an interactive map.

---

## Key Features

- Hyper-local PM2.5 estimation in sensor blind spots  
- Spatial interpolation using Inverse Distance Weighting (IDW)  
- Short-term air quality prediction (Now, +3h, +6h, +12h)  
- Dynamic heatmap overlay with red (hazardous) and green (safer) zones  
- Cloud-based scalable backend architecture  
- Multi-source environmental data fusion  

---
---

## Unique Differentiators

- **Blind Spot Coverage:** Goes beyond official AQI averages by interpolating unsensed areas.  
- **Predictive Intelligence:** Short-term forecasts (+3h, +6h, +12h) help commuters plan safer routes.  
- **Multi-Source Fusion:** Weather, traffic, fire anomalies, and meteorological data enrich predictions.  
- **Citizen-Centric Design:** Simple map interface answers the question: *“Is it safe to breathe here, right now?”*  

---

## Example Use Cases

- **Commuters:** Check if their route passes through hazardous zones.  
- **Schools:** Decide whether outdoor activities are safe.  
- **Hospitals:** Alert vulnerable patients about upcoming spikes.  
- **Policy Makers:** Identify hidden hotspots for targeted interventions.  

---

## Dynamic Heatmap & Prediction Logic

- Sparse monitoring data is interpolated across a city grid  
- Pollution values are estimated for unsensed locations  
- Temporal and environmental signals adjust short-term predictions  
- Heatmap updates dynamically based on prediction time selection  
- Color intensity represents relative pollution severity  

---

## Cloud & Backend Architecture

The backend is deployed on **AWS** with the following components:

- **AWS Lambda** – Serverless computation for data processing  
- **Amazon EC2** – Backend server execution (via MobaXterm)  
- **Amazon S3** – Storage for datasets and processed outputs  
- **Amazon DynamoDB** – NoSQL storage for air-quality records  
- **AWS IAM** – Secure access and role management  
- **Elastic IP** – Stable public endpoint for backend services  
- **Amazon CloudWatch** – Logging and monitoring  

Backend services are implemented in **Python 3.9.25**.

---

## Modeling & Libraries

- **Language:** Python 3.9.25  
- **Libraries:**  
  - NumPy  
  - JSON  
  - Boto3 (AWS SDK for Python)  

---

## Data Sources & APIs

Vital Air integrates multiple public APIs to enrich prediction accuracy:

- WAQI API – Air pollution data  
- Weather API – Weather conditions  
- Traffic API – Traffic intensity data  
- Meteorological API – Forecasts  
- Fire/thermal anomaly API – Fire risk data  

---

## Demo Coverage

- **Delhi**  
- **Maharashtra (Mumbai / Pune)**  

The system architecture supports scaling to additional cities across India.

---

## Future Roadmap

- **Mobile App Integration** (push notifications for personal AQI alerts)  
- **IoT Sensor Expansion** (plug-and-play with low-cost sensors)  
- **AI Model Upgrade** (deep learning for temporal-spatial forecasting)  
- **Global Scaling** (extend beyond India to other high-risk cities worldwide)  

---

<img width="600" height="750" alt="image" src="https://github.com/user-attachments/assets/d327b566-6ccc-4738-ba59-c2923b7836b3" />



---

## Workflow

-Data Collection
-WAQI API → PM2.5 readings
-Weather API → Temperature, humidity, wind
-Traffic API → Congestion levels
-Fire anomaly API → Thermal hotspots
-Data Storage
-Raw API data → S3
-Processed records → DynamoDB
-Spatial Interpolation
-Grid-generator
-Apply Inverse Distance Weighting (IDW)
-Estimate PM2.5 in blind spots
-Prediction Engine
-Short-term forecasting:
-Now +3h +6h +12h
-Heatmap Rendering → S3
-Grid-based pollution values
-Color-coded severity zones
-Real-time updates on user selection


### ⚙️ Setup Instructions

## Step 1: AWS Setup using IAM user
-aws configure
-Enter Access Key, Secret Key, region: eu-north-1

## step 2: Create

-Three S3 buckets
-Three DynamoDB tables,

## step 3 : Permissions policies attach with IAM user 

-AdministratorAccess
-AmazonDynamoDBFullAccess
-AmazonEC2FullAccess
-AWSLambda_FullAccess
-AmazonS3FullAccess
-AWSLambdaDynamoDBExecutionRole
-CloudWatchLogsFullAccess
-AmazonS3ObjectLambdaExecutionRolePolicy

## Step 4: Install Required Packages

-pip install fastapi uvicorn boto3 numpy python-dotenv httpx
-pip install python and remaining python library 

## Step 5: Run the Backend

python -m uvicorn main:app --host 0.0.0.0 --port 3000

or 

python file-name

---

### Feasibility

-Uses publicly available APIs
-Serverless + EC2 hybrid reduces infrastructure cost
-Python-based lightweight implementation
-No expensive ML hardware required

---

### Scalability

-DynamoDB auto-scales
-Lambda handles burst traffic
-S3 provides unlimited storage
-Easily extendable to:
-Additional Indian cities
-International locations
-Architecture supports horizontal scaling with minimal reconfiguration.

---

### Sustainability Impact

-Vital Air contributes to:
-Public Health Protection
-Early warning for vulnerable populations
-Reduced Exposure
-Route optimization for commuters
-Data-Driven Governance
-Identify hidden hotspots
-Enable targeted interventions
-Climate Awareness
-Improve citizen understanding of environmental risk.

---
