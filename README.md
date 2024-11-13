
# Sparkify Data Warehouse ETL Pipeline

### Project Overview
Sparkify, a music streaming startup, has grown its user base and song database and is moving its data processes to the cloud. As a data engineer, the task is to build an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms it into a set of dimensional tables for analytics. This allows the analytics team to gain insights into users' listening habits.

## Tools and Technologies

- **Languages:** Python, SQL
- **Libraries and Frameworks:** Jupyter Notebook
- **AWS Services:** Redshift, S3, EC2, IAM, VPC, Boto3, CLI
- **Data Processing Tool:** AWS Redshift with S3 integration

## AWS Configuration

### 1. IAM Role Creation
- Create `myRedshiftRole` with `AmazonS3ReadOnlyAccess` permissions.

### 2. Security Group Creation
- Set up `redshift_security_group` to authorize access to the Redshift cluster.

### 3. IAM User for Redshift
- Create an IAM user with `AmazonRedshiftFullAccess` and `AmazonS3ReadOnlyAccess` permissions.

### 4. Redshift Cluster Launch
- Set up `redshift-cluster-1` using `myRedshiftRole` and `redshift_security_group`.
- **Note:** Remember to delete the cluster after use to avoid unexpected costs.

---

## Data Exploration and Setup

1. **Configure AWS CLI:**
   
Input your AWS Access Key ID, AWS Secret Access Key, and default region.

### Explore Data in S3: List S3 bucket contents:


aws s3 ls s3://udacity-dend/log_data/2018/11/
aws s3 ls s3://udacity-dend/song_data/A/A/A/
Download Sample Data Locally:

aws s3 cp s3://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json sample_data/
aws s3 cp s3://udacity-dend/log_data/2018/11/2018-11-30-events.json sample_data/
aws s3 cp s3://udacity-dend/log_json_path.json sample_data/

## ETL Pipeline Overview

### Files in the Project

1. sql_queries.py: Contains SQL queries for creating, dropping, and inserting into tables.
2. create_tables.py: Initializes the database schema, including staging, fact, and dimension tables.
3. etl.py: Extracts data from S3, loads it into Redshift staging tables, and then transforms the data into a star schema for analysis.
4. etl_test.ipynb: Validates AWS configurations and the ETL process.

## Database Schema for Song Play Analysis

### Staging Tables

#### staging_events: Raw log data on user activity.

 Columns: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

#### staging_songs: Song metadata.

 Columns: artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year

#### Fact Table

##### songplays: Records user song plays.

Columns: songplay_id (PK), start_time, user_id, level, song_id, artist_id, session_id, duration, user_agent

#### Dimension Tables

##### users: Information on Sparkify users.

Columns: user_id (PK), first_name, last_name, gender, level

##### songs: Information on songs in the database.

Columns: song_id (PK), title, artist_id, year, duration

##### artists: Information on song artists.

Columns: artist_id (PK), name, location, latitude, longitude

##### time: Timestamps of records in songplays, broken down into specific units.

Columns: start_time (PK), hour, day, week, month, year, weekday

