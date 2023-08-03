from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.s3_key_sensor import S3KeySensor
import requests
from airflow.operators.python_operator import PythonOperator
import hashlib
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 15),
}


def get_LA_data(api_url):
    try:
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()

            # Select only the desired fields from each record
            selected_data = []
            for record in data:
                selected_record = {
                    'spaceid': record.get('spaceid'),
                    'blockface': record.get('blockface'),
                    'metertype': record.get('metertype'),
                    'ratetype': record.get('ratetype'),
                    'raterange': record.get('raterange'),
                    'timelimit': record.get('timelimit'),
                    'latlng': record.get('latlng')
                }
                selected_data.append(selected_record)

            return selected_data
        else:
            print(f"Failed to get data from {api_url}. Status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error occurred while making the request to {api_url}: {e}")
        return None

def get_and_merge_data(api_url1, api_url2, common_key='spaceid'):
    data_1 = get_LA_data(api_url1)

    try:
        response_2 = requests.get(api_url2)

        if response_2.status_code == 200:
            data_2 = response_2.json()
        else:
            print(f"Failed to get data from {api_url2}. Status code: {response_2.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Error occurred while making the request to {api_url2}: {e}")
        return None

    merged_json = []
    key_set = set(item[common_key] for item in data_2)

    for item1 in data_1:
        key = item1[common_key]
        for item2 in data_2:
            if item2[common_key] == key:
                merged_item = {**item1, **item2}
                merged_json.append(merged_item)
                break

    return merged_json


def calculate_data_hash(data):
    # Convert the data to a JSON string
    data_str = json.dumps(data, sort_keys=True)
    
    # Check if already existent.
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

def upload_data_to_s3(bucket_name, folder_name, **context):
    data = context['task_instance'].xcom_pull(task_ids='fetch_and_merge_data')
    try:
        # Initialize S3Hook
        s3_hook = S3Hook(aws_conn_id="insert_connection")

        # Use a fixed S3 key for the file object
        file_name = "data.json"
        s3_key = folder_name + file_name

        #the hash of the new data
        data_hash = calculate_data_hash(data)

        # Check if the file with the same key already exists in S3
        existing_data = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)

        # Calculate the hash of the existing data (if it exists)
        existing_data_hash = calculate_data_hash(existing_data) if existing_data else None

        # Compare the hashes to check for duplicates
        if existing_data_hash != data_hash:
            # Upload the JSON data to S3, which will overwrite the existing object with the same key
            s3_hook.load_string(json.dumps(data), key=s3_key, bucket_name=bucket_name)
            print(f"Data uploaded to S3 bucket: {bucket_name}, folder: {folder_name}, filename: {file_name}")
        else:
            print("Data already exists in S3. Skipping upload to avoid duplicates.")
    except Exception as e:
        print(f"Error occurred while uploading data to S3: {e}")

#Step-4 From s3 bucket to postgresql
#create the tables
def create_tables(**kwargs):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='nejjfqyu',
        user= 'nejjfqyu',
        password= 'Xd27RYX7w3VkVAkbxUN9Q94A9lmVdmEV',
        port='5432',
        host= 'stampy.db.elephantsql.com'
    ) # Replace with actual credentials
    cursor = conn.cursor()

    try:
        try:
            cursor.execute('DROP TABLE IF EXISTS parking_clean;')
            print("Table 'parking_clean' dropped successfully.")
        except Error as drop_error:
            print(f"Error occurred while dropping table: {drop_error}")

        # Create the tables
        cursor.execute('CREATE TABLE IF NOT EXISTS parking_clean (spaceid  VARCHAR, blockface  VARCHAR, metertype  VARCHAR, ratetype  VARCHAR, raterange  VARCHAR, timelimit  VARCHAR, latlng  JSON, eventtime  VARCHAR, occupancystate  VARCHAR);')
        
        conn.commit()
        print("Tables created successfully.")

    finally:
        # Close the database connection
        if conn is not None:
            cursor.close()
            conn.close()


def upload_to_postgres(bucket_name, folder_name, **kwargs):
    s3_hook = S3Hook(aws_conn_id='insert_connection')  # Specify your AWS connection ID

    # Fetch data (list of file keys)
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix=folder_name)

    # Check if any files are found
    if not objects:
        raise ValueError("No files found in the specified S3 folder.")

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='nejjfqyu',
        user= 'nejjfqyu',
        password= 'Xd27RYX7w3VkVAkbxUN9Q94A9lmVdmEV',
        port='5432',
        host= 'stampy.db.elephantsql.com'
    )

    try:
        # Create a new cursor and execute SQL statements
        cursor = conn.cursor()

        try:
            # Process each file
            for file_key in objects:
                # Read the JSON file from S3
                json_data = s3_hook.read_key(file_key, bucket_name=bucket_name)

                # Parse the JSON data
                try:
                    json_objects = json.loads(json_data)
                    print("JSON data loaded successfully.")

                    # Iterate over the JSON objects and insert into PostgreSQL
                    for obj in json_objects:

                        spaceid = obj['spaceid']
                        blockface = obj['blockface']
                        metertype = obj['metertype']
                        ratetype = obj['ratetype']
                        raterange = obj['raterange']
                        timelimit = obj['timelimit']
                        latlng = json.dumps(obj['latlng'])
                        eventtime = obj['eventtime']
                        occupancystate = obj['occupancystate']
                  
                        
                            # Insert into PostgreSQL table
                        cursor.execute("INSERT INTO parking_clean (spaceid, blockface, metertype, ratetype, raterange, timelimit, latlng, eventtime, occupancystate) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s)",
                                           (spaceid, blockface, metertype, ratetype, raterange, timelimit, latlng, eventtime, occupancystate))
                            
                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Invalid JSON data in file '{file_key}': {e}")

        finally:
            # Close the cursor
            if cursor is not None:
                cursor.close()

    finally:
        # Close the database connection
        if conn is not None:
            conn.close()
    
        

api_url1 = "https://data.lacity.org/resource/s49e-q6j2.json"
api_url2 = "https://data.lacity.org/resource/e7h6-4a3e.json"
bucket_name = 'insert_bucket_name'  # Replace with your S3 bucket name
folder_name = 'capdata/'  # Replace with the desired folder name

with DAG("to_psql", schedule_interval=None, default_args=default_args) as dag:
    fetch_and_merge_task = PythonOperator(
        task_id="fetch_and_merge_data",
        python_callable=get_and_merge_data,
        op_args=[api_url1, api_url2, "spaceid"]
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_data_to_s3,
        op_args=[bucket_name, folder_name],
        provide_context=True,
    )
    
    s3_sensor = S3KeySensor(
        task_id='s3_sensor',
        bucket_key=f'{folder_name}',
        wildcard_match=True,
        bucket_name=bucket_name,
        timeout=60 * 60 * 24,  # 1 day timeout
        poke_interval=60,  # Check every minute
    )
    
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
        provide_context=True,
    )
 
    upload_to_postgres_task = PythonOperator(
        task_id="upload_to_postgres",
        python_callable=upload_to_postgres,
        op_kwargs={
            'bucket_name': 'insert_bucket_name',
            'folder_name': 'capdata/'
        },
        provide_context=True
    )

    fetch_and_merge_task >> upload_to_s3_task >> s3_sensor >>  create_tables_task >> upload_to_postgres_task
