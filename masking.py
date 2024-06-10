import boto3
import psycopg2
import hashlib
import json

def mask_pii(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()

def flatten_json(json_obj):
    flattened_data = {
        "user_id": json_obj.get("user_id"),
        "device_type": json_obj.get("device_type"),
        "masked_ip": mask_pii(json_obj.get("ip")),
        "masked_device_id": mask_pii(json_obj.get("device_id")),
        "locale": json_obj.get("locale"),
        "app_version": json_obj.get("app_version"),
        "create_date": json_obj.get("create_date")  
    }
    return flattened_data

def write_to_postgres(records):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for record in records:
        cursor.execute(insert_query, (
            record["user_id"],
            record["device_type"],
            record["masked_ip"],
            record["masked_device_id"],
            record["locale"],
            record["app_version"],
            record["create_date"]
        ))

    conn.commit()
    cursor.close()
    conn.close()

def consume_messages(queue_url):
    sqs = boto3.client('sqs', endpoint_url='http://localhost:4566')
    total_processed_records = 0
    print("Starting message consumption...")
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10  
        )
        messages = response.get('Messages', [])
        if not messages:
            print("No more messages in the queue. Exiting...")
            break 
        processed_records = []
        for message in messages:
            body = message['Body']
            data = json.loads(body)
            flattened_data = flatten_json(data)
            processed_records.append(flattened_data)
            
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        total_processed_records += len(processed_records)
        print(f"Processed {len(processed_records)} records. Total processed records: {total_processed_records}")
        write_to_postgres(processed_records)
    print("Message consumption completed.")

def main():
    queue_url = 'http://localhost:4566/000000000000/login-queue'  
    consume_messages(queue_url)
    print("ETL process completed.")

if __name__ == "__main__":
    main()