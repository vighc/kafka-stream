import json
import csv
import boto3
import datetime

def kafka_backup(event, context):
  # Get the Kafka message from the event.
  kafka_message = event["data"]

  # Get the current date.
  today = datetime.date.today()

  # Get the CSV file name for today.
  csv_file_name = "stock_market_{}.csv".format(today.strftime("%Y%m%d"))

  # Check if the CSV file exists.
  s3 = boto3.resource("s3")
  csv_file = s3.Object(bucket=event["bucket"], key=csv_file_name)
  if not csv_file.exists():
    # The CSV file does not exist, so create it.
    with open(csv_file_name, "w") as csvfile:
      writer = csv.writer(csvfile, delimiter=",")
      writer.writerow(["STOCK_NAME", "CURR_VALUE", "CLOSE_VALUE", "CURRENT_TIME"])

  # Parse the Kafka message.
  STOCK_NAME, CURR_VALUE, CLOSE_VALUE, CURRENT_TIME = kafka_message.split(",")

  # Update the CSV file with the Kafka message.
  with open(csv_file_name, "a") as csvfile:
    writer = csv.writer(csvfile, delimiter=",")
    writer.writerow([STOCK_NAME, CURR_VALUE, CLOSE_VALUE, CURRENT_TIME])

  # Return the success message.
  return {"message": "Success!"}