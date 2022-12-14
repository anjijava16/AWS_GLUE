import os

try:
    import boto3
    import json
    from datetime import datetime
    import calendar
    import random
    import time
    import json
    from faker import Faker
    import uuid
    from time import sleep
    from random import randint

    from dotenv import load_dotenv
    load_dotenv("../Streams/.env")
except Exception as e:
    pass


my_stream_name = 'clicksStreams'


kinesis_client = boto3.client('kinesis',
                              region_name='us-east-1',
                              aws_access_key_id=os.getenv("DEV_AWS_ACCESS_KEY"),
                              aws_secret_access_key=os.getenv("DEV_AWS_SECRET_KEY")
                              )
fakerr = Faker()

class Datetime(object):
    @staticmethod
    def get_year_month_day_hour_minute_seconds():
        """
        Return Year month and day
        :return: str str str
        """
        dt = datetime.now()
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        minute = dt.minute
        seconds = dt.second
        return year, month, day, hour,minute, seconds

def run():

    for i in range(1, 100):
        year, month, day, hour,minute, seconds= Datetime.get_year_month_day_hour_minute_seconds()
        json_data = {
            "_id": uuid.uuid4().__str__(),
            "job_id": randint(1, 20),
            "referrer": str(fakerr.url()),
            "agent": random.choice(['Chrome','Safari','Edge']),
            "agent_version": "13",
            "os": random.choice(['Android','ios','windows']),
            "os_version": "13.7.0",
            "device": random.choice(["iPhone", 'IPad','Laptop','Mobile']),
            "language": "en",
            "location": {
                "longitude": int(fakerr.longitude()),
                "latitude": int(fakerr.latitude()),
                "area_code": 707.0,
                "continent_code": "NA",
                "dma_code": 807.0,
                "city": fakerr.city(),
                "region": str(fakerr.country()),
                "country_code": fakerr.country(),
                "isp": "AS7922 Comcast Cable Communications, LLC",
                "postal_code": str(randint(10000, 100000)),
                "country": str(fakerr.country()),
                "region_code": fakerr.state()
            },
            "date": datetime.now().strftime('%Y-%m-%d'),
            "year":year.__str__(),
            'month':month.__str__(),
            "day":day.__str__(),
            "hour":hour.__str__(),
            "minute":minute.__str__(),
            "seconds":seconds.__str__(),
            "column_to_update_integer": random.randint(0,1000000000),
            "column_to_update_string": random.choice(["White","Red","Yellow", "Silver"]),
            "name":random.choice(["Person1","Person2","Person3", "Person4"])

        }
        print(json_data)

        sleep(1)
        put_response = kinesis_client.put_record(
            StreamName=my_stream_name,
            Data=json.dumps(json_data),
            PartitionKey='name')
        print(put_response)

run()
