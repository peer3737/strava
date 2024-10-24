from supporting.strava import Strava
from datetime import datetime, timedelta
import logging
from database.db import Connection
import boto3
import json
import os


formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] %(message)s')
log = logging.getLogger()
log.setLevel("INFO")
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
for handler in log.handlers:
    log.removeHandler(handler)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)
db = Connection()
strava = Strava()


# init

def lambda_handler(event, context):
    lambda_client = boto3.client('lambda')
    try:
        find_activities = db.get_all(table='activity', order_by='start_date_local', order_by_type='desc')
        start_date = str(find_activities[9] + timedelta(days=1))[0:10] + " 00:00:00.000000"
        end_date = "2100-01-01 00:00:00.000000"
        log.info(
            f"Getting activities between {str(find_activities[9] + timedelta(days=1))[0:10] + ' 00:00:00'} and 2100-01-01 00:00:00")

        # start_date = '2023-07-01 00:00:00.000000'
        # end_date = '2023-07-23 00:00:00.000000'
        start_unix = int(datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
        end_unix = int(datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S.%f").timestamp())
        page = 1
        page_size = 200

        # get all activities
        all_activities = strava.getactvities(start_date=start_unix, end_date=end_unix, page=page, pagesize=page_size)
        log.info(f"Found {len(all_activities)} activities")

        # log.info(all_activities)
        for activity in all_activities:
            activity_keys = ['id', 'name', 'description', 'distance', 'moving_time', 'elapsed_time',
                             'total_elevation_gain',
                             'type', 'sport_type', 'workout_type', 'start_date_local', 'average_heartrate',
                             'max_heartrate',
                             'suffer_score']
            content = {}
            activity_id = activity['id']

            activity_details = strava.activity(activity_id=activity_id)
            for activity_key in activity_keys:
                if activity_key in activity_details:
                    content[activity_key] = activity_details[activity_key]
                else:
                    content[activity_key] = None

            db.insert(table='activity', json_data=content)

            # get activity streams
            streams = strava.activity_stream(activity_id=activity_id)
            stream_keys = ["time", "distance", "latlng", "heartrate", "altitude", "cadence"]
            content = {
                "activity_id": activity_id
            }
            for stream_key in stream_keys:
                if stream_key in streams:
                    content[stream_key] = ','.join(map(str, streams[stream_key]["data"]))
                else:
                    content[stream_key] = None

            db.insert(table='activity_streams', json_data=content)

            # get activity laps
            lap_keys = ['name', 'split', 'distance', 'moving_time', 'elapsed_time', 'start_index',
                        'total_elevation_gain', 'average_cadence', 'average_heartrate', 'max_heartrate', 'pace_zone']
            laps = strava.activity_laps(activity_id=activity_id)
            content = []
            for lap in laps:
                lap_content = {
                    "activity_id": activity_id
                }
                for lap_key in lap_keys:
                    if lap_key in lap:
                        lap_content[lap_key] = lap[lap_key]
                    else:
                        lap_content[lap_key] = None
                content.append(lap_content)
            if len(content) > 0:
                db.insert(table='activity_laps', json_data=content, mode='many')
            else:
                log.info(f"No laps detected for activity with ID={activity_id}")
    except Exception as e:
        log.error('Something went wrong')
        log.error(e)
        payload = {
            "to": os.environ['MAIL_CONTACT'],
            "subject": "Strava sync went wrong",
            "content": str(e)
        }
        lambda_client.invoke(
            FunctionName='sendMail',  # Replace with the name of your sendMail function
            InvocationType='Event',  # Use 'RequestResponse' for synchronous invocation
            Payload=json.dumps(payload)
        )
