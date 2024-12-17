import logging
import os
from datetime import datetime, timedelta
from supporting.strava import Strava
from supporting import aws
from database.db import Connection
import boto3
import json


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


def lambda_handler(event, context):
    is_test = os.getenv('IS_TEST') == 'true'
    database_id = os.getenv('DATABASE_ID')
    database_settings = aws.dynamodb_query(table='database_settings', id=database_id)
    db_host = database_settings[0]['host']
    db_user = database_settings[0]['user']
    db_password = database_settings[0]['password']
    db_port = database_settings[0]['port']
    db = Connection(user=db_user, password=db_password, host=db_host, port=db_port, charset="utf8mb4")
    strava = Strava(db)
    devices_result = db.get_all(table='device', type='all')
    devices_in_db = {}
    for device in devices_result:
        devices_in_db[device[1]] = device[0]

    lambda_client = boto3.client('lambda')

    try:
        find_activities = db.get_all(table='activity', order_by='start_date_local', order_by_type='desc')
        start_date = f"{str(find_activities[9] + timedelta(seconds=1))}.000000"
        end_date = "2100-01-01 00:00:00.000000"
        log.info(
            f"Getting activities between {str(find_activities[9] + timedelta(seconds=1))} and 2100-01-01 00:00:00")

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
                             'suffer_score', 'gear_id', 'device_name']
            content = {}
            activity_id = activity['id']

            activity_details = strava.activity(activity_id=activity_id)
            for activity_key in activity_keys:
                if activity_key in activity_details:
                    content[activity_key] = activity_details[activity_key]
                    if activity_key == 'start_date_local':
                        content[activity_key] = f'{content[activity_key][0:10]} {content[activity_key][11:19]}'
                    if activity_key == 'device_name':
                        if activity_details[activity_key] in devices_in_db:
                            content[activity_key] = devices_in_db[activity_details[activity_key]]

                        else:
                            device_input = {
                                "name": content[activity_key]
                            }
                            db.insert(table='device', json_data=device_input)
                            devices_result = db.get_all(table='device', type='all')
                            devices_in_db = {}
                            for device in devices_result:
                                devices_in_db[device[1]] = device[0]

                            content[activity_key] = devices_in_db[activity_details[activity_key]]
                else:
                    content[activity_key] = None
            if content['elapsed_time'] is None:
                content['average_speed'] = 0
            elif float(content['elapsed_time']) == 0:
                content['average_speed'] = 0
            else:
                content['average_speed'] = float(content['distance']) / float(content['elapsed_time'])

            db.insert(table='activity', json_data=content)

            log.info("Update streams")
            function_name = "strava-streams-test" if is_test else "strava-streams"

            payload = {
                "activity_id": activity_id
            }
            result = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",  # Asynchronous invocation
                Payload=json.dumps(payload)
            )
            payload_result = json.loads(result['Payload'].read())
            latlng = json.loads(payload_result)['latlng']

            log.info("Update laps")
            function_name = "strava-laps-test" if is_test else "strava-laps"
            payload = {
                "activity_id": activity_id
            }
            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event",  # Asynchronous invocation
                Payload=json.dumps(payload)
            )

            if latlng is not None:
                log.info("Update weather")
                function_name = "strava-weather-test" if is_test else "strava-weather"
                payload = {
                    "activity_id": activity_id
                }

                lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType="RequestResponse",  # Asynchronous invocation
                    Payload=json.dumps(payload)
                )
                log.info("Update direction")
                function_name = "strava-direction-test" if is_test else "strava-direction"
                payload = {
                    "activity_id": activity_id
                }

                lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType="Event",  # Asynchronous invocation
                    Payload=json.dumps(payload)
                )
            log.info("Update efforts")
            function_name = "strava-efforts-test" if is_test else "efforts-gear"
            payload = {
                "activity_id": activity_id
            }

            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event",  # Asynchronous invocation
                Payload=json.dumps(payload)
            )
        if len(all_activities) > 0:
            log.info("Update gear")
            function_name = "strava-gear-test" if is_test else "strava-gear"

            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event"
            )
            log.info("Update stats")
            function_name = "strava-stats-test" if is_test else "strava-stats"


            lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="Event"  # Asynchronous invocation

            )


    except Exception as e:
        log.error('Something went wrong')
        log.error(e)
        # payload = {
        #     "to": os.getenv('MAIL_CONTACT'),
        #     "subject": "Strava sync went wrong",
        #     "content": str(e)
        # }
        # lambda_client.invoke(
        #     FunctionName='sendMail',  # Replace with the name of your sendMail function
        #     InvocationType='Event',  # Use 'RequestResponse' for synchronous invocation
        #     Payload=json.dumps(payload)
        # )
        exit()

    db.close()

