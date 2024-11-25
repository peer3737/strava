import logging


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


def execute(db, strava, activity_id):
    log.info(f'Update gear')
    gear_id = db.get_specific(table='activity', where=f'id = {activity_id}', order_by_type='desc')[0][15]
    if gear_id is not None:
        strava_gear = strava.getgear(gear_id=gear_id)
        log.info(f'Update {strava_gear["name"]}')
        update_data = {
            "is_primary": strava_gear["primary"],
            "name": strava_gear["name"],
            "nickname": strava_gear["nickname"],
            "resource_state": strava_gear["resource_state"],
            "is_retired": strava_gear["retired"],
            "distance": strava_gear["distance"],
            "converted_distance": strava_gear["converted_distance"],
            "brand_name": strava_gear["brand_name"],
            "model_name": strava_gear["model_name"],
            "description": strava_gear["description"],
            "notification_distance": strava_gear["notification_distance"],
        }

        check_gear = db.get_specific(table='gear', where=f"id = '{gear_id}'", order_by_type='desc')
        if len(check_gear) == 0:
            update_data['id'] = gear_id
            db.insert(table="gear", json_data=update_data)

        else:
            db.update(table="gear", json_data=update_data, record_id=gear_id)
