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


def execute(db, strava):
    log.info(f'Update gear')
    gear = db.get_all(table="gear", type="all")
    for item in gear:
        gear_id = item[0]
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
        db.update(table="gear", json_data=update_data, record_id=gear_id)
