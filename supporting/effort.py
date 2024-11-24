import logging
import os
import json
from database.db import Connection
from supporting import aws
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def float_to_time_string(seconds):
    # Convert float to total seconds
    total_seconds = int(seconds)

    # Calculate hours, minutes, and seconds
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # Format the time string based on the value of hours
    if hours > 0:
        return f"{hours:02}:{minutes:02}:{seconds:02}"  # hh:mm:ss format
    else:
        return f"{minutes:02}:{seconds:02}"  # mm:ss format


def find_fastest_and_slowest_segment_optimized(time_values, distance_values, target_distance):
    n = len(time_values)
    min_time_value = float('inf')
    max_time_value = float('-inf')
    fastest_segment_start = None
    slowest_segment_start = None

    j = 0  # Initialize the second pointer

    for i in range(n):
        # Move the second pointer to find a valid segment
        while j < n and float(distance_values[j]) - float(distance_values[i]) < target_distance:
            j += 1

        # Check if we've found a valid segment
        if j < n:
            time_taken = float(time_values[j]) - float(time_values[i])
            if time_taken < min_time_value:
                min_time_value = time_taken
                fastest_segment_start = i
            if time_taken > max_time_value:
                max_time_value = time_taken
                slowest_segment_start = i

    return min_time_value, max_time_value, distance_values[fastest_segment_start], distance_values[slowest_segment_start]


def execute(db, activity_id):
    activities = db.get_specific(table='activity', where=f'id = {activity_id}', order_by_type='desc')
    effort_list = [100, 200, 400, 800, 1000, 1500, 3000, 5000, 10000, 15000, 20000, 21097, 30000, 42195]
    act_counter = 0
    total_act = len(activities)
    for activity in activities:
        activity_id = activity[0]
        efforts = {x: {"best": "", "best_unix": 0, "best_start": 0, "worst": "", "worst_unix": 0, "worst_start": 0} for x in effort_list}
        act_counter += 1
        log.info(f'Handling activity {activity_id} ({act_counter}/{total_act})')
        activity_streams = db.get_specific(table="activity_streams", where=f"activity_id = {activity_id}")[0]
        if activity_streams is None:
            continue
        if activity_streams[2] is None:
            continue
        if activity_streams[3] is None:
            continue
        times = activity_streams[2].split(',')
        distances = activity_streams[3].split(',')

        for target in efforts:
            if float(distances[len(distances)-1])-float(distances[0]) >= float(target):
                min_time, max_time, fastest_start, slowest_start = find_fastest_and_slowest_segment_optimized(times, distances, target)
                efforts[target]["best_unix"] = min_time
                efforts[target]["best"] = float_to_time_string(min_time)
                efforts[target]["worst_unix"] = max_time
                efforts[target]["worst"] = float_to_time_string(max_time)
                efforts[target]["best_start"] = fastest_start
                efforts[target]["worst_start"] = slowest_start

        data_input = {
            "activity_id": activity_id,
            "effort": json.dumps(efforts)
        }
        db.insert(table='activity_effort', json_data=data_input)
