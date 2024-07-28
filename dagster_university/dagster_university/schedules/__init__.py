from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from ..jobs import trip_update_job, weekly_update_job

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *", # every 5th of the month at midnight
)

weekly_update_schedule = ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="0 0 * * 1", # every Monday at midnight
    # cron_schedule="* * * * *", # every minute
)