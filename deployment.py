from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from update_medanta_reports import medanta_flow


# Build the deployment for hourly run
deployment = Deployment.build_from_flow(
    flow=medanta_flow,
    name="medanta_gsheet_update_hourly",
    work_queue_name="medanta_queue",
    schedules=[CronSchedule(cron="0 * * * *", timezone="Asia/Kolkata")],  # every hour
    tags=["gsheet", "medanta"],
    version=1
)

if __name__ == "__main__":
    deployment.apply()
