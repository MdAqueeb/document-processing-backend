from celery import Celery
from app.db.session import SessionLocal
from app.models.job import Job
import time
import redis
import json

celery = Celery(
    "worker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

redis_client = redis.Redis(host="localhost", port=6379, db=0)


@celery.task
def process_document(job_id: str):
    db = SessionLocal()

    try:
        job = db.query(Job).filter(Job.id == job_id).first()

        if not job:
            return

        job.status = "processing"
        db.commit()

        redis_client.publish("job_progress", json.dumps({"job_id": job_id, "progress": 0, "event": "job_started"}))

        for progress in range(0, 101, 20):
            time.sleep(1)

            job.progress = progress
            db.commit()

            redis_client.publish(
                "job_progress",
                json.dumps({
                    "job_id": job_id,
                    "progress": progress,
                    "event": "processing"
                })
            )

        job.status = "completed"
        redis_client.publish("job_progress", json.dumps({"job_id": job_id, "progress": 100, "event": "job_completed"}))
        job.result = {"message": "Done"}
        db.commit()

    except Exception as e:
        job.status = "failed"
        db.commit()
        redis_client.publish("job_progress", json.dumps({"job_id": job_id, "progress": 0, "event": "job_failed"}))

    finally:
        db.close()