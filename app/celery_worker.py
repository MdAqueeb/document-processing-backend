# celery_app.py
from celery import Celery
import os
from dotenv import load_dotenv
import redis
import json
import time
from app.db.session import SessionLocal
from app.models.job import Job

load_dotenv()

REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

CELERY_REDIS_URL = f"rediss://default:{REDIS_TOKEN}@aware-dane-72089.upstash.io:6379"

# Initialize Celery
celery = Celery(
    "worker",
    broker=CELERY_REDIS_URL,
    backend=CELERY_REDIS_URL
)

redis_client = redis.Redis(
    host="aware-dane-72089.upstash.io",
    port=6379,
    password=REDIS_TOKEN,
    ssl=True,          # Must use TLS
    decode_responses=True
)

@celery.task
def process_document(job_id: str):
    db = SessionLocal()

    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return

        job.status = "processing"
        db.commit()

        redis_client.publish(
            "job_progress",
            json.dumps({"job_id": job_id, "progress": 0, "event": "job_started"})
        )

        for progress in range(0, 101, 20):
            time.sleep(1)
            job.progress = progress
            db.commit()

            redis_client.publish(
                "job_progress",
                json.dumps({"job_id": job_id, "progress": progress, "event": "processing"})
            )

        job.status = "completed"
        job.result = {"message": "Done"}
        db.commit()
        redis_client.publish(
            "job_progress",
            json.dumps({"job_id": job_id, "progress": 100, "event": "job_completed"})
        )

    except Exception as e:
        job.status = "failed"
        db.commit()
        redis_client.publish(
            "job_progress",
            json.dumps({"job_id": job_id, "progress": 0, "event": "job_failed"})
        )

    finally:
        db.close()