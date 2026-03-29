from fastapi import FastAPI, UploadFile, File, Body, Query, Depends, HTTPException
from typing import List, Optional
from sqlalchemy.orm import Session
from app.db.base import Base
from app.db.session import engine, SessionLocal
from app.models.document import Document
from app.models.job import Job
from app.schemas import FinalizeRequest
from app.celery_worker import process_document
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import redis
import csv
import json
from io import StringIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.Redis(host="localhost", port=6379, db=0)

Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return {"message": "Backend running 🚀"}

@app.post("/upload")
async def upload_files(files: List[UploadFile] = File(...), db: Session = Depends(get_db)):
    results = []

    for file in files:
        doc = Document(
            filename=file.filename,
            file_type=file.content_type,
            file_size=0,
            status="queued"
        )
        db.add(doc)
        db.commit()
        db.refresh(doc)

        job = Job(
            document_id=doc.id,
            status="queued",
            progress=0
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        process_document.delay(job.id)

        results.append({
            "document_id": doc.id,
            "job_id": job.id
        })

    return {
        "message": "Upload successful",
        "documents": results
    }

@app.get("/progress/{job_id}")
def stream_progress(job_id: str):
    pubsub = redis_client.pubsub()
    pubsub.subscribe("job_progress") 

    def event_stream():
        try:
            for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"].decode())

                    if data["job_id"] == job_id:
                        yield f"data: {json.dumps(data)}\n\n"
        finally:
            pubsub.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/documents")
def list_documents(
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(Document)

    if status:
        query = query.filter(Document.status == status)

    if search:
        query = query.filter(Document.filename.ilike(f"%{search}%"))

    docs = query.all()

    result = []
    for doc in docs:
        job = db.query(Job).filter(Job.document_id == doc.id).first()

        result.append({
            "document_id": doc.id,
            "filename": doc.filename,
            "status": doc.status,
            "job_status": job.status if job else None,
            "progress": job.progress if job else 0,
            "job_id": job.id if job else None
        })

    return result


@app.get("/documents/{document_id}")
def get_document(document_id: str, db: Session = Depends(get_db)):
    document = db.query(Document).filter(Document.id == document_id).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    job = db.query(Job).filter(Job.document_id == document_id).first()

    return {
        "document_id": document.id,
        "filename": document.filename,
        "status": document.status,
        "job_id": job.id if job else None,
        "progress": job.progress if job else 0,
        "job_status": job.status if job else None,
        "result": job.result if job and job.result else None
    }

@app.post("/retry/{job_id}")
def retry_job(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be retried")

    job.status = "queued"
    job.progress = 0
    db.commit()

    process_document.delay(job.id)

    return {"message": "Job retried"}


@app.put("/finalize/{job_id}")
def finalize_job(job_id: str, data: FinalizeRequest = Body(...), db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    job.result = data.model_dump()
    job.status = "finalized"
    db.commit()

    return {"message": "Job finalized"}

@app.get("/export/json/{job_id}")
def export_json(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job or not job.result:
        raise HTTPException(status_code=404, detail="No data available")

    return job.result


@app.get("/export/csv/{job_id}")
def export_csv(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job or not job.result:
        raise HTTPException(status_code=404, detail="No data available")

    output = StringIO()
    writer = csv.writer(output)

    writer.writerow(job.result.keys())
    writer.writerow(job.result.values())

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=job_{job_id}.csv"
        }
    )