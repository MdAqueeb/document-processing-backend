from sqlalchemy import Column, String, Integer, ForeignKey, JSON

from app.db.base import Base
import uuid

class Job(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    document_id = Column(String, ForeignKey("documents.id"))
    status = Column(String)
    progress = Column(Integer, default=0)
    result = Column(JSON, nullable=True)
