from sqlalchemy import Column, String, Integer
from app.db.base import Base
import uuid

class Document(Base):
    __tablename__ = "documents"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    filename = Column(String)
    file_type = Column(String)
    file_size = Column(Integer)
    status = Column(String)
