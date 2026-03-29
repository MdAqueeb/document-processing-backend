from pydantic import BaseModel
from typing import List

class FinalizeRequest(BaseModel):
    title: str
    category: str
    summary: str
    keywords: List[str]
