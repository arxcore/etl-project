from pydantic import BaseModel
from typing import Any


class BaseFetcherReturn(BaseModel):
    api_type: str
    fetch_result: Any


class BaseParseReturn(BaseModel):
    parse_result: dict[str, float]
