from pydantic import BaseModel
from typing import Any, Literal


class BaseFetcherReturn(BaseModel):
    api_type: Literal["bls", "fred", "bea"]
    fetch_result: Any


class BaseParseReturn(BaseModel):
    parse_result: dict[str, float]
