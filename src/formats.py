from enum import Enum
from typing import List

from pydantic import BaseModel


class DocType(Enum):
    EARNINGS_CALL = "earnings_call"
    FILING_8K = "filing_8k"
    FILING_10K = "filing_10k"
    FILING_DEF14A = "filing_def14a"
    FILING_10Q = "filing_10q"


class QuarterOutput(BaseModel):
    quarter: str


class ExtractedOutput(BaseModel):
    titles: List[str]
    values: List[str]
    units: List[str]


class ClassificationOutput(BaseModel):
    type_: str
    period: str
    unit: str
    category: str
    title: str


class MetricOutput(BaseModel):
    title: str
    unit: str
    type_: str
    category: str


class MetricListOutput(BaseModel):
    data: List[MetricOutput]


class CellOutput(BaseModel):
    value: str
    period: str


class CellListOutput(BaseModel):
    data: List[CellOutput]


class TableCellOutput(BaseModel):
    title: str
    value: str
    unit: str
    period: str
    type_: str
    category: str


class TableDataOutput(BaseModel):
    data: List[TableCellOutput]
