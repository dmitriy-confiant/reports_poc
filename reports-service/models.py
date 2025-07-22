from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Any
from pydantic import BaseModel, Field
import uuid


class ReportType(str, Enum):
    PUBLISHER_BLOCKS = "publisher_blocks"
    ORDERS_BY_DAY = "orders_by_day"
    HEAVINESS_SCORE = "heaviness_score"
    # ...


class ReportStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReportRequest(BaseModel):
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    report_type: ReportType
    filters: Dict[str, Any] = Field(default_factory=dict)
    user_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    ttl_hours: int = Field(default=24, ge=1, le=168)  # 1 hour to 7 days


class ReportMetadata(BaseModel):
    report_id: str
    report_type: ReportType
    status: ReportStatus
    user_id: str
    progress: int
    message: str
    filters: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    ttl_hours: int
    expires_at: datetime
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    error: Optional[str] = None


class ReportGenerationMessage(BaseModel):
    report_id: str
    report_type: ReportType
    filters: Dict[str, Any]
    user_id: str
    ttl_hours: int
