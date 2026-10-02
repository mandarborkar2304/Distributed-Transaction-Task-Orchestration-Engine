import enum
import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    String,
    Enum,
    ForeignKey,
    Integer,
    Text,
    Index,
    text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP
from sqlalchemy.orm import relationship

from src.database import Base

class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"

def utc_now():
    return datetime.now(timezone.utc)

class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(String(128), unique=True, nullable=False)
    job_type = Column(String(64), nullable=False)
    status = Column(Enum(TaskStatus), nullable=False, default=TaskStatus.PENDING)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    tasks = relationship("JobTask", back_populates="job", cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (
        Index("ix_jobs_status_created_at", "status", "created_at"),
        Index("ix_jobs_idempotency_key", "idempotency_key", unique=True),
    )


class JobTask(Base):
    __tablename__ = "job_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    handler_name = Column(String(64), nullable=False)
    status = Column(Enum(TaskStatus), nullable=False, default=TaskStatus.PENDING)
    payload = Column(JSONB, nullable=False, default={})
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    locked_by = Column(String(64), nullable=True)
    heartbeat_at = Column(TIMESTAMP(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    job = relationship("Job", back_populates="tasks", lazy="selectin")

    __table_args__ = (
        Index("ix_job_tasks_status_created_at", "status", "created_at"),
        Index(
            "ix_job_tasks_pending_running",
            "status",
            "created_at",
            postgresql_where=text("status IN ('PENDING', 'RUNNING')")
        ),
        Index("ix_job_tasks_job_id", "job_id"),
        Index(
            "ix_job_tasks_heartbeat_at",
            "heartbeat_at",
            postgresql_where=text("heartbeat_at IS NOT NULL")
        ),
    )
