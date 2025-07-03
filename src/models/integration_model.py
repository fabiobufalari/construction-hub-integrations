"""
SQLAlchemy models for Integration Service
"""

from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import UUID
import uuid

# Import db from app
from app import db

class IntegrationLogModel(db.Model):
    """
    SQLAlchemy model for integration logs
    """
    __tablename__ = 'integration_logs'
    
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connector_name = db.Column(db.String(100), nullable=False)
    operation = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), nullable=False)  # success, error, warning
    details = db.Column(db.Text)
    request_data = db.Column(db.JSON)
    response_data = db.Column(db.JSON)
    error_message = db.Column(db.Text)
    execution_time_ms = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'connector_name': self.connector_name,
            'operation': self.operation,
            'status': self.status,
            'details': self.details,
            'request_data': self.request_data,
            'response_data': self.response_data,
            'error_message': self.error_message,
            'execution_time_ms': self.execution_time_ms,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ConnectorConfigModel(db.Model):
    """
    SQLAlchemy model for connector configurations
    """
    __tablename__ = 'connector_configs'
    
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connector_name = db.Column(db.String(100), nullable=False, unique=True)
    connector_type = db.Column(db.String(50), nullable=False)
    config_data = db.Column(db.JSON, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'connector_name': self.connector_name,
            'connector_type': self.connector_type,
            'config_data': self.config_data,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class IntegrationJobModel(db.Model):
    """
    SQLAlchemy model for integration jobs
    """
    __tablename__ = 'integration_jobs'
    
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name = db.Column(db.String(200), nullable=False)
    connector_name = db.Column(db.String(100), nullable=False)
    job_type = db.Column(db.String(50), nullable=False)  # sync, send, scheduled
    schedule_expression = db.Column(db.String(100))  # cron expression
    job_config = db.Column(db.JSON)
    status = db.Column(db.String(20), default='active')  # active, paused, disabled
    last_run_at = db.Column(db.DateTime)
    next_run_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': str(self.id),
            'job_name': self.job_name,
            'connector_name': self.connector_name,
            'job_type': self.job_type,
            'schedule_expression': self.schedule_expression,
            'job_config': self.job_config,
            'status': self.status,
            'last_run_at': self.last_run_at.isoformat() if self.last_run_at else None,
            'next_run_at': self.next_run_at.isoformat() if self.next_run_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

