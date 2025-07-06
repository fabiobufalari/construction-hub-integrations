"""
Integration model using existing SQLAlchemy configuration
"""

from app import db
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
import uuid

class Integration(db.Model):
    """
    Integration model for managing external system integrations
    """
    __tablename__ = 'integrations'
    
    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    integration_type = db.Column(db.String(50), nullable=False)  # 'bank', 'payment', 'accounting', etc.
    endpoint_url = db.Column(db.String(255))
    api_key = db.Column(db.String(255))
    is_active = db.Column(db.Boolean, default=True)
    last_sync = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model to dictionary"""
        return {
            'id': str(self.id),
            'name': self.name,
            'description': self.description,
            'integration_type': self.integration_type,
            'endpoint_url': self.endpoint_url,
            'is_active': self.is_active,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create model from dictionary"""
        return cls(
            name=data.get('name'),
            description=data.get('description'),
            integration_type=data.get('integration_type'),
            endpoint_url=data.get('endpoint_url'),
            api_key=data.get('api_key'),
            is_active=data.get('is_active', True)
        )
    
    def __repr__(self):
        return f'<Integration {self.name}>'

