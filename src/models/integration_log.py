"""
Integration Log Model
Tracks integration operations and their results
"""

from datetime import datetime
from typing import Optional

class IntegrationLog:
    """
    Model for logging integration operations
    """
    
    def __init__(self, 
                 connector_name: str,
                 operation: str,
                 status: str,
                 details: Optional[str] = None,
                 timestamp: Optional[datetime] = None):
        """
        Initialize integration log entry
        
        Args:
            connector_name: Name of the connector
            operation: Operation performed
            status: Status of the operation (success, error, warning)
            details: Optional additional details
            timestamp: Timestamp of the operation
        """
        self.connector_name = connector_name
        self.operation = operation
        self.status = status
        self.details = details
        self.timestamp = timestamp or datetime.utcnow()
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'connector_name': self.connector_name,
            'operation': self.operation,
            'status': self.status,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }
    
    def __str__(self) -> str:
        return f"[{self.timestamp}] {self.connector_name}.{self.operation}: {self.status}"

