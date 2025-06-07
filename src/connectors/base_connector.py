"""
Base Connector Class
Defines the interface for all integration connectors
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from src.models.integration_log import IntegrationLog

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """
    Abstract base class for all integration connectors
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connector with configuration
        
        Args:
            config: Dictionary containing connector configuration
        """
        self.config = config
        self.name = self.__class__.__name__
        self.is_connected = False
        self.last_sync = None
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the external system
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close connection to the external system
        
        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to the external system
        
        Returns:
            Dict containing connection test results
        """
        pass
    
    @abstractmethod
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Synchronize data from the external system
        
        Args:
            data_type: Type of data to synchronize
            filters: Optional filters to apply
            
        Returns:
            Dict containing sync results
        """
        pass
    
    @abstractmethod
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """
        Send data to the external system
        
        Args:
            data: Data to send
            data_type: Type of data being sent
            
        Returns:
            Dict containing send results
        """
        pass
    
    def log_operation(self, operation: str, status: str, details: Optional[str] = None):
        """
        Log integration operation
        
        Args:
            operation: Operation performed
            status: Status of the operation (success, error, warning)
            details: Optional additional details
        """
        try:
            log_entry = IntegrationLog(
                connector_name=self.name,
                operation=operation,
                status=status,
                details=details,
                timestamp=datetime.utcnow()
            )
            # In a real implementation, this would save to database
            logger.info(f"[{self.name}] {operation}: {status} - {details}")
        except Exception as e:
            logger.error(f"Failed to log operation: {str(e)}")
    
    def validate_config(self) -> List[str]:
        """
        Validate connector configuration
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if not self.config:
            errors.append("Configuration is required")
            return errors
            
        # Common validation rules
        required_fields = self.get_required_config_fields()
        for field in required_fields:
            if field not in self.config:
                errors.append(f"Required field '{field}' is missing")
            elif not self.config[field]:
                errors.append(f"Required field '{field}' cannot be empty")
                
        return errors
    
    @abstractmethod
    def get_required_config_fields(self) -> List[str]:
        """
        Get list of required configuration fields
        
        Returns:
            List of required field names
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get connector status information
        
        Returns:
            Dict containing status information
        """
        return {
            'name': self.name,
            'connected': self.is_connected,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'config_valid': len(self.validate_config()) == 0
        }

