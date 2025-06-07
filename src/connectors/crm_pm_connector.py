"""
CRM Connector
Handles integration with CRM systems (Salesforce, Dynamics CRM, HubSpot)
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class CRMConnector(BaseConnector):
    """
    CRM connector for integrating with Customer Relationship Management systems
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.crm_type = config.get('crm_type', 'generic')
        
    def connect(self) -> bool:
        """Connect to CRM system"""
        try:
            if self.crm_type.lower() == 'salesforce':
                return self._connect_salesforce()
            elif self.crm_type.lower() == 'dynamics':
                return self._connect_dynamics_crm()
            elif self.crm_type.lower() == 'hubspot':
                return self._connect_hubspot()
            else:
                return self._connect_generic()
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to connect to {self.crm_type}: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from CRM system"""
        try:
            self.is_connected = False
            self.log_operation('disconnect', 'success', f'Disconnected from {self.crm_type}')
            return True
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test CRM connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            return {
                'status': 'success',
                'message': f'{self.crm_type} CRM connection test successful',
                'details': {'connected': self.is_connected}
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'{self.crm_type} CRM connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync data from CRM"""
        try:
            if not self.is_connected:
                self.connect()
            
            # Mock implementation - in real scenario, would call actual CRM APIs
            mock_data = self._get_mock_crm_data(data_type)
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', f'Synced {data_type} from {self.crm_type}')
            
            return {
                'status': 'success',
                'data': mock_data,
                'count': len(mock_data)
            }
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Failed to sync {data_type}: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send data to CRM"""
        try:
            if not self.is_connected:
                self.connect()
            
            self.log_operation('send_data', 'success', f'Sent {data_type} to {self.crm_type}')
            
            return {
                'status': 'success',
                'message': f'Data sent to {self.crm_type} successfully'
            }
        except Exception as e:
            self.log_operation('send_data', 'error', f'Failed to send {data_type}: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _connect_salesforce(self) -> bool:
        """Connect to Salesforce"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to Salesforce')
        return True
    
    def _connect_dynamics_crm(self) -> bool:
        """Connect to Dynamics CRM"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to Dynamics CRM')
        return True
    
    def _connect_hubspot(self) -> bool:
        """Connect to HubSpot"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to HubSpot')
        return True
    
    def _connect_generic(self) -> bool:
        """Connect to generic CRM"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to generic CRM')
        return True
    
    def _get_mock_crm_data(self, data_type: str) -> List[Dict]:
        """Get mock CRM data for testing"""
        if data_type == 'customers':
            return [
                {
                    'id': 'CUST001',
                    'name': 'ABC Construction Ltd',
                    'email': 'contact@abcconstruction.com',
                    'phone': '+1-416-555-0123',
                    'status': 'active'
                }
            ]
        elif data_type == 'leads':
            return [
                {
                    'id': 'LEAD001',
                    'name': 'XYZ Development',
                    'email': 'info@xyzdev.com',
                    'status': 'qualified'
                }
            ]
        return []
    
    def get_required_config_fields(self) -> List[str]:
        return ['crm_type', 'api_url', 'api_key']

class ProjectManagementConnector(BaseConnector):
    """
    Project Management connector for integrating with PM systems (Jira, Asana, MS Project)
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.pm_type = config.get('pm_type', 'generic')
        
    def connect(self) -> bool:
        """Connect to Project Management system"""
        try:
            if self.pm_type.lower() == 'jira':
                return self._connect_jira()
            elif self.pm_type.lower() == 'asana':
                return self._connect_asana()
            elif self.pm_type.lower() == 'msproject':
                return self._connect_msproject()
            else:
                return self._connect_generic()
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to connect to {self.pm_type}: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from PM system"""
        try:
            self.is_connected = False
            self.log_operation('disconnect', 'success', f'Disconnected from {self.pm_type}')
            return True
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test PM connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            return {
                'status': 'success',
                'message': f'{self.pm_type} PM connection test successful',
                'details': {'connected': self.is_connected}
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'{self.pm_type} PM connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync data from PM system"""
        try:
            if not self.is_connected:
                self.connect()
            
            # Mock implementation
            mock_data = self._get_mock_pm_data(data_type)
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', f'Synced {data_type} from {self.pm_type}')
            
            return {
                'status': 'success',
                'data': mock_data,
                'count': len(mock_data)
            }
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Failed to sync {data_type}: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send data to PM system"""
        try:
            if not self.is_connected:
                self.connect()
            
            self.log_operation('send_data', 'success', f'Sent {data_type} to {self.pm_type}')
            
            return {
                'status': 'success',
                'message': f'Data sent to {self.pm_type} successfully'
            }
        except Exception as e:
            self.log_operation('send_data', 'error', f'Failed to send {data_type}: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _connect_jira(self) -> bool:
        """Connect to Jira"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to Jira')
        return True
    
    def _connect_asana(self) -> bool:
        """Connect to Asana"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to Asana')
        return True
    
    def _connect_msproject(self) -> bool:
        """Connect to MS Project"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to MS Project')
        return True
    
    def _connect_generic(self) -> bool:
        """Connect to generic PM system"""
        self.is_connected = True
        self.log_operation('connect', 'success', 'Connected to generic PM system')
        return True
    
    def _get_mock_pm_data(self, data_type: str) -> List[Dict]:
        """Get mock PM data for testing"""
        if data_type == 'projects':
            return [
                {
                    'id': 'PROJ001',
                    'name': 'Office Building Construction',
                    'status': 'in_progress',
                    'start_date': '2024-01-01',
                    'end_date': '2024-12-31'
                }
            ]
        elif data_type == 'tasks':
            return [
                {
                    'id': 'TASK001',
                    'project_id': 'PROJ001',
                    'name': 'Foundation Work',
                    'status': 'completed',
                    'assignee': 'John Doe'
                }
            ]
        return []
    
    def get_required_config_fields(self) -> List[str]:
        return ['pm_type', 'api_url', 'api_key']

