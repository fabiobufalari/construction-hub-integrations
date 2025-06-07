"""
Custom Connector
Handles custom integrations and plugin-based connectors
"""

from typing import Dict, Any, List, Optional, Callable
import json
import logging
import importlib
import inspect
from datetime import datetime
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class CustomConnector(BaseConnector):
    """
    Custom connector for plugin-based integrations
    Allows users to define their own integration logic
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.plugin_module = None
        self.plugin_instance = None
        self.custom_handlers = {}
        
    def connect(self) -> bool:
        """Load and initialize custom plugin"""
        try:
            plugin_path = self.config.get('plugin_path')
            plugin_class = self.config.get('plugin_class')
            
            if not plugin_path or not plugin_class:
                self.log_operation('connect', 'error', 'Plugin path and class are required')
                return False
            
            # Load plugin module
            spec = importlib.util.spec_from_file_location("custom_plugin", plugin_path)
            self.plugin_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.plugin_module)
            
            # Get plugin class
            plugin_cls = getattr(self.plugin_module, plugin_class)
            
            # Validate plugin class
            if not self._validate_plugin_class(plugin_cls):
                self.log_operation('connect', 'error', 'Invalid plugin class structure')
                return False
            
            # Initialize plugin
            plugin_config = self.config.get('plugin_config', {})
            self.plugin_instance = plugin_cls(plugin_config)
            
            # Initialize plugin if it has an init method
            if hasattr(self.plugin_instance, 'initialize'):
                init_result = self.plugin_instance.initialize()
                if not init_result:
                    self.log_operation('connect', 'error', 'Plugin initialization failed')
                    return False
            
            self.is_connected = True
            self.log_operation('connect', 'success', f'Custom plugin {plugin_class} loaded successfully')
            return True
            
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to load custom plugin: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Cleanup custom plugin"""
        try:
            if self.plugin_instance and hasattr(self.plugin_instance, 'cleanup'):
                self.plugin_instance.cleanup()
            
            self.plugin_instance = None
            self.plugin_module = None
            self.is_connected = False
            
            self.log_operation('disconnect', 'success', 'Custom plugin disconnected')
            return True
            
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect custom plugin: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test custom plugin connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            if not self.plugin_instance:
                return {
                    'status': 'error',
                    'message': 'Plugin not loaded',
                    'details': {'connected': False}
                }
            
            # Test plugin if it has a test method
            if hasattr(self.plugin_instance, 'test_connection'):
                test_result = self.plugin_instance.test_connection()
                return {
                    'status': 'success' if test_result else 'error',
                    'message': 'Custom plugin connection test completed',
                    'details': {
                        'connected': self.is_connected,
                        'test_result': test_result
                    }
                }
            
            return {
                'status': 'success',
                'message': 'Custom plugin loaded (no test method available)',
                'details': {'connected': self.is_connected}
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Custom plugin connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync data using custom plugin"""
        try:
            if not self.is_connected or not self.plugin_instance:
                self.connect()
            
            if not hasattr(self.plugin_instance, 'sync_data'):
                return {
                    'status': 'error',
                    'message': 'Plugin does not implement sync_data method',
                    'data': []
                }
            
            # Call plugin sync method
            result = self.plugin_instance.sync_data(data_type, filters)
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', f'Custom plugin synced data type: {data_type}')
            
            return {
                'status': 'success',
                'data': result.get('data', []) if isinstance(result, dict) else result,
                'count': len(result.get('data', [])) if isinstance(result, dict) and 'data' in result else 0,
                'plugin_result': result
            }
            
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Custom plugin sync failed: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send data using custom plugin"""
        try:
            if not self.is_connected or not self.plugin_instance:
                self.connect()
            
            if not hasattr(self.plugin_instance, 'send_data'):
                return {
                    'status': 'error',
                    'message': 'Plugin does not implement send_data method'
                }
            
            # Call plugin send method
            result = self.plugin_instance.send_data(data, data_type)
            
            self.log_operation('send_data', 'success', f'Custom plugin sent data type: {data_type}')
            
            return {
                'status': 'success',
                'message': 'Data sent via custom plugin',
                'plugin_result': result
            }
            
        except Exception as e:
            self.log_operation('send_data', 'error', f'Custom plugin send failed: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def execute_custom_method(self, method_name: str, *args, **kwargs) -> Dict[str, Any]:
        """Execute a custom method on the plugin"""
        try:
            if not self.is_connected or not self.plugin_instance:
                self.connect()
            
            if not hasattr(self.plugin_instance, method_name):
                return {
                    'status': 'error',
                    'message': f'Plugin does not implement method: {method_name}'
                }
            
            method = getattr(self.plugin_instance, method_name)
            result = method(*args, **kwargs)
            
            self.log_operation('execute_custom_method', 'success', f'Executed custom method: {method_name}')
            
            return {
                'status': 'success',
                'message': f'Custom method {method_name} executed successfully',
                'result': result
            }
            
        except Exception as e:
            self.log_operation('execute_custom_method', 'error', f'Custom method execution failed: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def register_custom_handler(self, event_type: str, handler_func: Callable):
        """Register a custom event handler"""
        self.custom_handlers[event_type] = handler_func
        self.log_operation('register_handler', 'success', f'Registered handler for event: {event_type}')
    
    def trigger_custom_event(self, event_type: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger a custom event"""
        try:
            if event_type not in self.custom_handlers:
                return {
                    'status': 'error',
                    'message': f'No handler registered for event: {event_type}'
                }
            
            handler = self.custom_handlers[event_type]
            result = handler(event_data)
            
            self.log_operation('trigger_event', 'success', f'Triggered event: {event_type}')
            
            return {
                'status': 'success',
                'message': f'Event {event_type} triggered successfully',
                'result': result
            }
            
        except Exception as e:
            self.log_operation('trigger_event', 'error', f'Event trigger failed: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_plugin_info(self) -> Dict[str, Any]:
        """Get information about the loaded plugin"""
        if not self.plugin_instance:
            return {'status': 'error', 'message': 'No plugin loaded'}
        
        plugin_info = {
            'class_name': self.plugin_instance.__class__.__name__,
            'module_name': self.plugin_instance.__class__.__module__,
            'methods': []
        }
        
        # Get available methods
        for name, method in inspect.getmembers(self.plugin_instance, predicate=inspect.ismethod):
            if not name.startswith('_'):
                plugin_info['methods'].append({
                    'name': name,
                    'signature': str(inspect.signature(method))
                })
        
        # Get plugin metadata if available
        if hasattr(self.plugin_instance, 'get_metadata'):
            plugin_info['metadata'] = self.plugin_instance.get_metadata()
        
        return plugin_info
    
    def _validate_plugin_class(self, plugin_cls) -> bool:
        """Validate that the plugin class has required structure"""
        try:
            # Check if it's a class
            if not inspect.isclass(plugin_cls):
                return False
            
            # Check for required methods (at least one of sync_data or send_data)
            has_sync = hasattr(plugin_cls, 'sync_data')
            has_send = hasattr(plugin_cls, 'send_data')
            
            if not (has_sync or has_send):
                logger.warning("Plugin class should implement at least sync_data or send_data method")
            
            return True
            
        except Exception as e:
            logger.error(f"Plugin validation failed: {str(e)}")
            return False
    
    def get_required_config_fields(self) -> List[str]:
        return ['plugin_path', 'plugin_class']

class PluginTemplate:
    """
    Template class for custom plugins
    Users can inherit from this class to create their own plugins
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize plugin with configuration
        
        Args:
            config: Plugin-specific configuration
        """
        self.config = config
    
    def initialize(self) -> bool:
        """
        Initialize the plugin
        
        Returns:
            bool: True if initialization successful
        """
        return True
    
    def cleanup(self):
        """Cleanup plugin resources"""
        pass
    
    def test_connection(self) -> bool:
        """
        Test the plugin connection/functionality
        
        Returns:
            bool: True if test successful
        """
        return True
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Sync data from external system
        
        Args:
            data_type: Type of data to sync
            filters: Optional filters
            
        Returns:
            Dict containing sync results
        """
        raise NotImplementedError("sync_data method must be implemented")
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """
        Send data to external system
        
        Args:
            data: Data to send
            data_type: Type of data
            
        Returns:
            Dict containing send results
        """
        raise NotImplementedError("send_data method must be implemented")
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get plugin metadata
        
        Returns:
            Dict containing plugin metadata
        """
        return {
            'name': self.__class__.__name__,
            'version': '1.0.0',
            'description': 'Custom plugin',
            'author': 'Unknown'
        }

# Example custom plugin implementation
class ExampleCustomPlugin(PluginTemplate):
    """
    Example implementation of a custom plugin
    This demonstrates how to create a custom connector
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_url = config.get('api_url')
        self.api_key = config.get('api_key')
        self.session = None
    
    def initialize(self) -> bool:
        """Initialize HTTP session"""
        try:
            import requests
            self.session = requests.Session()
            if self.api_key:
                self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
            return True
        except Exception:
            return False
    
    def cleanup(self):
        """Close HTTP session"""
        if self.session:
            self.session.close()
    
    def test_connection(self) -> bool:
        """Test API connection"""
        try:
            if not self.session or not self.api_url:
                return False
            
            response = self.session.get(f"{self.api_url}/health")
            return response.status_code == 200
        except Exception:
            return False
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync data from API"""
        try:
            endpoint = f"{self.api_url}/{data_type}"
            params = filters or {}
            
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            
            return {
                'data': response.json(),
                'status_code': response.status_code
            }
        except Exception as e:
            return {
                'error': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send data to API"""
        try:
            endpoint = f"{self.api_url}/{data_type}"
            
            response = self.session.post(endpoint, json=data)
            response.raise_for_status()
            
            return {
                'success': True,
                'response': response.json(),
                'status_code': response.status_code
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get plugin metadata"""
        return {
            'name': 'ExampleCustomPlugin',
            'version': '1.0.0',
            'description': 'Example custom plugin for API integration',
            'author': 'Construction Hub Team',
            'supported_operations': ['sync_data', 'send_data'],
            'config_fields': ['api_url', 'api_key']
        }

