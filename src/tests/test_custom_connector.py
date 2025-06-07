"""
Unit tests for Custom Connector
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from src.connectors.custom_connector import CustomConnector, PluginTemplate, ExampleCustomPlugin

class TestCustomConnector:
    """Test CustomConnector implementation"""
    
    @pytest.fixture
    def custom_config(self):
        return {
            'plugin_path': '/path/to/plugin.py',
            'plugin_class': 'TestPlugin',
            'plugin_config': {'api_url': 'http://test.com'}
        }
    
    @pytest.fixture
    def custom_connector(self, custom_config):
        return CustomConnector(custom_config)
    
    def test_init(self, custom_connector):
        """Test CustomConnector initialization"""
        assert custom_connector.plugin_module is None
        assert custom_connector.plugin_instance is None
        assert custom_connector.custom_handlers == {}
        assert not custom_connector.is_connected
    
    def test_get_required_config_fields(self, custom_connector):
        """Test required config fields"""
        required_fields = custom_connector.get_required_config_fields()
        assert 'plugin_path' in required_fields
        assert 'plugin_class' in required_fields
    
    def test_validate_config(self, custom_connector):
        """Test config validation"""
        errors = custom_connector.validate_config()
        assert len(errors) == 0  # Should be valid with the fixture config
        
        # Test invalid config
        custom_connector.config = {}
        errors = custom_connector.validate_config()
        assert len(errors) > 0
    
    def test_register_custom_handler(self, custom_connector):
        """Test custom handler registration"""
        def test_handler(data):
            return data
        
        custom_connector.register_custom_handler('test_event', test_handler)
        assert 'test_event' in custom_connector.custom_handlers
        assert custom_connector.custom_handlers['test_event'] == test_handler
    
    def test_trigger_custom_event(self, custom_connector):
        """Test custom event triggering"""
        def test_handler(data):
            return {'processed': data}
        
        custom_connector.register_custom_handler('test_event', test_handler)
        
        result = custom_connector.trigger_custom_event('test_event', {'test': 'data'})
        assert result['status'] == 'success'
        assert result['result'] == {'processed': {'test': 'data'}}
        
        # Test unregistered event
        result = custom_connector.trigger_custom_event('unknown_event', {})
        assert result['status'] == 'error'
    
    @patch('importlib.util.spec_from_file_location')
    @patch('importlib.util.module_from_spec')
    def test_connect_success(self, mock_module_from_spec, mock_spec_from_file, custom_connector):
        """Test successful plugin loading"""
        # Mock plugin class
        mock_plugin_class = Mock()
        mock_plugin_instance = Mock()
        mock_plugin_instance.initialize.return_value = True
        mock_plugin_class.return_value = mock_plugin_instance
        
        # Mock module
        mock_module = Mock()
        setattr(mock_module, 'TestPlugin', mock_plugin_class)
        mock_module_from_spec.return_value = mock_module
        
        # Mock spec
        mock_spec = Mock()
        mock_loader = Mock()
        mock_spec.loader = mock_loader
        mock_spec_from_file.return_value = mock_spec
        
        result = custom_connector.connect()
        
        assert result is True
        assert custom_connector.is_connected is True
        assert custom_connector.plugin_instance is not None
    
    def test_connect_failure_missing_config(self):
        """Test plugin loading failure due to missing config"""
        config = {'plugin_path': '/path/to/plugin.py'}  # Missing plugin_class
        connector = CustomConnector(config)
        
        result = connector.connect()
        
        assert result is False
        assert connector.is_connected is False
    
    def test_disconnect(self, custom_connector):
        """Test plugin disconnection"""
        mock_plugin = Mock()
        custom_connector.plugin_instance = mock_plugin
        custom_connector.is_connected = True
        
        result = custom_connector.disconnect()
        
        assert result is True
        assert custom_connector.is_connected is False
        assert custom_connector.plugin_instance is None
        mock_plugin.cleanup.assert_called_once()
    
    def test_execute_custom_method(self, custom_connector):
        """Test custom method execution"""
        mock_plugin = Mock()
        mock_plugin.custom_method.return_value = {'result': 'success'}
        custom_connector.plugin_instance = mock_plugin
        custom_connector.is_connected = True
        
        result = custom_connector.execute_custom_method('custom_method', 'arg1', key='value')
        
        assert result['status'] == 'success'
        assert result['result'] == {'result': 'success'}
        mock_plugin.custom_method.assert_called_once_with('arg1', key='value')
        
        # Test non-existent method
        result = custom_connector.execute_custom_method('non_existent_method')
        assert result['status'] == 'error'
    
    def test_get_plugin_info(self, custom_connector):
        """Test plugin info retrieval"""
        # Test without plugin
        result = custom_connector.get_plugin_info()
        assert result['status'] == 'error'
        
        # Test with plugin
        mock_plugin = Mock()
        mock_plugin.__class__.__name__ = 'TestPlugin'
        mock_plugin.__class__.__module__ = 'test_module'
        mock_plugin.get_metadata.return_value = {'version': '1.0.0'}
        custom_connector.plugin_instance = mock_plugin
        
        with patch('inspect.getmembers') as mock_getmembers:
            mock_getmembers.return_value = [('test_method', Mock())]
            with patch('inspect.signature') as mock_signature:
                mock_signature.return_value = 'test_signature'
                
                result = custom_connector.get_plugin_info()
                
                assert result['class_name'] == 'TestPlugin'
                assert result['module_name'] == 'test_module'
                assert result['metadata'] == {'version': '1.0.0'}

class TestPluginTemplate:
    """Test PluginTemplate base class"""
    
    def test_init(self):
        """Test PluginTemplate initialization"""
        config = {'test': 'config'}
        plugin = PluginTemplate(config)
        assert plugin.config == config
    
    def test_initialize(self):
        """Test default initialize method"""
        plugin = PluginTemplate({})
        assert plugin.initialize() is True
    
    def test_cleanup(self):
        """Test default cleanup method"""
        plugin = PluginTemplate({})
        plugin.cleanup()  # Should not raise exception
    
    def test_test_connection(self):
        """Test default test_connection method"""
        plugin = PluginTemplate({})
        assert plugin.test_connection() is True
    
    def test_sync_data_not_implemented(self):
        """Test sync_data raises NotImplementedError"""
        plugin = PluginTemplate({})
        with pytest.raises(NotImplementedError):
            plugin.sync_data('test_type')
    
    def test_send_data_not_implemented(self):
        """Test send_data raises NotImplementedError"""
        plugin = PluginTemplate({})
        with pytest.raises(NotImplementedError):
            plugin.send_data({}, 'test_type')
    
    def test_get_metadata(self):
        """Test default get_metadata method"""
        plugin = PluginTemplate({})
        metadata = plugin.get_metadata()
        assert 'name' in metadata
        assert 'version' in metadata
        assert 'description' in metadata
        assert 'author' in metadata

class TestExampleCustomPlugin:
    """Test ExampleCustomPlugin implementation"""
    
    @pytest.fixture
    def plugin_config(self):
        return {
            'api_url': 'http://test-api.com',
            'api_key': 'test-key'
        }
    
    @pytest.fixture
    def example_plugin(self, plugin_config):
        return ExampleCustomPlugin(plugin_config)
    
    def test_init(self, example_plugin):
        """Test ExampleCustomPlugin initialization"""
        assert example_plugin.api_url == 'http://test-api.com'
        assert example_plugin.api_key == 'test-key'
        assert example_plugin.session is None
    
    @patch('requests.Session')
    def test_initialize(self, mock_session, example_plugin):
        """Test plugin initialization"""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        
        result = example_plugin.initialize()
        
        assert result is True
        assert example_plugin.session is not None
        assert 'Authorization' in mock_session_instance.headers.update.call_args[0][0]
    
    def test_cleanup(self, example_plugin):
        """Test plugin cleanup"""
        mock_session = Mock()
        example_plugin.session = mock_session
        
        example_plugin.cleanup()
        
        mock_session.close.assert_called_once()
    
    @patch('requests.Session')
    def test_test_connection(self, mock_session, example_plugin):
        """Test connection testing"""
        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        example_plugin.initialize()
        result = example_plugin.test_connection()
        
        assert result is True
        mock_session_instance.get.assert_called_once_with('http://test-api.com/health')
    
    @patch('requests.Session')
    def test_sync_data(self, mock_session, example_plugin):
        """Test data synchronization"""
        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {'data': 'test'}
        mock_response.status_code = 200
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        example_plugin.initialize()
        result = example_plugin.sync_data('test_type', {'param': 'value'})
        
        assert 'data' in result
        assert result['status_code'] == 200
        mock_session_instance.get.assert_called_once_with(
            'http://test-api.com/test_type',
            params={'param': 'value'}
        )
    
    @patch('requests.Session')
    def test_send_data(self, mock_session, example_plugin):
        """Test data sending"""
        mock_session_instance = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {'success': True}
        mock_response.status_code = 201
        mock_session_instance.post.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        example_plugin.initialize()
        result = example_plugin.send_data({'test': 'data'}, 'test_type')
        
        assert result['success'] is True
        assert result['status_code'] == 201
        mock_session_instance.post.assert_called_once_with(
            'http://test-api.com/test_type',
            json={'test': 'data'}
        )
    
    def test_get_metadata(self, example_plugin):
        """Test metadata retrieval"""
        metadata = example_plugin.get_metadata()
        
        assert metadata['name'] == 'ExampleCustomPlugin'
        assert metadata['version'] == '1.0.0'
        assert 'supported_operations' in metadata
        assert 'config_fields' in metadata

if __name__ == '__main__':
    pytest.main([__file__])

