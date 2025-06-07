"""
Unit tests for ERP Integration Module
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from src.integrations.erp_integration import ERPIntegrationModule
from src.connectors.base_connector import BaseConnector

class TestERPIntegrationModule:
    """Test ERP Integration Module"""
    
    @pytest.fixture
    def mock_connector(self):
        """Create mock connector"""
        connector = Mock(spec=BaseConnector)
        connector.config = {
            'erp_type': 'sap',
            'sap_company_code': '1000',
            'sap_client': '100'
        }
        connector.last_sync = datetime.utcnow()
        connector.get_status.return_value = {
            'name': 'MockConnector',
            'connected': True,
            'last_sync': datetime.utcnow().isoformat(),
            'config_valid': True
        }
        return connector
    
    @pytest.fixture
    def erp_module(self, mock_connector):
        """Create ERP integration module"""
        return ERPIntegrationModule(mock_connector)
    
    def test_init(self, erp_module, mock_connector):
        """Test ERP module initialization"""
        assert erp_module.connector == mock_connector
        assert erp_module.erp_type == 'sap'
        assert erp_module.module_name == 'ERP_SAP'
    
    def test_sync_financial_data_success(self, erp_module, mock_connector):
        """Test successful financial data sync"""
        # Mock connector response
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': [
                {
                    'BELNR': '1234567890',
                    'LIFNR': 'V001',
                    'NAME1': 'Test Vendor',
                    'XBLNR': 'INV-001',
                    'WRBTR': '1000.00',
                    'WAERS': 'CAD',
                    'ZFBDT': '2024-01-15',
                    'BUDAT': '2024-01-10',
                    'AUGBL': ''
                }
            ]
        }
        
        result = erp_module.sync_financial_data(['accounts_payable'])
        
        assert result['module'] == 'ERP_SAP'
        assert result['total_synced'] == 1
        assert 'accounts_payable' in result['results']
        assert result['results']['accounts_payable']['status'] == 'success'
        assert result['results']['accounts_payable']['count'] == 1
        
        # Check transformed data
        transformed_data = result['results']['accounts_payable']['data'][0]
        assert transformed_data['id'] == '1234567890'
        assert transformed_data['vendor_id'] == 'V001'
        assert transformed_data['amount'] == 1000.0
        assert transformed_data['status'] == 'open'
        assert transformed_data['erp_source'] == 'SAP'
    
    def test_sync_financial_data_error(self, erp_module, mock_connector):
        """Test financial data sync error"""
        mock_connector.sync_data.return_value = {
            'status': 'error',
            'message': 'Connection failed'
        }
        
        result = erp_module.sync_financial_data(['accounts_payable'])
        
        assert result['total_synced'] == 0
        assert result['results']['accounts_payable']['status'] == 'error'
        assert 'Connection failed' in result['results']['accounts_payable']['message']
    
    def test_send_financial_data(self, erp_module, mock_connector):
        """Test sending financial data to ERP"""
        mock_connector.send_data.return_value = {
            'status': 'success',
            'message': 'Data sent successfully'
        }
        
        data = [
            {
                'vendor_id': 'V001',
                'invoice_number': 'INV-001',
                'amount': 1000.00,
                'currency': 'CAD',
                'due_date': '2024-01-15',
                'posting_date': '2024-01-10'
            }
        ]
        
        result = erp_module.send_financial_data('accounts_payable', data)
        
        assert result['status'] == 'success'
        assert result['data_type'] == 'accounts_payable'
        assert result['records_sent'] == 1
        assert result['module'] == 'ERP_SAP'
    
    def test_map_sap_endpoint(self, erp_module):
        """Test SAP endpoint mapping"""
        assert erp_module._map_sap_endpoint('accounts_payable') == 'AP_INVOICE'
        assert erp_module._map_sap_endpoint('accounts_receivable') == 'AR_INVOICE'
        assert erp_module._map_sap_endpoint('general_ledger') == 'GL_ACCOUNT'
        assert erp_module._map_sap_endpoint('cost_centers') == 'COST_CENTER'
        assert erp_module._map_sap_endpoint('projects') == 'PROJECT_SYSTEM'
    
    def test_map_oracle_endpoint(self, erp_module):
        """Test Oracle endpoint mapping"""
        erp_module.erp_type = 'oracle'
        
        assert erp_module._map_oracle_endpoint('accounts_payable') == 'ap/invoices'
        assert erp_module._map_oracle_endpoint('accounts_receivable') == 'ar/invoices'
        assert erp_module._map_oracle_endpoint('general_ledger') == 'gl/journals'
    
    def test_map_dynamics_endpoint(self, erp_module):
        """Test Dynamics endpoint mapping"""
        erp_module.erp_type = 'dynamics'
        
        assert erp_module._map_dynamics_endpoint('accounts_payable') == 'vendorInvoices'
        assert erp_module._map_dynamics_endpoint('accounts_receivable') == 'customerInvoices'
        assert erp_module._map_dynamics_endpoint('general_ledger') == 'generalLedgerEntries'
    
    def test_transform_sap_data(self, erp_module):
        """Test SAP data transformation"""
        sap_data = [
            {
                'BELNR': '1234567890',
                'LIFNR': 'V001',
                'NAME1': 'Test Vendor',
                'XBLNR': 'INV-001',
                'WRBTR': '1000.00',
                'WAERS': 'CAD',
                'ZFBDT': '2024-01-15',
                'BUDAT': '2024-01-10',
                'AUGBL': ''
            }
        ]
        
        result = erp_module._transform_sap_data('accounts_payable', sap_data)
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == '1234567890'
        assert transformed['vendor_id'] == 'V001'
        assert transformed['vendor_name'] == 'Test Vendor'
        assert transformed['invoice_number'] == 'INV-001'
        assert transformed['amount'] == 1000.0
        assert transformed['currency'] == 'CAD'
        assert transformed['status'] == 'open'
        assert transformed['erp_source'] == 'SAP'
    
    def test_transform_oracle_data(self, erp_module):
        """Test Oracle data transformation"""
        oracle_data = [
            {
                'invoice_id': '12345',
                'vendor_id': 'V001',
                'vendor_name': 'Test Vendor',
                'invoice_num': 'INV-001',
                'invoice_amount': '1000.00',
                'invoice_currency_code': 'CAD',
                'terms_date': '2024-01-15',
                'invoice_date': '2024-01-10',
                'payment_status_flag': 'N'
            }
        ]
        
        result = erp_module._transform_oracle_data('accounts_payable', oracle_data)
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == '12345'
        assert transformed['vendor_id'] == 'V001'
        assert transformed['amount'] == 1000.0
        assert transformed['status'] == 'open'
        assert transformed['erp_source'] == 'Oracle'
    
    def test_transform_dynamics_data(self, erp_module):
        """Test Dynamics data transformation"""
        dynamics_data = [
            {
                'RecId': '12345',
                'VendAccount': 'V001',
                'VendorName': 'Test Vendor',
                'InvoiceNumber': 'INV-001',
                'InvoiceAmount': '1000.00',
                'CurrencyCode': 'CAD',
                'DueDate': '2024-01-15',
                'InvoiceDate': '2024-01-10',
                'InvoiceStatus': 'Open'
            }
        ]
        
        result = erp_module._transform_dynamics_data('accounts_payable', dynamics_data)
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == '12345'
        assert transformed['vendor_id'] == 'V001'
        assert transformed['amount'] == 1000.0
        assert transformed['status'] == 'Open'
        assert transformed['erp_source'] == 'Dynamics'
    
    def test_transform_to_sap_format(self, erp_module):
        """Test transformation to SAP format"""
        construction_hub_data = [
            {
                'vendor_id': 'V001',
                'invoice_number': 'INV-001',
                'amount': 1000.00,
                'currency': 'CAD',
                'due_date': '2024-01-15',
                'posting_date': '2024-01-10'
            }
        ]
        
        result = erp_module._transform_to_sap_format('accounts_payable', construction_hub_data)
        
        assert 'INVOICES' in result
        assert len(result['INVOICES']) == 1
        
        sap_record = result['INVOICES'][0]
        assert sap_record['LIFNR'] == 'V001'
        assert sap_record['XBLNR'] == 'INV-001'
        assert sap_record['WRBTR'] == 1000.00
        assert sap_record['WAERS'] == 'CAD'
    
    def test_get_integration_status(self, erp_module):
        """Test getting integration status"""
        status = erp_module.get_integration_status()
        
        assert status['module'] == 'ERP_SAP'
        assert status['erp_type'] == 'sap'
        assert 'connector_status' in status
        assert 'supported_data_types' in status
        assert 'accounts_payable' in status['supported_data_types']
        assert 'accounts_receivable' in status['supported_data_types']
        assert 'general_ledger' in status['supported_data_types']
        assert 'cost_centers' in status['supported_data_types']
        assert 'projects' in status['supported_data_types']
    
    def test_sync_accounts_payable(self, erp_module, mock_connector):
        """Test accounts payable sync shortcut method"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': []
        }
        
        result = erp_module.sync_accounts_payable({'date_from': '2024-01-01'})
        
        assert 'accounts_payable' in result['results']
        mock_connector.sync_data.assert_called_once()
    
    def test_sync_accounts_receivable(self, erp_module, mock_connector):
        """Test accounts receivable sync shortcut method"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': []
        }
        
        result = erp_module.sync_accounts_receivable()
        
        assert 'accounts_receivable' in result['results']
        mock_connector.sync_data.assert_called_once()

if __name__ == '__main__':
    pytest.main([__file__])

