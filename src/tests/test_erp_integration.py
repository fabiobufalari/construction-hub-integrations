"""
Unit tests for ERP Integration Module
Updated for PostgreSQL compatibility (migrated from Oracle)
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from src.integrations.erp_integration import ERPIntegrationModule
from src.connectors.base_connector import BaseConnector

class TestERPIntegrationModule:
    """Test ERP Integration Module with PostgreSQL support"""
    
    @pytest.fixture
    def mock_connector(self):
        """Create mock connector / Criar conector mock"""
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
    def postgresql_connector(self):
        """Create PostgreSQL ERP mock connector / Criar conector mock PostgreSQL ERP"""
        connector = Mock(spec=BaseConnector)
        connector.config = {
            'erp_type': 'postgresql_erp',
            'postgresql_schema': 'erp_schema',
            'company_id': 'COMP001',
            'default_currency': 'CAD'
        }
        connector.last_sync = datetime.utcnow()
        connector.get_status.return_value = {
            'name': 'PostgreSQLERPConnector',
            'connected': True,
            'last_sync': datetime.utcnow().isoformat(),
            'config_valid': True,
            'database_type': 'PostgreSQL'
        }
        return connector
    
    @pytest.fixture
    def erp_module(self, mock_connector):
        """Create ERP integration module / Criar módulo de integração ERP"""
        return ERPIntegrationModule(mock_connector)
    
    @pytest.fixture
    def postgresql_erp_module(self, postgresql_connector):
        """Create PostgreSQL ERP integration module / Criar módulo de integração PostgreSQL ERP"""
        return ERPIntegrationModule(postgresql_connector)
    
    def test_init(self, erp_module, mock_connector):
        """Test ERP module initialization / Testar inicialização do módulo ERP"""
        assert erp_module.connector == mock_connector
        assert erp_module.erp_type == 'sap'
        assert erp_module.module_name == 'ERP_SAP'
    
    def test_postgresql_init(self, postgresql_erp_module, postgresql_connector):
        """Test PostgreSQL ERP module initialization / Testar inicialização do módulo PostgreSQL ERP"""
        assert postgresql_erp_module.connector == postgresql_connector
        assert postgresql_erp_module.erp_type == 'postgresql_erp'
        assert postgresql_erp_module.module_name == 'ERP_POSTGRESQL_ERP'
    
    def test_sync_financial_data_success(self, erp_module, mock_connector):
        """Test successful financial data sync / Testar sincronização bem-sucedida de dados financeiros"""
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
    
    def test_postgresql_sync_financial_data_success(self, postgresql_erp_module, postgresql_connector):
        """Test successful PostgreSQL ERP financial data sync / Testar sincronização bem-sucedida de dados financeiros PostgreSQL ERP"""
        # Mock PostgreSQL connector response
        postgresql_connector.sync_data.return_value = {
            'status': 'success',
            'data': [
                {
                    'invoice_id': '12345',
                    'vendor_id': 'V001',
                    'vendor_name': 'Test Vendor',
                    'invoice_number': 'INV-001',
                    'invoice_amount': '1000.00',
                    'currency_code': 'CAD',
                    'due_date': '2024-01-15',
                    'invoice_date': '2024-01-10',
                    'payment_status': 'OPEN'
                }
            ]
        }
        
        result = postgresql_erp_module.sync_financial_data(['accounts_payable'])
        
        assert result['module'] == 'ERP_POSTGRESQL_ERP'
        assert result['total_synced'] == 1
        assert 'accounts_payable' in result['results']
        assert result['results']['accounts_payable']['status'] == 'success'
        assert result['results']['accounts_payable']['count'] == 1
        
        # Check transformed data
        transformed_data = result['results']['accounts_payable']['data'][0]
        assert transformed_data['id'] == '12345'
        assert transformed_data['vendor_id'] == 'V001'
        assert transformed_data['amount'] == 1000.0
        assert transformed_data['status'] == 'open'
        assert transformed_data['erp_source'] == 'PostgreSQL_ERP'
    
    def test_sync_financial_data_error(self, erp_module, mock_connector):
        """Test financial data sync error / Testar erro na sincronização de dados financeiros"""
        mock_connector.sync_data.return_value = {
            'status': 'error',
            'message': 'Connection failed'
        }
        
        result = erp_module.sync_financial_data(['accounts_payable'])
        
        assert result['total_synced'] == 0
        assert result['results']['accounts_payable']['status'] == 'error'
        assert 'Connection failed' in result['results']['accounts_payable']['message']
    
    def test_send_financial_data(self, erp_module, mock_connector):
        """Test sending financial data to ERP / Testar envio de dados financeiros para ERP"""
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
        """Test SAP endpoint mapping / Testar mapeamento de endpoints SAP"""
        assert erp_module._map_sap_endpoint('accounts_payable') == 'AP_INVOICE'
        assert erp_module._map_sap_endpoint('accounts_receivable') == 'AR_INVOICE'
        assert erp_module._map_sap_endpoint('general_ledger') == 'GL_ACCOUNT'
        assert erp_module._map_sap_endpoint('cost_centers') == 'COST_CENTER'
        assert erp_module._map_sap_endpoint('projects') == 'PROJECT_SYSTEM'
    
    def test_map_postgresql_erp_endpoint(self, postgresql_erp_module):
        """Test PostgreSQL ERP endpoint mapping / Testar mapeamento de endpoints PostgreSQL ERP"""
        assert postgresql_erp_module._map_postgresql_erp_endpoint('accounts_payable') == 'api/v1/ap/invoices'
        assert postgresql_erp_module._map_postgresql_erp_endpoint('accounts_receivable') == 'api/v1/ar/invoices'
        assert postgresql_erp_module._map_postgresql_erp_endpoint('general_ledger') == 'api/v1/gl/journals'
        assert postgresql_erp_module._map_postgresql_erp_endpoint('cost_centers') == 'api/v1/gl/costcenters'
        assert postgresql_erp_module._map_postgresql_erp_endpoint('projects') == 'api/v1/pm/projects'
    
    def test_map_dynamics_endpoint(self, erp_module):
        """Test Dynamics endpoint mapping / Testar mapeamento de endpoints Dynamics"""
        erp_module.erp_type = 'dynamics'
        
        assert erp_module._map_dynamics_endpoint('accounts_payable') == 'vendorInvoices'
        assert erp_module._map_dynamics_endpoint('accounts_receivable') == 'customerInvoices'
        assert erp_module._map_dynamics_endpoint('general_ledger') == 'generalLedgerEntries'
    
    def test_transform_sap_data(self, erp_module):
        """Test SAP data transformation / Testar transformação de dados SAP"""
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
    
    def test_transform_postgresql_erp_data(self, postgresql_erp_module):
        """Test PostgreSQL ERP data transformation / Testar transformação de dados PostgreSQL ERP"""
        postgresql_data = [
            {
                'invoice_id': '12345',
                'vendor_id': 'V001',
                'vendor_name': 'Test Vendor',
                'invoice_number': 'INV-001',
                'invoice_amount': '1000.00',
                'currency_code': 'CAD',
                'due_date': '2024-01-15',
                'invoice_date': '2024-01-10',
                'payment_status': 'OPEN'
            }
        ]
        
        result = postgresql_erp_module._transform_postgresql_erp_data('accounts_payable', postgresql_data)
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == '12345'
        assert transformed['vendor_id'] == 'V001'
        assert transformed['amount'] == 1000.0
        assert transformed['status'] == 'open'
        assert transformed['erp_source'] == 'PostgreSQL_ERP'
    
    def test_transform_dynamics_data(self, erp_module):
        """Test Dynamics data transformation / Testar transformação de dados Dynamics"""
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
        """Test transformation to SAP format / Testar transformação para formato SAP"""
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
    
    def test_transform_to_postgresql_erp_format(self, postgresql_erp_module):
        """Test transformation to PostgreSQL ERP format / Testar transformação para formato PostgreSQL ERP"""
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
        
        result = postgresql_erp_module._transform_to_postgresql_erp_format('accounts_payable', construction_hub_data)
        
        assert 'invoices' in result
        assert len(result['invoices']) == 1
        
        postgresql_record = result['invoices'][0]
        assert postgresql_record['vendor_id'] == 'V001'
        assert postgresql_record['invoice_number'] == 'INV-001'
        assert postgresql_record['invoice_amount'] == 1000.00
        assert postgresql_record['currency_code'] == 'CAD'
        assert postgresql_record['created_by'] == 'construction_hub_system'
    
    def test_get_integration_status(self, erp_module):
        """Test getting integration status / Testar obtenção do status de integração"""
        status = erp_module.get_integration_status()
        
        assert status['module'] == 'ERP_SAP'
        assert status['erp_type'] == 'sap'
        assert status['database_type'] == 'PostgreSQL'  # Updated from Oracle
        assert 'connector_status' in status
        assert 'supported_data_types' in status
        assert 'accounts_payable' in status['supported_data_types']
        assert 'accounts_receivable' in status['supported_data_types']
        assert 'general_ledger' in status['supported_data_types']
        assert 'cost_centers' in status['supported_data_types']
        assert 'projects' in status['supported_data_types']
        
        # Check PostgreSQL compatibility flags
        assert status['configuration']['postgresql_compatible'] == True
        assert status['configuration']['oracle_compatible'] == False
    
    def test_postgresql_get_integration_status(self, postgresql_erp_module):
        """Test getting PostgreSQL ERP integration status / Testar obtenção do status de integração PostgreSQL ERP"""
        status = postgresql_erp_module.get_integration_status()
        
        assert status['module'] == 'ERP_POSTGRESQL_ERP'
        assert status['erp_type'] == 'postgresql_erp'
        assert status['database_type'] == 'PostgreSQL'
        assert status['configuration']['postgresql_compatible'] == True
        assert status['configuration']['oracle_compatible'] == False
    
    def test_sync_accounts_payable(self, erp_module, mock_connector):
        """Test accounts payable sync shortcut method / Testar método de atalho para sincronização de contas a pagar"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': []
        }
        
        result = erp_module.sync_accounts_payable({'date_from': '2024-01-01'})
        
        assert 'accounts_payable' in result['results']
        mock_connector.sync_data.assert_called_once()
    
    def test_sync_accounts_receivable(self, erp_module, mock_connector):
        """Test accounts receivable sync shortcut method / Testar método de atalho para sincronização de contas a receber"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': []
        }
        
        result = erp_module.sync_accounts_receivable()
        
        assert 'accounts_receivable' in result['results']
        mock_connector.sync_data.assert_called_once()
    
    def test_get_postgresql_erp_filters(self, postgresql_erp_module):
        """Test PostgreSQL ERP specific filters / Testar filtros específicos do PostgreSQL ERP"""
        filters = postgresql_erp_module._get_postgresql_erp_filters('accounts_payable')
        
        assert 'database_schema' in filters
        assert 'company_id' in filters
        assert 'fiscal_year' in filters
        assert 'currency' in filters
        assert filters['currency'] == 'CAD'

if __name__ == '__main__':
    pytest.main([__file__])

