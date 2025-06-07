"""
Unit tests for Banking Integration Module
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from decimal import Decimal
from src.integrations.banking_integration import BankingIntegrationModule
from src.connectors.base_connector import BaseConnector

class TestBankingIntegrationModule:
    """Test Banking Integration Module"""
    
    @pytest.fixture
    def mock_connector(self):
        """Create mock connector"""
        connector = Mock(spec=BaseConnector)
        connector.config = {
            'bank_type': 'rbc',
            'payment_gateway': 'interac'
        }
        connector.last_sync = datetime.utcnow()
        connector.get_status.return_value = {
            'name': 'MockBankConnector',
            'connected': True,
            'last_sync': datetime.utcnow().isoformat(),
            'config_valid': True
        }
        return connector
    
    @pytest.fixture
    def banking_module(self, mock_connector):
        """Create banking integration module"""
        return BankingIntegrationModule(mock_connector)
    
    def test_init(self, banking_module, mock_connector):
        """Test banking module initialization"""
        assert banking_module.connector == mock_connector
        assert banking_module.bank_type == 'rbc'
        assert banking_module.payment_gateway == 'interac'
        assert banking_module.module_name == 'BANKING_RBC'
    
    def test_sync_bank_transactions_success(self, banking_module, mock_connector):
        """Test successful bank transaction sync"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': [
                {
                    'transaction_id': 'TXN001',
                    'account_number': '12345',
                    'transaction_date': '2024-01-10',
                    'description': 'Test Transaction',
                    'amount': '-100.00',
                    'currency': 'CAD',
                    'balance_after': '1900.00'
                }
            ]
        }
        
        result = banking_module.sync_bank_transactions(['12345'], '2024-01-01', '2024-01-31')
        
        assert result['module'] == 'BANKING_RBC'
        assert result['total_accounts_synced'] == 1
        assert '12345' in result['results']
        assert result['results']['12345']['status'] == 'success'
        assert result['results']['12345']['count'] == 1
        
        # Check transformed transaction
        transaction = result['results']['12345']['transactions'][0]
        assert transaction['id'] == 'TXN001'
        assert transaction['account_number'] == '12345'
        assert transaction['amount'] == Decimal('-100.00')
        assert transaction['transaction_type'] == 'debit'
        assert transaction['bank_source'] == 'rbc'
    
    def test_sync_bank_transactions_error(self, banking_module, mock_connector):
        """Test bank transaction sync error"""
        mock_connector.sync_data.return_value = {
            'status': 'error',
            'message': 'Bank API unavailable'
        }
        
        result = banking_module.sync_bank_transactions(['12345'])
        
        assert result['total_accounts_synced'] == 0
        assert result['results']['12345']['status'] == 'error'
        assert 'Bank API unavailable' in result['results']['12345']['message']
    
    def test_sync_account_balances_success(self, banking_module, mock_connector):
        """Test successful account balance sync"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': {
                'current_balance': '2500.75',
                'available_balance': '2400.75',
                'currency': 'CAD',
                'last_updated': '2024-01-10T10:00:00Z'
            }
        }
        
        result = banking_module.sync_account_balances(['12345'])
        
        assert result['module'] == 'BANKING_RBC'
        assert '12345' in result['results']
        assert result['results']['12345']['status'] == 'success'
        assert result['results']['12345']['current_balance'] == Decimal('2500.75')
        assert result['results']['12345']['available_balance'] == Decimal('2400.75')
        assert result['results']['12345']['currency'] == 'CAD'
    
    def test_initiate_payment_success(self, banking_module, mock_connector):
        """Test successful payment initiation"""
        mock_connector.send_data.return_value = {
            'status': 'success',
            'payment_id': 'PAY123',
            'transaction_id': 'TXN456'
        }
        
        payment_data = {
            'amount': 500.00,
            'recipient_account': '67890',
            'recipient_name': 'Test Recipient',
            'recipient_email': 'test@example.com',
            'currency': 'CAD'
        }
        
        result = banking_module.initiate_payment(payment_data)
        
        assert result['status'] == 'success'
        assert result['payment_id'] == 'PAY123'
        assert result['transaction_id'] == 'TXN456'
        assert result['amount'] == 500.00
        assert result['recipient'] == 'Test Recipient'
        assert result['payment_method'] == 'interac'
    
    def test_initiate_payment_validation_error(self, banking_module):
        """Test payment initiation with validation error"""
        payment_data = {
            'amount': -100.00,  # Invalid negative amount
            'recipient_name': 'Test Recipient'
            # Missing required fields
        }
        
        result = banking_module.initiate_payment(payment_data)
        
        assert result['status'] == 'error'
        assert 'Payment validation failed' in result['message']
        assert 'errors' in result
    
    def test_check_payment_status(self, banking_module, mock_connector):
        """Test payment status check"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': {
                'status': 'completed',
                'amount': '500.00',
                'currency': 'CAD',
                'created_date': '2024-01-10T09:00:00Z',
                'completed_date': '2024-01-10T09:05:00Z'
            }
        }
        
        result = banking_module.check_payment_status('PAY123')
        
        assert result['status'] == 'success'
        assert result['payment_id'] == 'PAY123'
        assert result['payment_status'] == 'completed'
        assert result['amount'] == Decimal('500.00')
        assert result['payment_method'] == 'interac'
    
    def test_sync_payment_methods(self, banking_module, mock_connector):
        """Test payment methods sync"""
        mock_connector.sync_data.return_value = {
            'status': 'success',
            'data': [
                {
                    'id': 'interac',
                    'name': 'Interac e-Transfer',
                    'type': 'email_transfer',
                    'enabled': True,
                    'fees': '1.50',
                    'processing_time': '5 minutes',
                    'min_amount': '0.01',
                    'max_amount': '3000.00',
                    'daily_limit': '3000.00'
                }
            ]
        }
        
        result = banking_module.sync_payment_methods()
        
        assert result['status'] == 'success'
        assert result['count'] == 1
        
        method = result['payment_methods'][0]
        assert method['id'] == 'interac'
        assert method['name'] == 'Interac e-Transfer'
        assert method['fees'] == Decimal('1.50')
        assert method['limits']['max_amount'] == Decimal('3000.00')
    
    def test_generate_bank_reconciliation_report(self, banking_module, mock_connector):
        """Test bank reconciliation report generation"""
        # Mock transaction sync
        mock_connector.sync_data.side_effect = [
            {
                'status': 'success',
                'data': [
                    {
                        'transaction_id': 'TXN001',
                        'amount': '100.00',
                        'type': 'credit'
                    },
                    {
                        'transaction_id': 'TXN002',
                        'amount': '-50.00',
                        'type': 'debit'
                    }
                ]
            },
            {
                'status': 'success',
                'data': {
                    'current_balance': '2050.00'
                }
            }
        ]
        
        result = banking_module.generate_bank_reconciliation_report(
            '12345', '2024-01-01', '2024-01-31'
        )
        
        assert result['status'] == 'success'
        assert result['account_number'] == '12345'
        assert result['summary']['total_transactions'] == 2
        assert result['summary']['total_credits'] == 100.0
        assert result['summary']['total_debits'] == 50.0
        assert result['summary']['net_change'] == 50.0
        assert result['summary']['current_balance'] == 2050.0
    
    def test_parse_amount(self, banking_module):
        """Test amount parsing"""
        assert banking_module._parse_amount(100) == Decimal('100')
        assert banking_module._parse_amount(100.50) == Decimal('100.50')
        assert banking_module._parse_amount('$1,234.56') == Decimal('1234.56')
        assert banking_module._parse_amount('1,000.00') == Decimal('1000.00')
        assert banking_module._parse_amount(Decimal('500.25')) == Decimal('500.25')
        assert banking_module._parse_amount('invalid') == Decimal('0')
    
    def test_determine_transaction_type(self, banking_module):
        """Test transaction type determination"""
        # Test explicit type
        transaction1 = {'type': 'CREDIT', 'amount': '100.00'}
        assert banking_module._determine_transaction_type(transaction1) == 'credit'
        
        # Test by amount sign
        transaction2 = {'amount': '100.00'}
        assert banking_module._determine_transaction_type(transaction2) == 'credit'
        
        transaction3 = {'amount': '-50.00'}
        assert banking_module._determine_transaction_type(transaction3) == 'debit'
    
    def test_validate_payment_data(self, banking_module):
        """Test payment data validation"""
        # Valid payment data
        valid_data = {
            'amount': 100.00,
            'recipient_account': '12345678',
            'recipient_name': 'Test Recipient'
        }
        result = banking_module._validate_payment_data(valid_data)
        assert result['valid'] is True
        assert len(result['errors']) == 0
        
        # Invalid payment data
        invalid_data = {
            'amount': -100.00,  # Negative amount
            'recipient_account': '123',  # Too short
            # Missing recipient_name
        }
        result = banking_module._validate_payment_data(invalid_data)
        assert result['valid'] is False
        assert len(result['errors']) > 0
    
    def test_transform_to_interac_format(self, banking_module):
        """Test transformation to Interac format"""
        payment_data = {
            'amount': 500.00,
            'recipient_email': 'test@example.com',
            'recipient_name': 'Test Recipient',
            'message': 'Test payment',
            'security_question': 'What is 2+2?',
            'security_answer': '4'
        }
        
        result = banking_module._transform_to_interac_format(payment_data)
        
        assert result['payment_type'] == 'interac_etransfer'
        assert result['amount'] == '500.0'
        assert result['recipient_email'] == 'test@example.com'
        assert result['security_question'] == 'What is 2+2?'
    
    def test_get_integration_status(self, banking_module):
        """Test getting integration status"""
        status = banking_module.get_integration_status()
        
        assert status['module'] == 'BANKING_RBC'
        assert status['bank_type'] == 'rbc'
        assert status['payment_gateway'] == 'interac'
        assert 'connector_status' in status
        assert 'supported_operations' in status
        assert 'sync_transactions' in status['supported_operations']
        assert 'initiate_payment' in status['supported_operations']

if __name__ == '__main__':
    pytest.main([__file__])

