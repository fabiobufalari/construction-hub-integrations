"""
Banking Integration Module
Handles integration with banking systems and payment gateways
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from src.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

class BankingIntegrationModule:
    """
    Banking Integration Module for Construction Hub Financial Recovery System
    Provides specialized integration capabilities for banking systems and payment gateways
    """
    
    def __init__(self, connector: BaseConnector):
        """
        Initialize banking integration module
        
        Args:
            connector: Base connector instance for banking communication
        """
        self.connector = connector
        self.bank_type = connector.config.get('bank_type', 'generic')
        self.payment_gateway = connector.config.get('payment_gateway', 'generic')
        self.module_name = f"BANKING_{self.bank_type.upper()}"
        
    def sync_bank_transactions(self, account_numbers: List[str], 
                             date_from: Optional[str] = None,
                             date_to: Optional[str] = None) -> Dict[str, Any]:
        """
        Synchronize bank transactions from banking system
        
        Args:
            account_numbers: List of bank account numbers to sync
            date_from: Start date for transaction sync (YYYY-MM-DD)
            date_to: End date for transaction sync (YYYY-MM-DD)
            
        Returns:
            Dict containing sync results for each account
        """
        results = {}
        
        # Set default date range if not provided
        if not date_from:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        
        for account_number in account_numbers:
            try:
                logger.info(f"Syncing transactions for account {account_number} from {self.bank_type}")
                
                # Prepare bank-specific filters
                filters = {
                    'account_number': account_number,
                    'date_from': date_from,
                    'date_to': date_to
                }
                
                # Apply bank-specific filters
                bank_filters = self._apply_bank_specific_filters(filters)
                
                # Sync transactions using connector
                sync_result = self.connector.sync_data('transactions', bank_filters)
                
                if sync_result.get('status') == 'success':
                    # Transform bank data to Construction Hub format
                    transformed_transactions = self._transform_bank_transactions(
                        sync_result.get('data', [])
                    )
                    
                    results[account_number] = {
                        'status': 'success',
                        'count': len(transformed_transactions),
                        'transactions': transformed_transactions,
                        'date_range': f"{date_from} to {date_to}"
                    }
                else:
                    results[account_number] = {
                        'status': 'error',
                        'message': sync_result.get('message', 'Unknown error'),
                        'date_range': f"{date_from} to {date_to}"
                    }
                    
            except Exception as e:
                logger.error(f"Failed to sync transactions for account {account_number}: {str(e)}")
                results[account_number] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        return {
            'module': self.module_name,
            'timestamp': datetime.utcnow().isoformat(),
            'results': results,
            'total_accounts_synced': sum(1 for r in results.values() if r.get('status') == 'success')
        }
    
    def sync_account_balances(self, account_numbers: List[str]) -> Dict[str, Any]:
        """
        Synchronize account balances from banking system
        
        Args:
            account_numbers: List of bank account numbers
            
        Returns:
            Dict containing balance information for each account
        """
        results = {}
        
        for account_number in account_numbers:
            try:
                logger.info(f"Syncing balance for account {account_number}")
                
                filters = {'account_number': account_number}
                bank_filters = self._apply_bank_specific_filters(filters)
                
                # Sync balance using connector
                sync_result = self.connector.sync_data('balance', bank_filters)
                
                if sync_result.get('status') == 'success':
                    balance_data = sync_result.get('data', {})
                    
                    results[account_number] = {
                        'status': 'success',
                        'current_balance': self._parse_amount(balance_data.get('current_balance', 0)),
                        'available_balance': self._parse_amount(balance_data.get('available_balance', 0)),
                        'currency': balance_data.get('currency', 'CAD'),
                        'last_updated': balance_data.get('last_updated', datetime.utcnow().isoformat())
                    }
                else:
                    results[account_number] = {
                        'status': 'error',
                        'message': sync_result.get('message', 'Unknown error')
                    }
                    
            except Exception as e:
                logger.error(f"Failed to sync balance for account {account_number}: {str(e)}")
                results[account_number] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        return {
            'module': self.module_name,
            'timestamp': datetime.utcnow().isoformat(),
            'results': results
        }
    
    def initiate_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Initiate a payment through the banking system or payment gateway
        
        Args:
            payment_data: Payment information
            
        Returns:
            Dict containing payment initiation results
        """
        try:
            logger.info(f"Initiating payment via {self.payment_gateway}")
            
            # Validate payment data
            validation_result = self._validate_payment_data(payment_data)
            if not validation_result['valid']:
                return {
                    'status': 'error',
                    'message': 'Payment validation failed',
                    'errors': validation_result['errors']
                }
            
            # Transform to bank/gateway format
            bank_payment_data = self._transform_payment_to_bank_format(payment_data)
            
            # Send payment using connector
            send_result = self.connector.send_data(bank_payment_data, 'payment')
            
            if send_result.get('status') == 'success':
                return {
                    'status': 'success',
                    'payment_id': send_result.get('payment_id'),
                    'transaction_id': send_result.get('transaction_id'),
                    'amount': payment_data.get('amount'),
                    'currency': payment_data.get('currency', 'CAD'),
                    'recipient': payment_data.get('recipient_name'),
                    'payment_method': self.payment_gateway,
                    'timestamp': datetime.utcnow().isoformat()
                }
            else:
                return {
                    'status': 'error',
                    'message': send_result.get('message', 'Payment failed'),
                    'payment_method': self.payment_gateway
                }
                
        except Exception as e:
            logger.error(f"Failed to initiate payment: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'payment_method': self.payment_gateway
            }
    
    def check_payment_status(self, payment_id: str) -> Dict[str, Any]:
        """
        Check the status of a payment
        
        Args:
            payment_id: Payment identifier
            
        Returns:
            Dict containing payment status information
        """
        try:
            logger.info(f"Checking status for payment {payment_id}")
            
            filters = {'payment_id': payment_id}
            bank_filters = self._apply_bank_specific_filters(filters)
            
            # Check status using connector
            status_result = self.connector.sync_data('payment_status', bank_filters)
            
            if status_result.get('status') == 'success':
                status_data = status_result.get('data', {})
                
                return {
                    'status': 'success',
                    'payment_id': payment_id,
                    'payment_status': status_data.get('status'),
                    'amount': self._parse_amount(status_data.get('amount', 0)),
                    'currency': status_data.get('currency', 'CAD'),
                    'created_date': status_data.get('created_date'),
                    'completed_date': status_data.get('completed_date'),
                    'failure_reason': status_data.get('failure_reason'),
                    'payment_method': self.payment_gateway
                }
            else:
                return {
                    'status': 'error',
                    'message': status_result.get('message', 'Failed to check payment status'),
                    'payment_id': payment_id
                }
                
        except Exception as e:
            logger.error(f"Failed to check payment status: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'payment_id': payment_id
            }
    
    def sync_payment_methods(self) -> Dict[str, Any]:
        """
        Synchronize available payment methods from the banking system
        
        Returns:
            Dict containing available payment methods
        """
        try:
            logger.info("Syncing available payment methods")
            
            # Sync payment methods using connector
            sync_result = self.connector.sync_data('payment_methods', {})
            
            if sync_result.get('status') == 'success':
                methods_data = sync_result.get('data', [])
                
                # Transform to standard format
                payment_methods = []
                for method in methods_data:
                    payment_methods.append({
                        'id': method.get('id'),
                        'name': method.get('name'),
                        'type': method.get('type'),
                        'enabled': method.get('enabled', True),
                        'fees': self._parse_amount(method.get('fees', 0)),
                        'currency': method.get('currency', 'CAD'),
                        'processing_time': method.get('processing_time'),
                        'limits': {
                            'min_amount': self._parse_amount(method.get('min_amount', 0)),
                            'max_amount': self._parse_amount(method.get('max_amount', 0)),
                            'daily_limit': self._parse_amount(method.get('daily_limit', 0))
                        }
                    })
                
                return {
                    'status': 'success',
                    'payment_methods': payment_methods,
                    'count': len(payment_methods),
                    'timestamp': datetime.utcnow().isoformat()
                }
            else:
                return {
                    'status': 'error',
                    'message': sync_result.get('message', 'Failed to sync payment methods')
                }
                
        except Exception as e:
            logger.error(f"Failed to sync payment methods: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def generate_bank_reconciliation_report(self, account_number: str, 
                                          date_from: str, date_to: str) -> Dict[str, Any]:
        """
        Generate bank reconciliation report
        
        Args:
            account_number: Bank account number
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            
        Returns:
            Dict containing reconciliation report
        """
        try:
            logger.info(f"Generating reconciliation report for account {account_number}")
            
            # Get bank transactions
            bank_sync = self.sync_bank_transactions([account_number], date_from, date_to)
            
            if bank_sync['results'][account_number]['status'] != 'success':
                return {
                    'status': 'error',
                    'message': 'Failed to sync bank transactions for reconciliation'
                }
            
            bank_transactions = bank_sync['results'][account_number]['transactions']
            
            # Calculate reconciliation data
            total_debits = sum(
                t['amount'] for t in bank_transactions 
                if t['transaction_type'] == 'debit'
            )
            total_credits = sum(
                t['amount'] for t in bank_transactions 
                if t['transaction_type'] == 'credit'
            )
            net_change = total_credits - total_debits
            
            # Get account balance
            balance_sync = self.sync_account_balances([account_number])
            current_balance = 0
            if balance_sync['results'][account_number]['status'] == 'success':
                current_balance = balance_sync['results'][account_number]['current_balance']
            
            return {
                'status': 'success',
                'account_number': account_number,
                'period': f"{date_from} to {date_to}",
                'summary': {
                    'total_transactions': len(bank_transactions),
                    'total_debits': float(total_debits),
                    'total_credits': float(total_credits),
                    'net_change': float(net_change),
                    'current_balance': float(current_balance)
                },
                'transactions': bank_transactions,
                'generated_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to generate reconciliation report: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _apply_bank_specific_filters(self, filters: Dict) -> Dict:
        """Apply bank-specific filters"""
        bank_filters = filters.copy()
        
        if self.bank_type.lower() == 'rbc':
            bank_filters.update(self._get_rbc_filters())
        elif self.bank_type.lower() == 'td':
            bank_filters.update(self._get_td_filters())
        elif self.bank_type.lower() == 'bmo':
            bank_filters.update(self._get_bmo_filters())
        elif self.bank_type.lower() == 'scotiabank':
            bank_filters.update(self._get_scotiabank_filters())
        
        return bank_filters
    
    def _get_rbc_filters(self) -> Dict:
        """Get RBC-specific filters"""
        return {
            'institution_id': '003',
            'format': 'json',
            'include_pending': True
        }
    
    def _get_td_filters(self) -> Dict:
        """Get TD Bank-specific filters"""
        return {
            'institution_id': '004',
            'format': 'json',
            'include_holds': True
        }
    
    def _get_bmo_filters(self) -> Dict:
        """Get BMO-specific filters"""
        return {
            'institution_id': '001',
            'format': 'json',
            'include_memo': True
        }
    
    def _get_scotiabank_filters(self) -> Dict:
        """Get Scotiabank-specific filters"""
        return {
            'institution_id': '002',
            'format': 'json',
            'include_categories': True
        }
    
    def _transform_bank_transactions(self, bank_data: List[Dict]) -> List[Dict]:
        """Transform bank transaction data to Construction Hub format"""
        transformed = []
        
        for transaction in bank_data:
            transformed.append({
                'id': transaction.get('transaction_id', transaction.get('id')),
                'account_number': transaction.get('account_number'),
                'transaction_date': transaction.get('transaction_date', transaction.get('date')),
                'posting_date': transaction.get('posting_date'),
                'description': transaction.get('description', transaction.get('memo')),
                'amount': self._parse_amount(transaction.get('amount', 0)),
                'transaction_type': self._determine_transaction_type(transaction),
                'currency': transaction.get('currency', 'CAD'),
                'balance_after': self._parse_amount(transaction.get('balance_after', 0)),
                'reference_number': transaction.get('reference_number'),
                'category': transaction.get('category'),
                'bank_source': self.bank_type
            })
        
        return transformed
    
    def _determine_transaction_type(self, transaction: Dict) -> str:
        """Determine if transaction is debit or credit"""
        amount = self._parse_amount(transaction.get('amount', 0))
        
        # Check explicit type field first
        if 'type' in transaction:
            return transaction['type'].lower()
        
        # Determine by amount sign
        return 'credit' if amount > 0 else 'debit'
    
    def _validate_payment_data(self, payment_data: Dict) -> Dict[str, Any]:
        """Validate payment data"""
        errors = []
        
        required_fields = ['amount', 'recipient_account', 'recipient_name']
        for field in required_fields:
            if field not in payment_data:
                errors.append(f"Required field '{field}' is missing")
        
        # Validate amount
        try:
            amount = self._parse_amount(payment_data.get('amount', 0))
            if amount <= 0:
                errors.append("Amount must be greater than zero")
        except (ValueError, TypeError):
            errors.append("Invalid amount format")
        
        # Validate account number format (basic validation)
        recipient_account = payment_data.get('recipient_account', '')
        if len(recipient_account) < 5:
            errors.append("Invalid recipient account number")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _transform_payment_to_bank_format(self, payment_data: Dict) -> Dict:
        """Transform payment data to bank-specific format"""
        if self.payment_gateway.lower() == 'interac':
            return self._transform_to_interac_format(payment_data)
        elif self.payment_gateway.lower() == 'wire':
            return self._transform_to_wire_format(payment_data)
        elif self.payment_gateway.lower() == 'ach':
            return self._transform_to_ach_format(payment_data)
        else:
            return payment_data
    
    def _transform_to_interac_format(self, payment_data: Dict) -> Dict:
        """Transform to Interac e-Transfer format"""
        return {
            'payment_type': 'interac_etransfer',
            'amount': str(payment_data.get('amount')),
            'currency': payment_data.get('currency', 'CAD'),
            'recipient_email': payment_data.get('recipient_email'),
            'recipient_name': payment_data.get('recipient_name'),
            'message': payment_data.get('message', ''),
            'security_question': payment_data.get('security_question'),
            'security_answer': payment_data.get('security_answer')
        }
    
    def _transform_to_wire_format(self, payment_data: Dict) -> Dict:
        """Transform to wire transfer format"""
        return {
            'payment_type': 'wire_transfer',
            'amount': str(payment_data.get('amount')),
            'currency': payment_data.get('currency', 'CAD'),
            'beneficiary_name': payment_data.get('recipient_name'),
            'beneficiary_account': payment_data.get('recipient_account'),
            'beneficiary_bank': payment_data.get('recipient_bank'),
            'swift_code': payment_data.get('swift_code'),
            'purpose_code': payment_data.get('purpose_code'),
            'reference': payment_data.get('reference')
        }
    
    def _transform_to_ach_format(self, payment_data: Dict) -> Dict:
        """Transform to ACH transfer format"""
        return {
            'payment_type': 'ach_transfer',
            'amount': str(payment_data.get('amount')),
            'currency': payment_data.get('currency', 'CAD'),
            'receiver_name': payment_data.get('recipient_name'),
            'receiver_account': payment_data.get('recipient_account'),
            'receiver_routing': payment_data.get('recipient_routing'),
            'transaction_code': payment_data.get('transaction_code', '22'),
            'description': payment_data.get('description')
        }
    
    def _parse_amount(self, amount) -> Decimal:
        """Parse amount to Decimal for precise calculations"""
        if isinstance(amount, (int, float)):
            return Decimal(str(amount))
        elif isinstance(amount, str):
            # Remove currency symbols and commas
            cleaned = amount.replace('$', '').replace(',', '').strip()
            return Decimal(cleaned)
        elif isinstance(amount, Decimal):
            return amount
        else:
            return Decimal('0')
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get banking integration status"""
        return {
            'module': self.module_name,
            'bank_type': self.bank_type,
            'payment_gateway': self.payment_gateway,
            'connector_status': self.connector.get_status(),
            'supported_operations': [
                'sync_transactions',
                'sync_balances',
                'initiate_payment',
                'check_payment_status',
                'reconciliation'
            ],
            'last_sync': self.connector.last_sync.isoformat() if self.connector.last_sync else None
        }

