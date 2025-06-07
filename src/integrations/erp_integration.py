"""
ERP Integration Module
Handles integration with ERP systems (SAP, Oracle, Microsoft Dynamics)
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from src.connectors.base_connector import BaseConnector

logger = logging.getLogger(__name__)

class ERPIntegrationModule:
    """
    ERP Integration Module for Construction Hub Financial Recovery System
    Provides specialized integration capabilities for ERP systems
    """
    
    def __init__(self, connector: BaseConnector):
        """
        Initialize ERP integration module
        
        Args:
            connector: Base connector instance for ERP communication
        """
        self.connector = connector
        self.erp_type = connector.config.get('erp_type', 'generic')
        self.module_name = f"ERP_{self.erp_type.upper()}"
        
    def sync_financial_data(self, data_types: List[str], filters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Synchronize financial data from ERP system
        
        Args:
            data_types: List of financial data types to sync
            filters: Optional filters for data synchronization
            
        Returns:
            Dict containing sync results for each data type
        """
        results = {}
        
        for data_type in data_types:
            try:
                logger.info(f"Syncing {data_type} from {self.erp_type} ERP")
                
                # Map Construction Hub data types to ERP-specific endpoints
                erp_endpoint = self._map_data_type_to_erp_endpoint(data_type)
                
                # Apply ERP-specific filters
                erp_filters = self._apply_erp_specific_filters(data_type, filters)
                
                # Sync data using connector
                sync_result = self.connector.sync_data(erp_endpoint, erp_filters)
                
                if sync_result.get('status') == 'success':
                    # Transform ERP data to Construction Hub format
                    transformed_data = self._transform_erp_data(data_type, sync_result.get('data', []))
                    
                    results[data_type] = {
                        'status': 'success',
                        'count': len(transformed_data),
                        'data': transformed_data,
                        'erp_endpoint': erp_endpoint
                    }
                else:
                    results[data_type] = {
                        'status': 'error',
                        'message': sync_result.get('message', 'Unknown error'),
                        'erp_endpoint': erp_endpoint
                    }
                    
            except Exception as e:
                logger.error(f"Failed to sync {data_type} from ERP: {str(e)}")
                results[data_type] = {
                    'status': 'error',
                    'message': str(e)
                }
        
        return {
            'module': self.module_name,
            'timestamp': datetime.utcnow().isoformat(),
            'results': results,
            'total_synced': sum(1 for r in results.values() if r.get('status') == 'success')
        }
    
    def send_financial_data(self, data_type: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Send financial data to ERP system
        
        Args:
            data_type: Type of financial data being sent
            data: List of financial records to send
            
        Returns:
            Dict containing send results
        """
        try:
            logger.info(f"Sending {len(data)} {data_type} records to {self.erp_type} ERP")
            
            # Map Construction Hub data type to ERP endpoint
            erp_endpoint = self._map_data_type_to_erp_endpoint(data_type)
            
            # Transform Construction Hub data to ERP format
            erp_data = self._transform_to_erp_format(data_type, data)
            
            # Send data using connector
            send_result = self.connector.send_data(erp_data, erp_endpoint)
            
            return {
                'module': self.module_name,
                'data_type': data_type,
                'records_sent': len(data),
                'erp_endpoint': erp_endpoint,
                'status': send_result.get('status', 'unknown'),
                'message': send_result.get('message', ''),
                'timestamp': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to send {data_type} to ERP: {str(e)}")
            return {
                'module': self.module_name,
                'data_type': data_type,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def sync_accounts_payable(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync accounts payable data from ERP"""
        return self.sync_financial_data(['accounts_payable'], filters)
    
    def sync_accounts_receivable(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync accounts receivable data from ERP"""
        return self.sync_financial_data(['accounts_receivable'], filters)
    
    def sync_general_ledger(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync general ledger data from ERP"""
        return self.sync_financial_data(['general_ledger'], filters)
    
    def sync_cost_centers(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync cost center data from ERP"""
        return self.sync_financial_data(['cost_centers'], filters)
    
    def sync_projects(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync project data from ERP"""
        return self.sync_financial_data(['projects'], filters)
    
    def _map_data_type_to_erp_endpoint(self, data_type: str) -> str:
        """
        Map Construction Hub data types to ERP-specific endpoints
        
        Args:
            data_type: Construction Hub data type
            
        Returns:
            ERP-specific endpoint
        """
        if self.erp_type.lower() == 'sap':
            return self._map_sap_endpoint(data_type)
        elif self.erp_type.lower() == 'oracle':
            return self._map_oracle_endpoint(data_type)
        elif self.erp_type.lower() == 'dynamics':
            return self._map_dynamics_endpoint(data_type)
        else:
            # Generic mapping
            return data_type.replace('_', '-')
    
    def _map_sap_endpoint(self, data_type: str) -> str:
        """Map data types to SAP endpoints"""
        sap_mappings = {
            'accounts_payable': 'AP_INVOICE',
            'accounts_receivable': 'AR_INVOICE',
            'general_ledger': 'GL_ACCOUNT',
            'cost_centers': 'COST_CENTER',
            'projects': 'PROJECT_SYSTEM'
        }
        return sap_mappings.get(data_type, data_type.upper())
    
    def _map_oracle_endpoint(self, data_type: str) -> str:
        """Map data types to Oracle endpoints"""
        oracle_mappings = {
            'accounts_payable': 'ap/invoices',
            'accounts_receivable': 'ar/invoices',
            'general_ledger': 'gl/journals',
            'cost_centers': 'gl/costcenters',
            'projects': 'ppm/projects'
        }
        return oracle_mappings.get(data_type, data_type.replace('_', '/'))
    
    def _map_dynamics_endpoint(self, data_type: str) -> str:
        """Map data types to Microsoft Dynamics endpoints"""
        dynamics_mappings = {
            'accounts_payable': 'vendorInvoices',
            'accounts_receivable': 'customerInvoices',
            'general_ledger': 'generalLedgerEntries',
            'cost_centers': 'dimensions',
            'projects': 'projects'
        }
        return dynamics_mappings.get(data_type, data_type)
    
    def _apply_erp_specific_filters(self, data_type: str, filters: Optional[Dict]) -> Dict:
        """
        Apply ERP-specific filters to data synchronization
        
        Args:
            data_type: Type of data being synced
            filters: Base filters
            
        Returns:
            ERP-specific filters
        """
        if not filters:
            filters = {}
        
        erp_filters = filters.copy()
        
        # Add ERP-specific filter logic
        if self.erp_type.lower() == 'sap':
            erp_filters.update(self._get_sap_filters(data_type))
        elif self.erp_type.lower() == 'oracle':
            erp_filters.update(self._get_oracle_filters(data_type))
        elif self.erp_type.lower() == 'dynamics':
            erp_filters.update(self._get_dynamics_filters(data_type))
        
        return erp_filters
    
    def _get_sap_filters(self, data_type: str) -> Dict:
        """Get SAP-specific filters"""
        return {
            'client': self.connector.config.get('sap_client', '100'),
            'language': 'EN'
        }
    
    def _get_oracle_filters(self, data_type: str) -> Dict:
        """Get Oracle-specific filters"""
        return {
            'ledger_id': self.connector.config.get('oracle_ledger_id'),
            'business_unit': self.connector.config.get('oracle_business_unit')
        }
    
    def _get_dynamics_filters(self, data_type: str) -> Dict:
        """Get Microsoft Dynamics-specific filters"""
        return {
            'company': self.connector.config.get('dynamics_company'),
            'dataAreaId': self.connector.config.get('dynamics_data_area_id')
        }
    
    def _transform_erp_data(self, data_type: str, erp_data: List[Dict]) -> List[Dict]:
        """
        Transform ERP data to Construction Hub format
        
        Args:
            data_type: Type of data being transformed
            erp_data: Raw ERP data
            
        Returns:
            Transformed data in Construction Hub format
        """
        if self.erp_type.lower() == 'sap':
            return self._transform_sap_data(data_type, erp_data)
        elif self.erp_type.lower() == 'oracle':
            return self._transform_oracle_data(data_type, erp_data)
        elif self.erp_type.lower() == 'dynamics':
            return self._transform_dynamics_data(data_type, erp_data)
        else:
            return erp_data  # No transformation for generic ERP
    
    def _transform_sap_data(self, data_type: str, sap_data: List[Dict]) -> List[Dict]:
        """Transform SAP data to Construction Hub format"""
        transformed = []
        
        for record in sap_data:
            if data_type == 'accounts_payable':
                transformed.append({
                    'id': record.get('BELNR'),  # Document number
                    'vendor_id': record.get('LIFNR'),
                    'vendor_name': record.get('NAME1'),
                    'invoice_number': record.get('XBLNR'),
                    'amount': float(record.get('WRBTR', 0)),
                    'currency': record.get('WAERS'),
                    'due_date': record.get('ZFBDT'),
                    'posting_date': record.get('BUDAT'),
                    'status': record.get('AUGBL') and 'paid' or 'open',
                    'erp_source': 'SAP'
                })
            elif data_type == 'accounts_receivable':
                transformed.append({
                    'id': record.get('BELNR'),
                    'customer_id': record.get('KUNNR'),
                    'customer_name': record.get('NAME1'),
                    'invoice_number': record.get('XBLNR'),
                    'amount': float(record.get('WRBTR', 0)),
                    'currency': record.get('WAERS'),
                    'due_date': record.get('ZFBDT'),
                    'posting_date': record.get('BUDAT'),
                    'status': record.get('AUGBL') and 'paid' or 'open',
                    'erp_source': 'SAP'
                })
            # Add more SAP transformations as needed
        
        return transformed
    
    def _transform_oracle_data(self, data_type: str, oracle_data: List[Dict]) -> List[Dict]:
        """Transform Oracle data to Construction Hub format"""
        transformed = []
        
        for record in oracle_data:
            if data_type == 'accounts_payable':
                transformed.append({
                    'id': record.get('invoice_id'),
                    'vendor_id': record.get('vendor_id'),
                    'vendor_name': record.get('vendor_name'),
                    'invoice_number': record.get('invoice_num'),
                    'amount': float(record.get('invoice_amount', 0)),
                    'currency': record.get('invoice_currency_code'),
                    'due_date': record.get('terms_date'),
                    'posting_date': record.get('invoice_date'),
                    'status': record.get('payment_status_flag') == 'Y' and 'paid' or 'open',
                    'erp_source': 'Oracle'
                })
            # Add more Oracle transformations as needed
        
        return transformed
    
    def _transform_dynamics_data(self, data_type: str, dynamics_data: List[Dict]) -> List[Dict]:
        """Transform Microsoft Dynamics data to Construction Hub format"""
        transformed = []
        
        for record in dynamics_data:
            if data_type == 'accounts_payable':
                transformed.append({
                    'id': record.get('RecId'),
                    'vendor_id': record.get('VendAccount'),
                    'vendor_name': record.get('VendorName'),
                    'invoice_number': record.get('InvoiceNumber'),
                    'amount': float(record.get('InvoiceAmount', 0)),
                    'currency': record.get('CurrencyCode'),
                    'due_date': record.get('DueDate'),
                    'posting_date': record.get('InvoiceDate'),
                    'status': record.get('InvoiceStatus'),
                    'erp_source': 'Dynamics'
                })
            # Add more Dynamics transformations as needed
        
        return transformed
    
    def _transform_to_erp_format(self, data_type: str, construction_hub_data: List[Dict]) -> Dict:
        """
        Transform Construction Hub data to ERP format
        
        Args:
            data_type: Type of data being transformed
            construction_hub_data: Construction Hub data
            
        Returns:
            Data in ERP format
        """
        if self.erp_type.lower() == 'sap':
            return self._transform_to_sap_format(data_type, construction_hub_data)
        elif self.erp_type.lower() == 'oracle':
            return self._transform_to_oracle_format(data_type, construction_hub_data)
        elif self.erp_type.lower() == 'dynamics':
            return self._transform_to_dynamics_format(data_type, construction_hub_data)
        else:
            return {'data': construction_hub_data}
    
    def _transform_to_sap_format(self, data_type: str, data: List[Dict]) -> Dict:
        """Transform to SAP format"""
        sap_records = []
        
        for record in data:
            if data_type == 'accounts_payable':
                sap_records.append({
                    'BUKRS': self.connector.config.get('sap_company_code'),
                    'LIFNR': record.get('vendor_id'),
                    'XBLNR': record.get('invoice_number'),
                    'WRBTR': record.get('amount'),
                    'WAERS': record.get('currency', 'CAD'),
                    'ZFBDT': record.get('due_date'),
                    'BUDAT': record.get('posting_date')
                })
        
        return {'INVOICES': sap_records}
    
    def _transform_to_oracle_format(self, data_type: str, data: List[Dict]) -> Dict:
        """Transform to Oracle format"""
        oracle_records = []
        
        for record in data:
            if data_type == 'accounts_payable':
                oracle_records.append({
                    'vendor_id': record.get('vendor_id'),
                    'invoice_num': record.get('invoice_number'),
                    'invoice_amount': record.get('amount'),
                    'invoice_currency_code': record.get('currency', 'CAD'),
                    'terms_date': record.get('due_date'),
                    'invoice_date': record.get('posting_date')
                })
        
        return {'invoices': oracle_records}
    
    def _transform_to_dynamics_format(self, data_type: str, data: List[Dict]) -> Dict:
        """Transform to Microsoft Dynamics format"""
        dynamics_records = []
        
        for record in data:
            if data_type == 'accounts_payable':
                dynamics_records.append({
                    'VendAccount': record.get('vendor_id'),
                    'InvoiceNumber': record.get('invoice_number'),
                    'InvoiceAmount': record.get('amount'),
                    'CurrencyCode': record.get('currency', 'CAD'),
                    'DueDate': record.get('due_date'),
                    'InvoiceDate': record.get('posting_date')
                })
        
        return {'vendorInvoices': dynamics_records}
    
    def get_integration_status(self) -> Dict[str, Any]:
        """
        Get ERP integration status
        
        Returns:
            Dict containing integration status information
        """
        return {
            'module': self.module_name,
            'erp_type': self.erp_type,
            'connector_status': self.connector.get_status(),
            'supported_data_types': [
                'accounts_payable',
                'accounts_receivable', 
                'general_ledger',
                'cost_centers',
                'projects'
            ],
            'last_sync': self.connector.last_sync.isoformat() if self.connector.last_sync else None
        }

