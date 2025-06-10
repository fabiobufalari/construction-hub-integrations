"""
ERP Integration Module
Handles integration with ERP systems (SAP, PostgreSQL-based ERPs, Microsoft Dynamics)
Migrated from Oracle to PostgreSQL for Construction Hub Financial Recovery System
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
    Now fully compatible with PostgreSQL-based ERPs
    """
    
    def __init__(self, connector: BaseConnector):
        """
        Initialize ERP integration module / Inicializar módulo de integração ERP
        
        Args:
            connector: Base connector instance for ERP communication
        """
        self.connector = connector
        self.erp_type = connector.config.get('erp_type', 'generic')
        self.module_name = f"ERP_{self.erp_type.upper()}"
        
    def sync_financial_data(self, data_types: List[str], filters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Synchronize financial data from ERP system / Sincronizar dados financeiros do sistema ERP
        
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
        Send financial data to ERP system / Enviar dados financeiros para o sistema ERP
        
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
        """Sync accounts payable data from ERP / Sincronizar dados de contas a pagar do ERP"""
        return self.sync_financial_data(['accounts_payable'], filters)
    
    def sync_accounts_receivable(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync accounts receivable data from ERP / Sincronizar dados de contas a receber do ERP"""
        return self.sync_financial_data(['accounts_receivable'], filters)
    
    def sync_general_ledger(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync general ledger data from ERP / Sincronizar dados do razão geral do ERP"""
        return self.sync_financial_data(['general_ledger'], filters)
    
    def sync_cost_centers(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync cost center data from ERP / Sincronizar dados de centros de custo do ERP"""
        return self.sync_financial_data(['cost_centers'], filters)
    
    def sync_projects(self, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Sync project data from ERP / Sincronizar dados de projetos do ERP"""
        return self.sync_financial_data(['projects'], filters)
    
    def _map_data_type_to_erp_endpoint(self, data_type: str) -> str:
        """
        Map Construction Hub data types to ERP-specific endpoints
        Updated to support PostgreSQL-based ERPs instead of Oracle
        
        Args:
            data_type: Construction Hub data type
            
        Returns:
            ERP-specific endpoint
        """
        if self.erp_type.lower() == 'sap':
            return self._map_sap_endpoint(data_type)
        elif self.erp_type.lower() == 'postgresql_erp':
            return self._map_postgresql_erp_endpoint(data_type)
        elif self.erp_type.lower() == 'dynamics':
            return self._map_dynamics_endpoint(data_type)
        else:
            # Generic mapping
            return data_type.replace('_', '-')
    
    def _map_sap_endpoint(self, data_type: str) -> str:
        """Map data types to SAP endpoints / Mapear tipos de dados para endpoints SAP"""
        sap_mappings = {
            'accounts_payable': 'AP_INVOICE',
            'accounts_receivable': 'AR_INVOICE',
            'general_ledger': 'GL_ACCOUNT',
            'cost_centers': 'COST_CENTER',
            'projects': 'PROJECT_SYSTEM'
        }
        return sap_mappings.get(data_type, data_type.upper())
    
    def _map_postgresql_erp_endpoint(self, data_type: str) -> str:
        """
        Map data types to PostgreSQL-based ERP endpoints
        Replaces Oracle endpoint mapping for better PostgreSQL compatibility
        """
        postgresql_erp_mappings = {
            'accounts_payable': 'api/v1/ap/invoices',
            'accounts_receivable': 'api/v1/ar/invoices',
            'general_ledger': 'api/v1/gl/journals',
            'cost_centers': 'api/v1/gl/costcenters',
            'projects': 'api/v1/pm/projects'
        }
        return postgresql_erp_mappings.get(data_type, f"api/v1/{data_type.replace('_', '/')}")
    
    def _map_dynamics_endpoint(self, data_type: str) -> str:
        """Map data types to Microsoft Dynamics endpoints / Mapear tipos de dados para endpoints Microsoft Dynamics"""
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
        Updated for PostgreSQL compatibility
        
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
        elif self.erp_type.lower() == 'postgresql_erp':
            erp_filters.update(self._get_postgresql_erp_filters(data_type))
        elif self.erp_type.lower() == 'dynamics':
            erp_filters.update(self._get_dynamics_filters(data_type))
        
        return erp_filters
    
    def _get_sap_filters(self, data_type: str) -> Dict:
        """Get SAP-specific filters / Obter filtros específicos do SAP"""
        return {
            'client': self.connector.config.get('sap_client', '100'),
            'language': 'EN'
        }
    
    def _get_postgresql_erp_filters(self, data_type: str) -> Dict:
        """
        Get PostgreSQL ERP-specific filters
        Replaces Oracle-specific filters for better PostgreSQL compatibility
        """
        return {
            'database_schema': self.connector.config.get('postgresql_schema', 'public'),
            'company_id': self.connector.config.get('company_id'),
            'fiscal_year': self.connector.config.get('fiscal_year'),
            'currency': self.connector.config.get('default_currency', 'CAD')
        }
    
    def _get_dynamics_filters(self, data_type: str) -> Dict:
        """Get Microsoft Dynamics-specific filters / Obter filtros específicos do Microsoft Dynamics"""
        return {
            'company': self.connector.config.get('dynamics_company'),
            'dataAreaId': self.connector.config.get('dynamics_data_area_id')
        }
    
    def _transform_erp_data(self, data_type: str, erp_data: List[Dict]) -> List[Dict]:
        """
        Transform ERP data to Construction Hub format
        Updated to handle PostgreSQL-based ERP data
        
        Args:
            data_type: Type of data being transformed
            erp_data: Raw ERP data
            
        Returns:
            Transformed data in Construction Hub format
        """
        if self.erp_type.lower() == 'sap':
            return self._transform_sap_data(data_type, erp_data)
        elif self.erp_type.lower() == 'postgresql_erp':
            return self._transform_postgresql_erp_data(data_type, erp_data)
        elif self.erp_type.lower() == 'dynamics':
            return self._transform_dynamics_data(data_type, erp_data)
        else:
            return erp_data  # No transformation for generic ERP
    
    def _transform_sap_data(self, data_type: str, sap_data: List[Dict]) -> List[Dict]:
        """Transform SAP data to Construction Hub format / Transformar dados SAP para formato Construction Hub"""
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
    
    def _transform_postgresql_erp_data(self, data_type: str, postgresql_data: List[Dict]) -> List[Dict]:
        """
        Transform PostgreSQL ERP data to Construction Hub format
        Replaces Oracle data transformation for PostgreSQL compatibility
        """
        transformed = []
        
        for record in postgresql_data:
            if data_type == 'accounts_payable':
                transformed.append({
                    'id': record.get('invoice_id'),
                    'vendor_id': record.get('vendor_id'),
                    'vendor_name': record.get('vendor_name'),
                    'invoice_number': record.get('invoice_number'),
                    'amount': float(record.get('invoice_amount', 0)),
                    'currency': record.get('currency_code', 'CAD'),
                    'due_date': record.get('due_date'),
                    'posting_date': record.get('invoice_date'),
                    'status': record.get('payment_status') == 'PAID' and 'paid' or 'open',
                    'erp_source': 'PostgreSQL_ERP'
                })
            elif data_type == 'accounts_receivable':
                transformed.append({
                    'id': record.get('invoice_id'),
                    'customer_id': record.get('customer_id'),
                    'customer_name': record.get('customer_name'),
                    'invoice_number': record.get('invoice_number'),
                    'amount': float(record.get('invoice_amount', 0)),
                    'currency': record.get('currency_code', 'CAD'),
                    'due_date': record.get('due_date'),
                    'posting_date': record.get('invoice_date'),
                    'status': record.get('payment_status') == 'PAID' and 'paid' or 'open',
                    'erp_source': 'PostgreSQL_ERP'
                })
            # Add more PostgreSQL ERP transformations as needed
        
        return transformed
    
    def _transform_dynamics_data(self, data_type: str, dynamics_data: List[Dict]) -> List[Dict]:
        """Transform Microsoft Dynamics data to Construction Hub format / Transformar dados Microsoft Dynamics para formato Construction Hub"""
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
        Updated to support PostgreSQL-based ERPs
        
        Args:
            data_type: Type of data being transformed
            construction_hub_data: Construction Hub data
            
        Returns:
            Data in ERP format
        """
        if self.erp_type.lower() == 'sap':
            return self._transform_to_sap_format(data_type, construction_hub_data)
        elif self.erp_type.lower() == 'postgresql_erp':
            return self._transform_to_postgresql_erp_format(data_type, construction_hub_data)
        elif self.erp_type.lower() == 'dynamics':
            return self._transform_to_dynamics_format(data_type, construction_hub_data)
        else:
            return {'data': construction_hub_data}
    
    def _transform_to_sap_format(self, data_type: str, data: List[Dict]) -> Dict:
        """Transform to SAP format / Transformar para formato SAP"""
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
    
    def _transform_to_postgresql_erp_format(self, data_type: str, data: List[Dict]) -> Dict:
        """
        Transform to PostgreSQL ERP format
        Replaces Oracle format transformation for PostgreSQL compatibility
        """
        postgresql_records = []
        
        for record in data:
            if data_type == 'accounts_payable':
                postgresql_records.append({
                    'vendor_id': record.get('vendor_id'),
                    'invoice_number': record.get('invoice_number'),
                    'invoice_amount': record.get('amount'),
                    'currency_code': record.get('currency', 'CAD'),
                    'due_date': record.get('due_date'),
                    'invoice_date': record.get('posting_date'),
                    'company_id': self.connector.config.get('company_id'),
                    'created_by': 'construction_hub_system',
                    'created_at': datetime.utcnow().isoformat()
                })
        
        return {'invoices': postgresql_records}
    
    def _transform_to_dynamics_format(self, data_type: str, data: List[Dict]) -> Dict:
        """Transform to Microsoft Dynamics format / Transformar para formato Microsoft Dynamics"""
        dynamics_records = []
        
        for record in data:
            if data_type == 'accounts_payable':
                dynamics_records.append({
                    'VendAccount': record.get('vendor_id'),
                    'InvoiceNumber': record.get('invoice_number'),
                    'InvoiceAmount': record.get('amount'),
                    'CurrencyCode': record.get('currency', 'CAD'),
                    'DueDate': record.get('due_date'),
                    'InvoiceDate': record.get('posting_date'),
                    'DataAreaId': self.connector.config.get('dynamics_data_area_id')
                })
        
        return {'VendorInvoices': dynamics_records}
    
    def get_integration_status(self) -> Dict[str, Any]:
        """
        Get current integration status
        Updated to reflect PostgreSQL compatibility
        """
        return {
            'module': self.module_name,
            'erp_type': self.erp_type,
            'database_type': 'PostgreSQL',  # Updated from Oracle
            'connector_status': self.connector.get_status(),
            'supported_data_types': [
                'accounts_payable',
                'accounts_receivable', 
                'general_ledger',
                'cost_centers',
                'projects'
            ],
            'last_sync': getattr(self.connector, 'last_sync', None),
            'configuration': {
                'postgresql_compatible': True,  # New flag
                'oracle_compatible': False,     # Deprecated
                'supports_real_time': True,
                'supports_batch': True
            }
        }

