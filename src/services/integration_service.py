"""
Integration Service - CRUD operations for integration management
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from flask import current_app
from sqlalchemy.exc import SQLAlchemyError
from app import db
from src.models.integration_model import IntegrationLogModel, ConnectorConfigModel, IntegrationJobModel

class IntegrationService:
    """
    Service class for integration operations
    """
    
    @staticmethod
    def create_integration_log(connector_name: str, operation: str, status: str, 
                             details: Optional[str] = None, request_data: Optional[Dict] = None,
                             response_data: Optional[Dict] = None, error_message: Optional[str] = None,
                             execution_time_ms: Optional[int] = None) -> IntegrationLogModel:
        """
        Create a new integration log entry
        """
        try:
            log_entry = IntegrationLogModel(
                connector_name=connector_name,
                operation=operation,
                status=status,
                details=details,
                request_data=request_data,
                response_data=response_data,
                error_message=error_message,
                execution_time_ms=execution_time_ms
            )
            
            db.session.add(log_entry)
            db.session.commit()
            
            current_app.logger.info(f"Created integration log: {connector_name}.{operation} - {status}")
            return log_entry
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error creating integration log: {str(e)}")
            raise
    
    @staticmethod
    def get_integration_logs(connector_name: Optional[str] = None, 
                           status: Optional[str] = None,
                           limit: int = 100) -> List[IntegrationLogModel]:
        """
        Get integration logs with optional filtering
        """
        try:
            query = IntegrationLogModel.query
            
            if connector_name:
                query = query.filter(IntegrationLogModel.connector_name == connector_name)
            
            if status:
                query = query.filter(IntegrationLogModel.status == status)
            
            logs = query.order_by(IntegrationLogModel.created_at.desc()).limit(limit).all()
            return logs
            
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching integration logs: {str(e)}")
            raise
    
    @staticmethod
    def get_integration_log_by_id(log_id: str) -> Optional[IntegrationLogModel]:
        """
        Get integration log by ID
        """
        try:
            return IntegrationLogModel.query.filter_by(id=log_id).first()
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching integration log {log_id}: {str(e)}")
            raise
    
    @staticmethod
    def create_connector_config(connector_name: str, connector_type: str, 
                              config_data: Dict[str, Any]) -> ConnectorConfigModel:
        """
        Create a new connector configuration
        """
        try:
            config = ConnectorConfigModel(
                connector_name=connector_name,
                connector_type=connector_type,
                config_data=config_data
            )
            
            db.session.add(config)
            db.session.commit()
            
            current_app.logger.info(f"Created connector config: {connector_name}")
            return config
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error creating connector config: {str(e)}")
            raise
    
    @staticmethod
    def get_connector_configs(is_active: Optional[bool] = None) -> List[ConnectorConfigModel]:
        """
        Get all connector configurations
        """
        try:
            query = ConnectorConfigModel.query
            
            if is_active is not None:
                query = query.filter(ConnectorConfigModel.is_active == is_active)
            
            configs = query.order_by(ConnectorConfigModel.connector_name).all()
            return configs
            
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching connector configs: {str(e)}")
            raise
    
    @staticmethod
    def get_connector_config_by_name(connector_name: str) -> Optional[ConnectorConfigModel]:
        """
        Get connector configuration by name
        """
        try:
            return ConnectorConfigModel.query.filter_by(connector_name=connector_name).first()
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching connector config {connector_name}: {str(e)}")
            raise
    
    @staticmethod
    def update_connector_config(connector_name: str, config_data: Dict[str, Any], 
                              is_active: Optional[bool] = None) -> Optional[ConnectorConfigModel]:
        """
        Update connector configuration
        """
        try:
            config = ConnectorConfigModel.query.filter_by(connector_name=connector_name).first()
            
            if not config:
                return None
            
            config.config_data = config_data
            if is_active is not None:
                config.is_active = is_active
            config.updated_at = datetime.utcnow()
            
            db.session.commit()
            
            current_app.logger.info(f"Updated connector config: {connector_name}")
            return config
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error updating connector config: {str(e)}")
            raise
    
    @staticmethod
    def delete_connector_config(connector_name: str) -> bool:
        """
        Delete connector configuration
        """
        try:
            config = ConnectorConfigModel.query.filter_by(connector_name=connector_name).first()
            
            if not config:
                return False
            
            db.session.delete(config)
            db.session.commit()
            
            current_app.logger.info(f"Deleted connector config: {connector_name}")
            return True
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error deleting connector config: {str(e)}")
            raise
    
    @staticmethod
    def create_integration_job(job_name: str, connector_name: str, job_type: str,
                             job_config: Optional[Dict] = None, 
                             schedule_expression: Optional[str] = None) -> IntegrationJobModel:
        """
        Create a new integration job
        """
        try:
            job = IntegrationJobModel(
                job_name=job_name,
                connector_name=connector_name,
                job_type=job_type,
                job_config=job_config,
                schedule_expression=schedule_expression
            )
            
            db.session.add(job)
            db.session.commit()
            
            current_app.logger.info(f"Created integration job: {job_name}")
            return job
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error creating integration job: {str(e)}")
            raise
    
    @staticmethod
    def get_integration_jobs(status: Optional[str] = None) -> List[IntegrationJobModel]:
        """
        Get all integration jobs
        """
        try:
            query = IntegrationJobModel.query
            
            if status:
                query = query.filter(IntegrationJobModel.status == status)
            
            jobs = query.order_by(IntegrationJobModel.job_name).all()
            return jobs
            
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching integration jobs: {str(e)}")
            raise
    
    @staticmethod
    def get_integration_job_by_id(job_id: str) -> Optional[IntegrationJobModel]:
        """
        Get integration job by ID
        """
        try:
            return IntegrationJobModel.query.filter_by(id=job_id).first()
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error fetching integration job {job_id}: {str(e)}")
            raise
    
    @staticmethod
    def update_integration_job(job_id: str, **kwargs) -> Optional[IntegrationJobModel]:
        """
        Update integration job
        """
        try:
            job = IntegrationJobModel.query.filter_by(id=job_id).first()
            
            if not job:
                return None
            
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
            
            job.updated_at = datetime.utcnow()
            db.session.commit()
            
            current_app.logger.info(f"Updated integration job: {job.job_name}")
            return job
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error updating integration job: {str(e)}")
            raise
    
    @staticmethod
    def delete_integration_job(job_id: str) -> bool:
        """
        Delete integration job
        """
        try:
            job = IntegrationJobModel.query.filter_by(id=job_id).first()
            
            if not job:
                return False
            
            db.session.delete(job)
            db.session.commit()
            
            current_app.logger.info(f"Deleted integration job: {job.job_name}")
            return True
            
        except SQLAlchemyError as e:
            db.session.rollback()
            current_app.logger.error(f"Error deleting integration job: {str(e)}")
            raise

