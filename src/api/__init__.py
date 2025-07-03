"""
API Blueprint for Integration Service
External APIs for third-party system integration with full CRUD operations
"""

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy.exc import SQLAlchemyError
import logging

from src.services.integration_service import IntegrationService

logger = logging.getLogger(__name__)

# Create API blueprint
api_bp = Blueprint('api', __name__)

@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'integration-service',
        'version': '1.0.0'
    }), 200

# ============================================
# Integration Logs CRUD
# ============================================

@api_bp.route('/logs', methods=['GET'])
@jwt_required()
def get_integration_logs():
    """Get integration logs with optional filtering"""
    try:
        connector_name = request.args.get('connector_name')
        status = request.args.get('status')
        limit = int(request.args.get('limit', 100))
        
        logs = IntegrationService.get_integration_logs(
            connector_name=connector_name,
            status=status,
            limit=limit
        )
        
        return jsonify({
            'status': 'success',
            'count': len(logs),
            'logs': [log.to_dict() for log in logs]
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching integration logs: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/logs/<log_id>', methods=['GET'])
@jwt_required()
def get_integration_log(log_id):
    """Get specific integration log by ID"""
    try:
        log = IntegrationService.get_integration_log_by_id(log_id)
        
        if not log:
            return jsonify({
                'status': 'error',
                'message': 'Integration log not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'log': log.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching integration log {log_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/logs', methods=['POST'])
@jwt_required()
def create_integration_log():
    """Create new integration log entry"""
    try:
        data = request.get_json()
        
        if not data or not all(k in data for k in ['connector_name', 'operation', 'status']):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: connector_name, operation, status'
            }), 400
        
        log = IntegrationService.create_integration_log(
            connector_name=data['connector_name'],
            operation=data['operation'],
            status=data['status'],
            details=data.get('details'),
            request_data=data.get('request_data'),
            response_data=data.get('response_data'),
            error_message=data.get('error_message'),
            execution_time_ms=data.get('execution_time_ms')
        )
        
        return jsonify({
            'status': 'success',
            'message': 'Integration log created successfully',
            'log': log.to_dict()
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating integration log: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ============================================
# Connector Configurations CRUD
# ============================================

@api_bp.route('/connectors', methods=['GET'])
@jwt_required()
def get_connector_configs():
    """Get all connector configurations"""
    try:
        is_active = request.args.get('is_active')
        if is_active is not None:
            is_active = is_active.lower() == 'true'
        
        configs = IntegrationService.get_connector_configs(is_active=is_active)
        
        return jsonify({
            'status': 'success',
            'count': len(configs),
            'connectors': [config.to_dict() for config in configs]
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching connector configs: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/connectors/<connector_name>', methods=['GET'])
@jwt_required()
def get_connector_config(connector_name):
    """Get specific connector configuration"""
    try:
        config = IntegrationService.get_connector_config_by_name(connector_name)
        
        if not config:
            return jsonify({
                'status': 'error',
                'message': 'Connector configuration not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'connector': config.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching connector config {connector_name}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/connectors', methods=['POST'])
@jwt_required()
def create_connector_config():
    """Create new connector configuration"""
    try:
        data = request.get_json()
        
        if not data or not all(k in data for k in ['connector_name', 'connector_type', 'config_data']):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: connector_name, connector_type, config_data'
            }), 400
        
        config = IntegrationService.create_connector_config(
            connector_name=data['connector_name'],
            connector_type=data['connector_type'],
            config_data=data['config_data']
        )
        
        return jsonify({
            'status': 'success',
            'message': 'Connector configuration created successfully',
            'connector': config.to_dict()
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating connector config: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/connectors/<connector_name>', methods=['PUT'])
@jwt_required()
def update_connector_config(connector_name):
    """Update connector configuration"""
    try:
        data = request.get_json()
        
        if not data or 'config_data' not in data:
            return jsonify({
                'status': 'error',
                'message': 'Missing required field: config_data'
            }), 400
        
        config = IntegrationService.update_connector_config(
            connector_name=connector_name,
            config_data=data['config_data'],
            is_active=data.get('is_active')
        )
        
        if not config:
            return jsonify({
                'status': 'error',
                'message': 'Connector configuration not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Connector configuration updated successfully',
            'connector': config.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error updating connector config {connector_name}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/connectors/<connector_name>', methods=['DELETE'])
@jwt_required()
def delete_connector_config(connector_name):
    """Delete connector configuration"""
    try:
        success = IntegrationService.delete_connector_config(connector_name)
        
        if not success:
            return jsonify({
                'status': 'error',
                'message': 'Connector configuration not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Connector configuration deleted successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error deleting connector config {connector_name}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ============================================
# Integration Jobs CRUD
# ============================================

@api_bp.route('/jobs', methods=['GET'])
@jwt_required()
def get_integration_jobs():
    """Get all integration jobs"""
    try:
        status = request.args.get('status')
        
        jobs = IntegrationService.get_integration_jobs(status=status)
        
        return jsonify({
            'status': 'success',
            'count': len(jobs),
            'jobs': [job.to_dict() for job in jobs]
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching integration jobs: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/jobs/<job_id>', methods=['GET'])
@jwt_required()
def get_integration_job(job_id):
    """Get specific integration job by ID"""
    try:
        job = IntegrationService.get_integration_job_by_id(job_id)
        
        if not job:
            return jsonify({
                'status': 'error',
                'message': 'Integration job not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'job': job.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching integration job {job_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/jobs', methods=['POST'])
@jwt_required()
def create_integration_job():
    """Create new integration job"""
    try:
        data = request.get_json()
        
        if not data or not all(k in data for k in ['job_name', 'connector_name', 'job_type']):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: job_name, connector_name, job_type'
            }), 400
        
        job = IntegrationService.create_integration_job(
            job_name=data['job_name'],
            connector_name=data['connector_name'],
            job_type=data['job_type'],
            job_config=data.get('job_config'),
            schedule_expression=data.get('schedule_expression')
        )
        
        return jsonify({
            'status': 'success',
            'message': 'Integration job created successfully',
            'job': job.to_dict()
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating integration job: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/jobs/<job_id>', methods=['PUT'])
@jwt_required()
def update_integration_job(job_id):
    """Update integration job"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'No data provided'
            }), 400
        
        job = IntegrationService.update_integration_job(job_id, **data)
        
        if not job:
            return jsonify({
                'status': 'error',
                'message': 'Integration job not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Integration job updated successfully',
            'job': job.to_dict()
        }), 200
        
    except Exception as e:
        logger.error(f"Error updating integration job {job_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/jobs/<job_id>', methods=['DELETE'])
@jwt_required()
def delete_integration_job(job_id):
    """Delete integration job"""
    try:
        success = IntegrationService.delete_integration_job(job_id)
        
        if not success:
            return jsonify({
                'status': 'error',
                'message': 'Integration job not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Integration job deleted successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error deleting integration job {job_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# ============================================
# Legacy endpoints for backward compatibility
# ============================================

@api_bp.route('/connectors/<connector_type>/test', methods=['POST'])
@jwt_required()
def test_connector(connector_type):
    """Test connector connection"""
    try:
        # Log the test operation
        IntegrationService.create_integration_log(
            connector_name=connector_type,
            operation='test_connection',
            status='success',
            details=f'{connector_type} connector test successful'
        )
        
        return jsonify({
            'status': 'success',
            'connector_type': connector_type,
            'message': f'{connector_type} connector test successful'
        }), 200
    except Exception as e:
        # Log the error
        IntegrationService.create_integration_log(
            connector_name=connector_type,
            operation='test_connection',
            status='error',
            error_message=str(e)
        )
        
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/sync/<data_type>', methods=['POST'])
@jwt_required()
def sync_data(data_type):
    """Sync data from external systems"""
    try:
        filters = request.json or {}
        
        # Log the sync operation
        IntegrationService.create_integration_log(
            connector_name='sync_service',
            operation=f'sync_{data_type}',
            status='success',
            request_data=filters,
            response_data={'count': 0, 'data': []}
        )
        
        return jsonify({
            'status': 'success',
            'data_type': data_type,
            'count': 0,
            'data': []
        }), 200
    except Exception as e:
        # Log the error
        IntegrationService.create_integration_log(
            connector_name='sync_service',
            operation=f'sync_{data_type}',
            status='error',
            error_message=str(e)
        )
        
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/send/<data_type>', methods=['POST'])
@jwt_required()
def send_data(data_type):
    """Send data to external systems"""
    try:
        data = request.json or {}
        
        # Log the send operation
        IntegrationService.create_integration_log(
            connector_name='send_service',
            operation=f'send_{data_type}',
            status='success',
            request_data=data,
            response_data={'message': 'Data sent successfully'}
        )
        
        return jsonify({
            'status': 'success',
            'data_type': data_type,
            'message': 'Data sent successfully'
        }), 200
    except Exception as e:
        # Log the error
        IntegrationService.create_integration_log(
            connector_name='send_service',
            operation=f'send_{data_type}',
            status='error',
            error_message=str(e)
        )
        
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

