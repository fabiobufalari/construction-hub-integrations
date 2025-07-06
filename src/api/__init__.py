"""
API Blueprint for Integration Service
External APIs for third-party system integration
"""

from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app import db
from src.models.integration import Integration
import logging
import uuid

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

# CRUD Endpoints for Integrations
@api_bp.route('/integrations', methods=['GET'])
@jwt_required()
def get_integrations():
    """Get all integrations"""
    try:
        integrations = Integration.query.all()
        return jsonify({
            'status': 'success',
            'data': [integration.to_dict() for integration in integrations]
        }), 200
    except Exception as e:
        logger.error(f"Error getting integrations: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/integrations', methods=['POST'])
@jwt_required()
def create_integration():
    """Create new integration"""
    try:
        data = request.json
        if not data or not data.get('name') or not data.get('integration_type'):
            return jsonify({
                'status': 'error',
                'message': 'Name and integration_type are required'
            }), 400
        
        integration = Integration.from_dict(data)
        db.session.add(integration)
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'data': integration.to_dict()
        }), 201
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating integration: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/integrations/<integration_id>', methods=['GET'])
@jwt_required()
def get_integration(integration_id):
    """Get integration by ID"""
    try:
        integration = Integration.query.get_or_404(integration_id)
        return jsonify({
            'status': 'success',
            'data': integration.to_dict()
        }), 200
    except Exception as e:
        logger.error(f"Error getting integration: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/integrations/<integration_id>', methods=['PUT'])
@jwt_required()
def update_integration(integration_id):
    """Update integration"""
    try:
        integration = Integration.query.get_or_404(integration_id)
        data = request.json
        
        if data.get('name'):
            integration.name = data['name']
        if data.get('description'):
            integration.description = data['description']
        if data.get('integration_type'):
            integration.integration_type = data['integration_type']
        if data.get('endpoint_url'):
            integration.endpoint_url = data['endpoint_url']
        if data.get('api_key'):
            integration.api_key = data['api_key']
        if 'is_active' in data:
            integration.is_active = data['is_active']
        
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'data': integration.to_dict()
        }), 200
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating integration: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/integrations/<integration_id>', methods=['DELETE'])
@jwt_required()
def delete_integration(integration_id):
    """Delete integration"""
    try:
        integration = Integration.query.get_or_404(integration_id)
        db.session.delete(integration)
        db.session.commit()
        
        return jsonify({
            'status': 'success',
            'message': 'Integration deleted successfully'
        }), 200
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting integration: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_bp.route('/connectors', methods=['GET'])
@jwt_required()
def list_connectors():
    """List available connectors"""
    return jsonify({
        'connectors': [
            'messaging',
            'custom',
            'erp',
            'banking',
            'crm',
            'project_management'
        ]
    }), 200

@api_bp.route('/connectors/<connector_type>/test', methods=['POST'])
@jwt_required()
def test_connector(connector_type):
    """Test connector connection"""
    try:
        # Mock implementation
        return jsonify({
            'status': 'success',
            'connector_type': connector_type,
            'message': f'{connector_type} connector test successful'
        }), 200
    except Exception as e:
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
        
        # Mock implementation
        return jsonify({
            'status': 'success',
            'data_type': data_type,
            'count': 0,
            'data': []
        }), 200
    except Exception as e:
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
        
        # Mock implementation
        return jsonify({
            'status': 'success',
            'data_type': data_type,
            'message': 'Data sent successfully'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
