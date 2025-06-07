"""
Messaging Connector
Handles integration with messaging systems (Kafka, RabbitMQ, ActiveMQ)
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)

class MessageConnector(BaseConnector):
    """
    Base class for messaging system connectors
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = None
        self.message_handlers = {}
        
    def register_message_handler(self, topic: str, handler_func):
        """
        Register a message handler for a specific topic
        
        Args:
            topic: Topic/queue name
            handler_func: Function to handle messages
        """
        self.message_handlers[topic] = handler_func
        
    def get_required_config_fields(self) -> List[str]:
        return ['host', 'port', 'messaging_type']

class KafkaConnector(MessageConnector):
    """
    Apache Kafka connector implementation
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.producer = None
        self.consumer = None
        
    def connect(self) -> bool:
        """Connect to Kafka cluster"""
        try:
            from kafka import KafkaProducer, KafkaConsumer
            
            bootstrap_servers = f"{self.config['host']}:{self.config['port']}"
            
            # Initialize producer
            self.producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            # Initialize consumer
            self.consumer = KafkaConsumer(
                bootstrap_servers=[bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id=self.config.get('consumer_group', 'construction-hub-integration')
            )
            
            self.is_connected = True
            self.log_operation('connect', 'success', 'Connected to Kafka cluster')
            return True
            
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to connect to Kafka: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from Kafka cluster"""
        try:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            
            self.is_connected = False
            self.log_operation('disconnect', 'success', 'Disconnected from Kafka cluster')
            return True
            
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect from Kafka: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test Kafka connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            # Test by getting cluster metadata
            metadata = self.producer.partitions_for('test-topic')
            
            return {
                'status': 'success',
                'message': 'Kafka connection test successful',
                'details': {
                    'connected': self.is_connected,
                    'cluster_metadata': str(metadata)
                }
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Kafka connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Consume messages from Kafka topics"""
        try:
            if not self.is_connected:
                self.connect()
            
            topic = filters.get('topic') if filters else data_type
            timeout_ms = filters.get('timeout', 5000) if filters else 5000
            
            # Subscribe to topic
            self.consumer.subscribe([topic])
            
            messages = []
            message_batch = self.consumer.poll(timeout_ms=timeout_ms)
            
            for topic_partition, msgs in message_batch.items():
                for message in msgs:
                    messages.append({
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'key': message.key,
                        'value': message.value,
                        'timestamp': message.timestamp
                    })
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', f'Consumed {len(messages)} messages from topic {topic}')
            
            return {
                'status': 'success',
                'data': messages,
                'count': len(messages),
                'topic': topic
            }
            
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Failed to consume messages: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send messages to Kafka topics"""
        try:
            if not self.is_connected:
                self.connect()
            
            topic = data.get('topic', data_type)
            message = data.get('message', data)
            key = data.get('key')
            
            # Send message
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            
            self.log_operation('send_data', 'success', f'Message sent to topic {topic}')
            
            return {
                'status': 'success',
                'message': 'Message sent successfully',
                'details': {
                    'topic': record_metadata.topic,
                    'partition': record_metadata.partition,
                    'offset': record_metadata.offset
                }
            }
            
        except Exception as e:
            self.log_operation('send_data', 'error', f'Failed to send message: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_required_config_fields(self) -> List[str]:
        return super().get_required_config_fields() + ['consumer_group']

class RabbitMQConnector(MessageConnector):
    """
    RabbitMQ connector implementation
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        self.channel = None
        
    def connect(self) -> bool:
        """Connect to RabbitMQ server"""
        try:
            import pika
            
            credentials = pika.PlainCredentials(
                self.config.get('username', 'guest'),
                self.config.get('password', 'guest')
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config['host'],
                port=self.config['port'],
                credentials=credentials,
                virtual_host=self.config.get('virtual_host', '/'),
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            self.is_connected = True
            self.log_operation('connect', 'success', 'Connected to RabbitMQ server')
            return True
            
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to connect to RabbitMQ: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from RabbitMQ server"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            
            self.is_connected = False
            self.log_operation('disconnect', 'success', 'Disconnected from RabbitMQ server')
            return True
            
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect from RabbitMQ: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test RabbitMQ connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            # Test by declaring a test queue
            self.channel.queue_declare(queue='test-queue', durable=False, auto_delete=True)
            
            return {
                'status': 'success',
                'message': 'RabbitMQ connection test successful',
                'details': {'connected': self.is_connected}
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'RabbitMQ connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """Consume messages from RabbitMQ queues"""
        try:
            if not self.is_connected:
                self.connect()
            
            queue = filters.get('queue') if filters else data_type
            max_messages = filters.get('max_messages', 10) if filters else 10
            
            # Declare queue if it doesn't exist
            self.channel.queue_declare(queue=queue, durable=True)
            
            messages = []
            for i in range(max_messages):
                method_frame, header_frame, body = self.channel.basic_get(queue=queue, auto_ack=True)
                
                if method_frame is None:
                    break  # No more messages
                
                try:
                    message_data = json.loads(body.decode('utf-8'))
                except json.JSONDecodeError:
                    message_data = body.decode('utf-8')
                
                messages.append({
                    'delivery_tag': method_frame.delivery_tag,
                    'exchange': method_frame.exchange,
                    'routing_key': method_frame.routing_key,
                    'message': message_data,
                    'timestamp': datetime.utcnow().isoformat()
                })
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', f'Consumed {len(messages)} messages from queue {queue}')
            
            return {
                'status': 'success',
                'data': messages,
                'count': len(messages),
                'queue': queue
            }
            
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Failed to consume messages: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send messages to RabbitMQ queues"""
        try:
            if not self.is_connected:
                self.connect()
            
            queue = data.get('queue', data_type)
            message = data.get('message', data)
            exchange = data.get('exchange', '')
            routing_key = data.get('routing_key', queue)
            
            # Declare queue if it doesn't exist
            self.channel.queue_declare(queue=queue, durable=True)
            
            # Send message
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
            )
            
            self.log_operation('send_data', 'success', f'Message sent to queue {queue}')
            
            return {
                'status': 'success',
                'message': 'Message sent successfully',
                'details': {
                    'queue': queue,
                    'exchange': exchange,
                    'routing_key': routing_key
                }
            }
            
        except Exception as e:
            self.log_operation('send_data', 'error', f'Failed to send message: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_required_config_fields(self) -> List[str]:
        return super().get_required_config_fields() + ['username', 'password']

class ActiveMQConnector(MessageConnector):
    """
    Apache ActiveMQ connector implementation
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection = None
        
    def connect(self) -> bool:
        """Connect to ActiveMQ server"""
        try:
            import stomp
            
            host_and_port = [(self.config['host'], self.config['port'])]
            
            self.connection = stomp.Connection(host_and_ports=host_and_port)
            
            if 'username' in self.config and 'password' in self.config:
                self.connection.connect(
                    username=self.config['username'],
                    passcode=self.config['password'],
                    wait=True
                )
            else:
                self.connection.connect(wait=True)
            
            self.is_connected = True
            self.log_operation('connect', 'success', 'Connected to ActiveMQ server')
            return True
            
        except Exception as e:
            self.log_operation('connect', 'error', f'Failed to connect to ActiveMQ: {str(e)}')
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from ActiveMQ server"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.disconnect()
            
            self.is_connected = False
            self.log_operation('disconnect', 'success', 'Disconnected from ActiveMQ server')
            return True
            
        except Exception as e:
            self.log_operation('disconnect', 'error', f'Failed to disconnect from ActiveMQ: {str(e)}')
            return False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test ActiveMQ connection"""
        try:
            if not self.is_connected:
                self.connect()
            
            return {
                'status': 'success',
                'message': 'ActiveMQ connection test successful',
                'details': {'connected': self.is_connected}
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'ActiveMQ connection test failed: {str(e)}',
                'details': {'connected': False}
            }
    
    def sync_data(self, data_type: str, filters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Note: ActiveMQ message consumption requires a listener pattern
        This is a simplified implementation for demonstration
        """
        try:
            if not self.is_connected:
                self.connect()
            
            # In a real implementation, you would set up message listeners
            # and store received messages in a queue or database
            
            self.last_sync = datetime.utcnow()
            self.log_operation('sync_data', 'success', 'ActiveMQ sync initiated (listener-based)')
            
            return {
                'status': 'success',
                'message': 'ActiveMQ sync requires listener setup',
                'data': [],
                'count': 0
            }
            
        except Exception as e:
            self.log_operation('sync_data', 'error', f'Failed to sync ActiveMQ messages: {str(e)}')
            return {
                'status': 'error',
                'message': str(e),
                'data': []
            }
    
    def send_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Send messages to ActiveMQ queues/topics"""
        try:
            if not self.is_connected:
                self.connect()
            
            destination = data.get('destination', f'/queue/{data_type}')
            message = data.get('message', data)
            headers = data.get('headers', {})
            
            # Send message
            self.connection.send(
                destination=destination,
                body=json.dumps(message),
                headers=headers
            )
            
            self.log_operation('send_data', 'success', f'Message sent to destination {destination}')
            
            return {
                'status': 'success',
                'message': 'Message sent successfully',
                'details': {
                    'destination': destination,
                    'headers': headers
                }
            }
            
        except Exception as e:
            self.log_operation('send_data', 'error', f'Failed to send message: {str(e)}')
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_required_config_fields(self) -> List[str]:
        return super().get_required_config_fields()

