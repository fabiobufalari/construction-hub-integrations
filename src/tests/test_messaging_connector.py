"""
Unit tests for Messaging Connectors
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from src.connectors.messaging_connector import (
    MessageConnector, 
    KafkaConnector, 
    RabbitMQConnector, 
    ActiveMQConnector
)

class TestMessageConnector:
    """Test base MessageConnector class"""
    
    def test_init(self):
        """Test MessageConnector initialization"""
        config = {'host': 'localhost', 'port': 9092, 'messaging_type': 'kafka'}
        connector = MessageConnector(config)
        
        assert connector.config == config
        assert connector.client is None
        assert connector.message_handlers == {}
        assert not connector.is_connected
    
    def test_register_message_handler(self):
        """Test message handler registration"""
        config = {'host': 'localhost', 'port': 9092, 'messaging_type': 'kafka'}
        connector = MessageConnector(config)
        
        def test_handler(message):
            return message
        
        connector.register_message_handler('test-topic', test_handler)
        assert 'test-topic' in connector.message_handlers
        assert connector.message_handlers['test-topic'] == test_handler

class TestKafkaConnector:
    """Test KafkaConnector implementation"""
    
    @pytest.fixture
    def kafka_config(self):
        return {
            'host': 'localhost',
            'port': 9092,
            'messaging_type': 'kafka',
            'consumer_group': 'test-group'
        }
    
    @pytest.fixture
    def kafka_connector(self, kafka_config):
        return KafkaConnector(kafka_config)
    
    @patch('src.connectors.messaging_connector.KafkaProducer')
    @patch('src.connectors.messaging_connector.KafkaConsumer')
    def test_connect_success(self, mock_consumer, mock_producer, kafka_connector):
        """Test successful Kafka connection"""
        mock_producer.return_value = Mock()
        mock_consumer.return_value = Mock()
        
        result = kafka_connector.connect()
        
        assert result is True
        assert kafka_connector.is_connected is True
        assert kafka_connector.producer is not None
        assert kafka_connector.consumer is not None
    
    @patch('src.connectors.messaging_connector.KafkaProducer')
    def test_connect_failure(self, mock_producer, kafka_connector):
        """Test Kafka connection failure"""
        mock_producer.side_effect = Exception("Connection failed")
        
        result = kafka_connector.connect()
        
        assert result is False
        assert kafka_connector.is_connected is False
    
    def test_disconnect(self, kafka_connector):
        """Test Kafka disconnection"""
        kafka_connector.producer = Mock()
        kafka_connector.consumer = Mock()
        kafka_connector.is_connected = True
        
        result = kafka_connector.disconnect()
        
        assert result is True
        assert kafka_connector.is_connected is False
        kafka_connector.producer.close.assert_called_once()
        kafka_connector.consumer.close.assert_called_once()
    
    @patch('src.connectors.messaging_connector.KafkaProducer')
    @patch('src.connectors.messaging_connector.KafkaConsumer')
    def test_test_connection(self, mock_consumer, mock_producer, kafka_connector):
        """Test Kafka connection test"""
        mock_producer_instance = Mock()
        mock_producer_instance.partitions_for.return_value = {'test-topic': [0, 1]}
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = Mock()
        
        result = kafka_connector.test_connection()
        
        assert result['status'] == 'success'
        assert 'Kafka connection test successful' in result['message']
    
    @patch('src.connectors.messaging_connector.KafkaProducer')
    @patch('src.connectors.messaging_connector.KafkaConsumer')
    def test_send_data(self, mock_consumer, mock_producer, kafka_connector):
        """Test sending data to Kafka"""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = 'test-topic'
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 123
        mock_future.get.return_value = mock_record_metadata
        mock_producer_instance.send.return_value = mock_future
        mock_producer.return_value = mock_producer_instance
        mock_consumer.return_value = Mock()
        
        kafka_connector.connect()
        
        data = {
            'topic': 'test-topic',
            'message': {'test': 'data'},
            'key': 'test-key'
        }
        
        result = kafka_connector.send_data(data, 'test-type')
        
        assert result['status'] == 'success'
        assert result['details']['topic'] == 'test-topic'
        assert result['details']['partition'] == 0
        assert result['details']['offset'] == 123
    
    @patch('src.connectors.messaging_connector.KafkaProducer')
    @patch('src.connectors.messaging_connector.KafkaConsumer')
    def test_sync_data(self, mock_consumer, mock_producer, kafka_connector):
        """Test syncing data from Kafka"""
        mock_producer.return_value = Mock()
        
        # Mock consumer
        mock_consumer_instance = Mock()
        mock_message = Mock()
        mock_message.topic = 'test-topic'
        mock_message.partition = 0
        mock_message.offset = 123
        mock_message.key = 'test-key'
        mock_message.value = {'test': 'data'}
        mock_message.timestamp = 1234567890
        
        mock_consumer_instance.poll.return_value = {
            'test-topic-0': [mock_message]
        }
        mock_consumer.return_value = mock_consumer_instance
        
        kafka_connector.connect()
        
        result = kafka_connector.sync_data('test-topic', {'topic': 'test-topic'})
        
        assert result['status'] == 'success'
        assert len(result['data']) == 1
        assert result['data'][0]['topic'] == 'test-topic'
        assert result['data'][0]['value'] == {'test': 'data'}

class TestRabbitMQConnector:
    """Test RabbitMQConnector implementation"""
    
    @pytest.fixture
    def rabbitmq_config(self):
        return {
            'host': 'localhost',
            'port': 5672,
            'messaging_type': 'rabbitmq',
            'username': 'guest',
            'password': 'guest'
        }
    
    @pytest.fixture
    def rabbitmq_connector(self, rabbitmq_config):
        return RabbitMQConnector(rabbitmq_config)
    
    @patch('src.connectors.messaging_connector.pika')
    def test_connect_success(self, mock_pika, rabbitmq_connector):
        """Test successful RabbitMQ connection"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.BlockingConnection.return_value = mock_connection
        
        result = rabbitmq_connector.connect()
        
        assert result is True
        assert rabbitmq_connector.is_connected is True
        assert rabbitmq_connector.connection is not None
        assert rabbitmq_connector.channel is not None
    
    @patch('src.connectors.messaging_connector.pika')
    def test_connect_failure(self, mock_pika, rabbitmq_connector):
        """Test RabbitMQ connection failure"""
        mock_pika.BlockingConnection.side_effect = Exception("Connection failed")
        
        result = rabbitmq_connector.connect()
        
        assert result is False
        assert rabbitmq_connector.is_connected is False
    
    def test_disconnect(self, rabbitmq_connector):
        """Test RabbitMQ disconnection"""
        mock_connection = Mock()
        mock_connection.is_closed = False
        rabbitmq_connector.connection = mock_connection
        rabbitmq_connector.is_connected = True
        
        result = rabbitmq_connector.disconnect()
        
        assert result is True
        assert rabbitmq_connector.is_connected is False
        mock_connection.close.assert_called_once()
    
    @patch('src.connectors.messaging_connector.pika')
    def test_send_data(self, mock_pika, rabbitmq_connector):
        """Test sending data to RabbitMQ"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.BlockingConnection.return_value = mock_connection
        
        rabbitmq_connector.connect()
        
        data = {
            'queue': 'test-queue',
            'message': {'test': 'data'},
            'exchange': '',
            'routing_key': 'test-queue'
        }
        
        result = rabbitmq_connector.send_data(data, 'test-type')
        
        assert result['status'] == 'success'
        assert result['details']['queue'] == 'test-queue'
        mock_channel.queue_declare.assert_called_once()
        mock_channel.basic_publish.assert_called_once()
    
    @patch('src.connectors.messaging_connector.pika')
    def test_sync_data(self, mock_pika, rabbitmq_connector):
        """Test syncing data from RabbitMQ"""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.BlockingConnection.return_value = mock_connection
        
        # Mock message
        mock_method_frame = Mock()
        mock_method_frame.delivery_tag = 1
        mock_method_frame.exchange = ''
        mock_method_frame.routing_key = 'test-queue'
        
        mock_header_frame = Mock()
        mock_body = json.dumps({'test': 'data'}).encode('utf-8')
        
        mock_channel.basic_get.side_effect = [
            (mock_method_frame, mock_header_frame, mock_body),
            (None, None, None)  # No more messages
        ]
        
        rabbitmq_connector.connect()
        
        result = rabbitmq_connector.sync_data('test-queue', {'queue': 'test-queue'})
        
        assert result['status'] == 'success'
        assert len(result['data']) == 1
        assert result['data'][0]['message'] == {'test': 'data'}

class TestActiveMQConnector:
    """Test ActiveMQConnector implementation"""
    
    @pytest.fixture
    def activemq_config(self):
        return {
            'host': 'localhost',
            'port': 61613,
            'messaging_type': 'activemq',
            'username': 'admin',
            'password': 'admin'
        }
    
    @pytest.fixture
    def activemq_connector(self, activemq_config):
        return ActiveMQConnector(activemq_config)
    
    @patch('src.connectors.messaging_connector.stomp')
    def test_connect_success(self, mock_stomp, activemq_connector):
        """Test successful ActiveMQ connection"""
        mock_connection = Mock()
        mock_stomp.Connection.return_value = mock_connection
        
        result = activemq_connector.connect()
        
        assert result is True
        assert activemq_connector.is_connected is True
        assert activemq_connector.connection is not None
        mock_connection.connect.assert_called_once()
    
    @patch('src.connectors.messaging_connector.stomp')
    def test_connect_failure(self, mock_stomp, activemq_connector):
        """Test ActiveMQ connection failure"""
        mock_stomp.Connection.side_effect = Exception("Connection failed")
        
        result = activemq_connector.connect()
        
        assert result is False
        assert activemq_connector.is_connected is False
    
    def test_disconnect(self, activemq_connector):
        """Test ActiveMQ disconnection"""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        activemq_connector.connection = mock_connection
        activemq_connector.is_connected = True
        
        result = activemq_connector.disconnect()
        
        assert result is True
        assert activemq_connector.is_connected is False
        mock_connection.disconnect.assert_called_once()
    
    @patch('src.connectors.messaging_connector.stomp')
    def test_send_data(self, mock_stomp, activemq_connector):
        """Test sending data to ActiveMQ"""
        mock_connection = Mock()
        mock_stomp.Connection.return_value = mock_connection
        
        activemq_connector.connect()
        
        data = {
            'destination': '/queue/test-queue',
            'message': {'test': 'data'},
            'headers': {'content-type': 'application/json'}
        }
        
        result = activemq_connector.send_data(data, 'test-type')
        
        assert result['status'] == 'success'
        assert result['details']['destination'] == '/queue/test-queue'
        mock_connection.send.assert_called_once()

if __name__ == '__main__':
    pytest.main([__file__])

