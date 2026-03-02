"""
Kafka Integration for Distributed Queue
Replaces Redis with Kafka for enterprise-grade event streaming
"""
import os
import json
import logging
import asyncio
import socket
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from uuid import UUID
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Kafka configuration for migration platform."""
    
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    
    # Topics
    TOPIC_JOBS = os.getenv("KAFKA_TOPIC_JOBS", "migration-jobs")
    TOPIC_CHUNKS = os.getenv("KAFKA_TOPIC_CHUNKS", "chunk-tasks")
    TOPIC_STATUS = os.getenv("KAFKA_TOPIC_STATUS", "job-status-updates")
    TOPIC_USAGE = os.getenv("KAFKA_TOPIC_USAGE", "usage-events")
    
    # Consumer Group
    CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "migration-workers")
    
    # Performance Settings
    NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "10"))
    REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    
    # Producer Settings
    PRODUCER_CONFIG = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'migration-producer',
        'acks': 'all',  # Wait for all replicas
        'retries': 3,
        'max.in.flight.requests.per.connection': 5,
        'enable.idempotence': True,  # Exactly-once semantics
        'compression.type': 'snappy',
        'linger.ms': 10,
        'batch.size': 32768,
    }
    
    # Consumer Settings
    CONSUMER_CONFIG = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # Manual commit for reliability
        'max.poll.interval.ms': 300000,  # 5 minutes
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 3000,
        'max.partition.fetch.bytes': 10485760,  # 10MB
    }


class KafkaProducerService:
    """High-throughput Kafka producer for job distribution."""
    
    def __init__(self):
        self.producer = Producer(KafkaConfig.PRODUCER_CONFIG)
        self.pending = 0
        logger.info(f"Kafka Producer initialized: {KafkaConfig.BOOTSTRAP_SERVERS}")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery confirmation."""
        self.pending -= 1
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
    
    def produce_job(self, job_id: str, tenant_id: str, job_data: Dict[str, Any]):
        """
        Produce a migration job to Kafka.
        Partitioned by tenant_id for isolation and ordering.
        """
        try:
            message = {
                'job_id': job_id,
                'tenant_id': tenant_id,
                'data': job_data,
                'timestamp': datetime.utcnow().isoformat(),
                'producer': 'control-plane'
            }
            
            self.producer.produce(
                topic=KafkaConfig.TOPIC_JOBS,
                key=tenant_id.encode('utf-8'),  # Partition by tenant
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            
            self.pending += 1
            self.producer.poll(0)  # Trigger callbacks
            
            logger.info(f"Produced job {job_id} for tenant {tenant_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce job {job_id}: {e}")
            return False
    
    def produce_chunk(self, chunk_id: str, job_id: str, tenant_id: str, chunk_data: Dict[str, Any]):
        """Produce a chunk task to Kafka."""
        try:
            message = {
                'chunk_id': chunk_id,
                'job_id': job_id,
                'tenant_id': tenant_id,
                'data': chunk_data,
                'timestamp': datetime.utcnow().isoformat(),
                'retry_count': 0
            }
            
            self.producer.produce(
                topic=KafkaConfig.TOPIC_CHUNKS,
                key=tenant_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            
            self.pending += 1
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce chunk {chunk_id}: {e}")
            return False
    
    def produce_status_update(self, job_id: str, tenant_id: str, status: str, metadata: Dict[str, Any] = None):
        """Produce job status update."""
        try:
            message = {
                'job_id': job_id,
                'tenant_id': tenant_id,
                'status': status,
                'metadata': metadata or {},
                'timestamp': datetime.utcnow().isoformat()
            }
            
            self.producer.produce(
                topic=KafkaConfig.TOPIC_STATUS,
                key=job_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            
            self.pending += 1
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce status update for {job_id}: {e}")
            return False
    
    def produce_usage_event(self, tenant_id: str, event_type: str, metrics: Dict[str, Any]):
        """Produce usage tracking event."""
        try:
            message = {
                'tenant_id': tenant_id,
                'event_type': event_type,
                'metrics': metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            self.producer.produce(
                topic=KafkaConfig.TOPIC_USAGE,
                key=tenant_id.encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=self.delivery_report
            )
            
            self.pending += 1
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce usage event: {e}")
            return False
    
    def flush(self, timeout: float = 10.0):
        """Flush all pending messages."""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        return remaining == 0
    
    def close(self):
        """Close producer and flush pending messages."""
        logger.info("Closing Kafka producer...")
        self.flush()
        self.producer = None


class KafkaConsumerService:
    """Kafka consumer for processing migration tasks."""
    
    def __init__(self, worker_id: str, topics: List[str]):
        self.worker_id = worker_id
        self.topics = topics
        
        config = KafkaConfig.CONSUMER_CONFIG.copy()
        config['client.id'] = f'worker-{worker_id}'
        
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
        self.running = False
        
        logger.info(f"Kafka Consumer '{worker_id}' subscribed to: {topics}")
    
    async def consume(self, handler: Callable, timeout: float = 1.0):
        """
        Consume messages and process with handler.
        
        Args:
            handler: Async function that processes messages
            timeout: Poll timeout in seconds
        """
        self.running = True
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Decode message
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    logger.info(f"Processing message from {msg.topic()} [partition {msg.partition()}]")
                    
                    # Process message
                    success = await handler(msg.topic(), key, value)
                    
                    if success:
                        # Manual commit after successful processing
                        self.consumer.commit(message=msg, asynchronous=False)
                        logger.debug(f"Committed offset {msg.offset()}")
                    else:
                        logger.warning(f"Message processing failed, will retry")
                        # Don't commit - message will be reprocessed
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    # Commit bad message to skip it
                    self.consumer.commit(message=msg, asynchronous=False)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Don't commit - message will be retried
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.close()
    
    def stop(self):
        """Stop consuming messages."""
        self.running = False
    
    def close(self):
        """Close consumer and commit offsets."""
        logger.info(f"Closing Kafka consumer '{self.worker_id}'...")
        if self.consumer:
            self.consumer.close()
            self.consumer = None


class KafkaAdmin:
    """Kafka admin operations for topic management."""
    
    def __init__(self):
        self.admin = AdminClient({'bootstrap.servers': KafkaConfig.BOOTSTRAP_SERVERS})
        logger.info("Kafka Admin client initialized")
    
    def create_topics(self):
        """Create all required topics if they don't exist."""
        topics = [
            NewTopic(
                KafkaConfig.TOPIC_JOBS,
                num_partitions=KafkaConfig.NUM_PARTITIONS,
                replication_factor=KafkaConfig.REPLICATION_FACTOR,
                config={
                    'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                    'compression.type': 'snappy',
                    'max.message.bytes': '10485760'  # 10MB
                }
            ),
            NewTopic(
                KafkaConfig.TOPIC_CHUNKS,
                num_partitions=KafkaConfig.NUM_PARTITIONS * 2,  # More partitions for chunks
                replication_factor=KafkaConfig.REPLICATION_FACTOR,
                config={
                    'retention.ms': str(3 * 24 * 60 * 60 * 1000),  # 3 days
                    'compression.type': 'snappy'
                }
            ),
            NewTopic(
                KafkaConfig.TOPIC_STATUS,
                num_partitions=KafkaConfig.NUM_PARTITIONS,
                replication_factor=KafkaConfig.REPLICATION_FACTOR,
                config={
                    'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                    'cleanup.policy': 'compact'  # Keep latest status per job
                }
            ),
            NewTopic(
                KafkaConfig.TOPIC_USAGE,
                num_partitions=KafkaConfig.NUM_PARTITIONS,
                replication_factor=KafkaConfig.REPLICATION_FACTOR,
                config={
                    'retention.ms': str(90 * 24 * 60 * 60 * 1000),  # 90 days
                    'compression.type': 'snappy'
                }
            ),
        ]
        
        futures = self.admin.create_topics(topics)
        
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic '{topic}' created successfully")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info(f"Topic '{topic}' already exists")
                else:
                    logger.error(f"Failed to create topic '{topic}': {e}")
    
    def list_topics(self):
        """List all topics."""
        metadata = self.admin.list_topics(timeout=10)
        return list(metadata.topics.keys())
    
    def get_consumer_lag(self, group_id: str = None):
        """Get consumer lag metrics."""
        # This would typically query Kafka metrics
        # Implementation depends on monitoring setup
        pass


# Global instances
_producer = None
_admin = None


def get_kafka_producer() -> Optional[KafkaProducerService]:
    """Get global Kafka producer instance."""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducerService()
        except Exception as e:
            logger.warning(f"Kafka producer not available: {e}")
            return None
    return _producer


def get_kafka_admin() -> KafkaAdmin:
    """Get global Kafka admin instance."""
    global _admin
    if _admin is None:
        _admin = KafkaAdmin()
    return _admin


def initialize_kafka():
    """Initialize Kafka topics and infrastructure."""
    try:
        # Check if Kafka is available
        import socket
        host, port = KafkaConfig.BOOTSTRAP_SERVERS.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        
        if result != 0:
            logger.warning(f"Kafka not available at {KafkaConfig.BOOTSTRAP_SERVERS} (this is OK for local development)")
            return False
        
        admin = get_kafka_admin()
        admin.create_topics()
        logger.info("Kafka infrastructure initialized successfully")
        return True
    except Exception as e:
        logger.warning(f"Kafka initialization skipped: {e} (this is OK for local development)")
        return False
