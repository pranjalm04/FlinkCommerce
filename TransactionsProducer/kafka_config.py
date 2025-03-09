from confluent_kafka import Producer
from logger import LoggerConfig

class KafkaProducerSingleton:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    def _initialize(self):
        self.logger = LoggerConfig(name="FlinkCommerce",log_file="C:\\Users\\pranj\\Desktop\\PycharmProjects\\FlinkCommerce\\logs\\Kafkalogs.log").logger
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'acks': 'all',
            'retries': 3,
            'linger.ms': 5,
        }
        self.producer = Producer(**self.config)

    def _on_send(self, err, record_metadata):
        if err:
            self.logger.warn(f"Failed to send message: {err}")
        else:
            self.logger.info(f"Message sent successfully to {record_metadata.topic} partition "
                             f"{record_metadata.partition} at offset {record_metadata.offset}")

    def send(self, topic, key, value):
        try:
            self.producer.produce(topic=topic, key=key, value=value, callback=self._on_send)
            self.producer.poll(1)
        except Exception as e:
            self.logger.error(f"Exception while sending message: {e}")

    def close(self):
        self.logger.info("Flushing the messages and closing the connection")
        self.producer.flush()

# Usage Example

