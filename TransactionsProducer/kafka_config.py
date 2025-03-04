
from confluent_kafka import Producer
from logger import logger
class kafkaProducer:
    _instance=None
    config={
        'bootstrap.servers':'localhost:9092',
        'acks': 'all',
        'retries': 3,
        'linger.ms': 5,
    }
    def onSend(self,err,recordMetaData):
        if err:
            logger.warn(f" Failed to send message: {err}")
        else:
            logger.info(f"Message sent successfully to {recordMetaData.topic} partition \
                        {recordMetaData.partition} at offset {recordMetaData.offset}")


    @staticmethod
    def getProducer():
        if kafkaProducer._instance is None:
            kafkaProducer._instance= Producer(**kafkaProducer.config)
        return kafkaProducer._instance
    def send(self,topic,key,value):
        try:
            producer=kafkaProducer.getProducer()
            producer.produce(topic=topic,key=key,value=value,callback=self.onSend)
            producer.poll(1)
        except Exception as e:
            logger.error(f"Exception while sending message: {e}")

    def close(self):
        logger.info(f'flushing the messages and closing the connection')
        if kafkaProducer._instance is not None:
            kafkaProducer._instance.flush()

