from confluent_kafka import Consumer, KafkaError


class OrderConsumer:
    def __init__(self, bootstrap_servers, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def consume_messages(self, topic):
        # Set up consumer configuration
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }

        # Create a consumer instance
        consumer = Consumer(conf)

        # Subscribe to the topic
        consumer.subscribe([topic])

        # Poll for new messages
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                print('Received message: {}'.format(msg.value().decode('utf-8')))


if __name__ == '__main__':
    consumer = OrderConsumer('localhost:9092', 'order-consumer')
    consumer.consume_messages('order-test')
