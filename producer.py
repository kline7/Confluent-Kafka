from confluent_kafka import Producer


class OrderProducer:
    def __init__(self, bootstrap_servers, client_id):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id

    def delivery_report(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def produce_messages(self, topic, num_messages):
        # Set up producer configuration
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id
        }

        # Create a producer instance
        producer = Producer(conf)

        # Send messages to the topic
        for i in range(num_messages):
            value = "Order " + str(i)
            producer.produce(topic, value.encode('utf-8'), callback=self.delivery_report)

        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()


if __name__ == '__main__':
    producer = OrderProducer('localhost:9092', 'order-producer')
    producer.produce_messages('order-test', 10)
