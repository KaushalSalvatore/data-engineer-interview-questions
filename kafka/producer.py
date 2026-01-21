from kafka import KafkaProducer 
 
producer = KafkaProducer( 
    bootstrap_servers=" host.docker.internal:29092", 
    acks="all", 
    retries=5, 
    enable_idempotence=True 
)
 
producer.send("orders", b"order_created") 
producer.flush() 
producer.close()