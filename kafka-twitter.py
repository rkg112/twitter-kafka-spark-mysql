from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "793637251083341825-fJEJfYSvsqAJUKpkO53n6NJN6UffSkK"
access_token_secret =  "i3UYgdYDRBkODmGzzHcWVJvkjpoWqlWNVdv0ft5ye0Qyc"
consumer_key =  "WeINxojRjcIkM6hQ8AyY0ANNA"
consumer_secret =  "8mtT7MOZdMC8JkkcZ6u0SZIQ21BGoHBPwR6UhgkcA1qPSR6TJY"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("test-new-topic", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["JustinBieber"], languages=["en"])
