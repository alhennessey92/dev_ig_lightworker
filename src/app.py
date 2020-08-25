
import time
import sys
import traceback
import logging

from trading_ig import IGService, IGStreamService
from trading_ig.config import config
from trading_ig.lightstreamer import Subscription


from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib


epic = "CHART:CS.D.EURUSD.MINI.IP:1MINUTE"

# HANDLE ALL KAFKA PRODUCTION INFO

# Read arguments and configurations and initialize
# args = ccloud_lib.parse_args()
config_file = 'librdkafka.config'
topic = "eurusd1minute"
conf = ccloud_lib.read_ccloud_config(config_file)

# Create Producer instance
producer = Producer({
    'bootstrap.servers': conf['bootstrap.servers'],
    'sasl.mechanisms': conf['sasl.mechanisms'],
    'security.protocol': conf['security.protocol'],
    'sasl.username': conf['sasl.username'],
    'sasl.password': conf['sasl.password'],
})

# Create topic if needed
ccloud_lib.create_topic(conf, topic)

delivered_records = 0

#///////////////////////////////////////////////


# A simple function acting as a Subscription listener
def on_prices_update(item_update):
    # print("price: %s " % item_update)
    
    candle_open = "{OFR_OPEN:<5}".format(**item_update["values"])
    candle_close = "{OFR_CLOSE:<5}".format(**item_update["values"])
    candle_high = "{OFR_HIGH:<5}".format(**item_update["values"])
    candle_low = "{OFR_LOW:<5}".format(**item_update["values"])
    candle_end = "{CONS_END:<1}".format(**item_update["values"])
    name = "{stock_name:<19}".format(stock_name=item_update["name"])
    candle_utm = "{UTM:<10}".format(**item_update["values"])
    record_key = "{UTM:<10}".format(**item_update["values"])
    new_candle_end = int(candle_end)
    if new_candle_end==1:

        s, ms = divmod(int(candle_utm), 1000)
        new_time = '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)
        print(new_time)

        print(
            "{stock_name:<19}: Time %s - " 
            "High {OFR_HIGH:>5} - Open {OFR_OPEN:>5} - Close {OFR_CLOSE:>5} - Low {OFR_LOW:>5} - End {CONS_END:>2}".format(
                stock_name=item_update["name"], **item_update["values"]
            ) % new_time
        )
        def acked(err, msg):
            global delivered_records
            """Delivery report handler called on
            successful or failed delivery of message
            """
            if err is not None:
                print("Failed to deliver message: {}".format(err))
            else:
                delivered_records += 1
                print("Produced record to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))

        
        record_value = json.dumps({'candle_open': candle_open, 'candle_close': candle_close, 'candle_high': candle_high, 'candle_low': candle_low, 'Name': name, 'candle_end': candle_end})
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)

    


def on_heartbeat_update(item_update):
    print(item_update["values"])

def on_account_update(balance_update):
    print("balance: %s " % balance_update)


def main():
    logging.basicConfig(level=logging.INFO)
    # logging.basicConfig(level=logging.DEBUG)

    ig_service = IGService(
        config.username, config.password, config.api_key, config.acc_type
    )

    ig_stream_service = IGStreamService(ig_service)
    ig_session = ig_stream_service.create_session()
    # Ensure configured account is selected
    accounts = ig_session[u"accounts"]
    for account in accounts:
        if account[u"accountId"] == config.acc_number:
            accountId = account[u"accountId"]
            break
        else:
            print("Account not found: {0}".format(config.acc_number))
            accountId = None
    ig_stream_service.connect(accountId)

    # Making a new Subscription in MERGE mode
    subscription_prices = Subscription(
        mode="MERGE",
        items=["CHART:CS.D.EURUSD.MINI.IP:1MINUTE"],
        fields=["UTM", "OFR_OPEN", "OFR_CLOSE", "OFR_HIGH", "OFR_LOW", "CONS_END"],
    )
    # adapter="QUOTE_ADAPTER")

    # Adding the "on_price_update" function to Subscription
    subscription_prices.addlistener(on_prices_update)

    # Registering the Subscription
    sub_key_prices = ig_stream_service.ls_client.subscribe(subscription_prices)

    # Making an other Subscription in MERGE mode
    subscription_account = Subscription(
        mode="MERGE", items=["ACCOUNT:" + accountId], fields=["AVAILABLE_CASH"],
    )
    #    #adapter="QUOTE_ADAPTER")

    # Adding the "on_balance_update" function to Subscription
    subscription_account.addlistener(on_account_update)

    # Registering the Subscription
    sub_key_account = ig_stream_service.ls_client.subscribe(subscription_account)






    heartbeat_items = ["TRADE:HB.U.HEARTBEAT.IP"]
    heartbeat = Subscription(
        mode='MERGE',
        items=heartbeat_items,
        fields=["HEARTBEAT"],
    )

    heartbeat.addlistener(on_heartbeat_update)
    sub_heartbeat = ig_stream_service.ls_client.subscribe(heartbeat)




    input(
        "{0:-^80}\n".format(
            "HIT CR TO UNSUBSCRIBE AND DISCONNECT FROM \
    LIGHTSTREAMER"
        )
    )

    # Disconnecting
    ig_stream_service.disconnect()
    producer.flush()


if __name__ == "__main__":
    main()