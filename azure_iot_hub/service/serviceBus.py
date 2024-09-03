from azure.servicebus import ServiceBusClient
from azure.servicebus import ServiceBusMessage
import os
from dotenv import load_dotenv

def send_message(CONNECTION_STR, QUEUE_NAME):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)
    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    message = ServiceBusMessage("Temperatura maxima interna excedida")
    sender.send_messages(message)

    sender.close()
    servicebus_client.close()

    return "sucess"


def receiver_message(CONNECTION_STR, QUEUE_NAME):
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)
    receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
    retorno = None

    with receiver:
        for message in receiver:
            retorno = message
            print(message)
            receiver.complete_message(message)
            break

    servicebus_client.close()

    return retorno

if __name__ == '__main__':
    
    load_dotenv()

    connection_string = os.environ.get('CONNECTION_STR')
    queue_name = os.environ.get('QUEUE_NAME')
    
    resultado = send_message(connection_string, queue_name)
    print(resultado)

    print(receiver_message(connection_string, queue_name))