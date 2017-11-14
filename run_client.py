from client import Client
import logging
import sys

serverAddress = 'localhost'
serverPort = 12345

if __name__ == '__main__':

    client = Client()

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


    client.connect_to_server(serverAddress, serverPort)