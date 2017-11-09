import asyncore
import marshal
import logging
import socket
import types

from channel_protocol import ChannelProtocol


class Client(ChannelProtocol):
    def __init__(self):
        ChannelProtocol.__init__(self)

        self.map = None
        self.reduce = None
        self.collect = None

    def connect_to_server(self, serverAddress, serverPort):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((serverAddress, serverPort))

        logging.info("Client connected to server at address %s:%s." % (serverAddress, serverPort))

        asyncore.loop()

    def process_command(self, command, data=None):
        logging.debug("Attempting to process command: %s." % command)

        commands = {
            "set_map" : self.set_map,
            "set_reduce" : self.set_reduce,
            "set_collect" : self.set_collect
        }

        if command in commands:
            commands[command](command, data)
        else:
            ChannelProtocol.process_command(command, data)

    def set_map(self, command, data):
        map_code = marshal.loads(data)
        self.map = types.FunctionType(map_code, globals(), "map")

        logging.debug("Client map function set.")

    def set_reduce(self, command, data):
        reduce_code = marshal.loads(data)
        self.reduce = types.FunctionType(reduce_code, globals(), "reduce")

        logging.debug("Client reduce function set.")

    def set_collect(self, command, data):
        collect_code = marshal.loads(data)
        self.collect = types.FunctionType(collect_code, globals(), "collect")

        logging.debug("Client collect function set.")

