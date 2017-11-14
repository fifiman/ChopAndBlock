import asyncore
import marshal
import logging
import socket
import types

from channel_protocol import ChannelProtocol


class Client(ChannelProtocol):
    def __init__(self):
        ChannelProtocol.__init__(self)

        self.map_fn = None
        self.reduce_fn = None
        self.collect_fn = None

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
            "set_collect" : self.set_collect,
            "map" : self.map,
            "reduce" : self.reduce
        }

        if command in commands:
            commands[command](command, data)
        else:
            ChannelProtocol.process_command(self, command, data)

    def map(self, command, data):
        logging.debug("Mapping %s." % data[0])
        results = {}

        for k, v in self.map_fn(data[0], data[1]):
            if k not in results:
                results[k] = []

            results[k].append(v)

        self.send_command("map_done", (data[0], results))

    def reduce(self, command, data):
        logging.debug("Reducing %s." % data[0])
        results = self.reduce_fn(data[0], data[1])

        self.send_command("reduce_done", (data[0], results))

    def set_map(self, command, data):
        map_code = marshal.loads(data)
        self.map_fn = types.FunctionType(map_code, globals(), "map")

        logging.debug("Client map function set.")

    def set_reduce(self, command, data):
        reduce_code = marshal.loads(data)
        self.reduce_fn = types.FunctionType(reduce_code, globals(), "reduce")

        logging.debug("Client reduce function set.")

    def set_collect(self, command, data):
        collect_code = marshal.loads(data)
        self.collect_fn = types.FunctionType(collect_code, globals(), "collect")

        logging.debug("Client collect function set.")

    def handle_close(self):
        logging.info("Client disconnecting.")
        self.close()

