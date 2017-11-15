import asyncore
import logging
import marshal
import socket
import types

from channel_protocol import ChannelProtocol


class Client(ChannelProtocol):
    """
    Client connects to server. After which immediately receives MapReduce functions to be used for current job. After
    functions have been loaded, client will process multiple independent map/reduce jobs; sending results back
    to the server every time.

    Attributes:
        map_fn (func): Map function.
        collect_fn (func): Collect function.
        reduce_fn (func): Reduce function.
    """
    def __init__(self):
        """
        Initialize client and it's parent class.
        """
        ChannelProtocol.__init__(self)

        self.map_fn = None
        self.reduce_fn = None
        self.collect_fn = None

    def connect_to_server(self, serverAddress, serverPort):
        """
        Connect client to server at given address, and process commands while connection is active.

        Args:
            serverAddress (str): Server address.
            serverPort (int): Server port number.

        Returns:
            None
        """
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((serverAddress, serverPort))

        logging.info("Client connected to server at address %s:%s." % (serverAddress, serverPort))

        asyncore.loop()

    def process_command(self, command, data=None):
        """
        Process given command with according optional data.

        Client overrides this class from it's parent with more possible commands. In case client does not find
        command, command is sent to be handled by parent class ChannelProtocol.

        Args:
            command (str): Command to process.
            data (None/str): Data to process with command, if exists.

        Returns:
            None or NotImplementedError if command does not exist in both client and channel protocol functions.
        """
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
        """
        Map given data based on loaded map function and send results to server.

        Args:
            command (str): Command being processed, not relevant to current mapping process.
            data (str): Data being mapped.

        Returns:
            None
        """
        logging.debug("Mapping %s." % data[0])
        results = {}

        for k, v in self.map_fn(data[0], data[1]):
            if k not in results:
                results[k] = []

            results[k].append(v)

        self.send_command("map_done", (data[0], results))

    def reduce(self, command, data):
        """
        Reduce mapped data based on loaded reduce function, and send results to server.

        Args:
            command (str): Command being processed, not relevant to current reducing process.
            data (str): Data being reduced.

        Returns:
            None
        """
        logging.debug("Reducing %s." % data[0])
        results = self.reduce_fn(data[0], data[1])

        self.send_command("reduce_done", (data[0], results))

    def set_map(self, command, data):
        """
        Set map function to be used by client.

        Args:
            command (str): Command currently being process.
            data (str): Binary version of function code.

        Returns:
            None
        """
        map_code = marshal.loads(data)
        self.map_fn = types.FunctionType(map_code, globals(), "map")

        logging.debug("Client map function set.")

    def set_reduce(self, command, data):
        """
        Set reduce function to be used by client.

        Args:
            command (str): Command currently being process.
            data (str): Binary version of function code.

        Returns:
            None
        """
        reduce_code = marshal.loads(data)
        self.reduce_fn = types.FunctionType(reduce_code, globals(), "reduce")

        logging.debug("Client reduce function set.")

    def set_collect(self, command, data):
        """
        Set collect function to be used by client.

        Args:
            command (str): Command currently being process.
            data (str): Binary version of function code.

        Returns:
            None
        """
        collect_code = marshal.loads(data)
        self.collect_fn = types.FunctionType(collect_code, globals(), "collect")

        logging.debug("Client collect function set.")

    def handle_close(self):
        """
        Close client.

        Returns:
             None
        """
        logging.info("Client disconnecting.")
        self.close()

