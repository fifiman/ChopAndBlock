from channel_protocol import ChannelProtocol
import marshal
import logging


class ServerChannel(ChannelProtocol):
    def __init__(self, connection, map, parentServer):

        ChannelProtocol.__init__(self, connection, map)
        self.parentServer = parentServer

        self.send_mapreduce_functions()

    def send_mapreduce_functions(self):
        commands_and_functions = [
            ("set_map", self.parentServer.map),
            ("set_reduce", self.parentServer.reduce),
            ("set_collect", self.parentServer.collect)
        ]

        for command, function in commands_and_functions:
            if function is not None: # Collect function does not have to exist.
                self.send_function(command, function)

    def send_function(self, command, func):
        func_code = marshal.dumps(func.func_code)
        self.send_command(command, func_code)

    def handle_close(self):
        logging.debug("Server channel closing.")
        self.close()
