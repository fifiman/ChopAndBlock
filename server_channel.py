from channel_protocol import ChannelProtocol
import marshal
import logging


class ServerChannel(ChannelProtocol):
    def __init__(self, connection, map, parentServer):

        ChannelProtocol.__init__(self, connection, map)
        self.parentServer = parentServer

        self.send_mapreduce_functions()
        self.start_new_task()

    def start_new_task(self):
        command, data = self.parentServer.task_manager.get_next_task()

        if command is not None:
            self.send_command(command, data)

    def process_command(self, command, data=None):
        commands = {"map_done" : self.map_done,
                    "reduce_done" : self.reduce_done
        }

        if command in commands:
            commands[command](command, data)
        else:
            ChannelProtocol.process_command(self, command, data)

    def map_done(self, command, data):
        self.parentServer.task_manager.map_done(data)
        self.start_new_task()

    def reduce_done(self, command, data):
        self.parentServer.task_manager.reduce_done(data)
        self.start_new_task()

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
