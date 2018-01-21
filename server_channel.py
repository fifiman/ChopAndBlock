from channel_protocol import ChannelProtocol
import marshal
import logging


class ServerChannel(ChannelProtocol):
    """
    Server channels are created by the server to communicate with clients. Server channels send the initial map reduce
    functions to the client, new tasks to be processed by clients, and receive processed data from clients to send
    back to the server.

    Attributes:
        parent_server (Server): Instance of parent server which created this server channel.
    """
    def __init__(self, connection, map, parent_server):
        """
        Initialize server channel and it's base class. Map reduce functions are immediately sent to the client,
        followed by the first map reduce task.

        Args:
            connection (Socket): Client connection.
            map ([Socket]): Socket map to which current server channel should be added to.
            parent_server (Server): Parent server which create current server channel instances.
        """
        ChannelProtocol.__init__(self, connection, map)
        self.parentServer = parent_server

        self.send_mapreduce_functions()
        self.start_new_task()

    def start_new_task(self):
        """
        Get new task from server and send to client.

        Returns:
            None
        """
        command, data = self.parentServer.task_manager.get_next_task()

        if command is not None:
            self.send_command(command, data)

    def process_command(self, command, data=None):
        """
        Process given command with according optional data.

        ServerChannel overrides this class from it's parent with more possible commands. In case ServerChannel does not
        find the command it is sent to be handled by parent class ChannelProtocol.

        Args:
            command (str): Command to process.
            data (None/str): Data to process with command, if exists.

        Returns:
            None or NotImplementedError if command does not exist in both client and channel protocol functions.
        """
        commands = {"map_done": self.map_done,
                    "reduce_done": self.reduce_done
                    }

        if command in commands:
            commands[command](command, data)
        else:
            ChannelProtocol.process_command(self, command, data)

    def map_done(self, command, data):
        """
        Send finished map task data back to parent server. Immediately send client a new task.

        Args:
            command (str): Command to be processed, in this case is "map_done".
            data: Map task data to be sent to parent server.

        Returns:
            None
        """
        self.parentServer.task_manager.map_done(data)
        self.start_new_task()

    def reduce_done(self, command, data):
        """
        Send finished reduce task data back to parent server. Immediately send client a new task.

        Args:
            command (str): Command to be processed, in this case is "reduce_done".
            data: Reduce task data to be sent to parent server.

        Returns:
            None
        """
        self.parentServer.task_manager.reduce_done(data)
        self.start_new_task()

    def send_mapreduce_functions(self):
        """
        Send map reduce functions to clients. Functions are dumped to a binary format and sent as a command with data.

        Returns:
             None
        """
        commands_and_functions = [
            ("set_map", self.parentServer.map),
            ("set_reduce", self.parentServer.reduce),
            ("set_collect", self.parentServer.collect)
        ]

        for command, func in commands_and_functions:
            if func is not None:  # Collect function does not have to exist.
                self.send_function(command, func)

    def send_function(self, command, func):
        """
        Given command and function, send command along with function in binary format to the client.

        Args:
            command (str): Command to be processed by client.
            func (Function): Function to be sent to client in binary format.

        Returns:
            None
        """
        func_code = marshal.dumps(func.func_code)
        self.send_command(command, func_code)

    def handle_close(self):
        """
        Close ServerChannel and log it.

        Returns:
            None
        """
        logging.debug("Server channel closing.")
        self.close()
