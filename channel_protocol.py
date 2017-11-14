import asynchat
import cPickle as pickle
import logging


class ChannelProtocol(asynchat.async_chat):
    """
    ChannelProtocol implements underlying structure for client and server channels. In particular, handling of
    buffer terminator cases.

    Args:
        connection (socket): Connect to already existing socket.
        socket_map (map[socket]): Socket map to which to be added to, if exists.

    Attributes:
        buffer ([str]): Buffer containing blocks of incoming data.
        mid_command (str/None): Previous command while we are waiting on binary data associated to that command.
    """

    COMMAND_SEPARATOR = ":"
    DEFAULT_TERMINATOR = "\n"

    def __init__(self, connection=None, socket_map=None):

        asynchat.async_chat.__init__(self, sock=connection, map=socket_map)

        self.set_terminator(ChannelProtocol.DEFAULT_TERMINATOR)

        self.buffer = []
        self.mid_command = None

    def collect_incoming_data(self, data):
        self.buffer.append(data)

    def found_terminator(self):
        buffer_data = ''.join(self.buffer)
        self.buffer[:] = []

        if self.mid_command is not None:
            # Unpickle data and process command

            command = self.mid_command
            data = pickle.loads(buffer_data)

            self.mid_command = None
            self.set_terminator(ChannelProtocol.DEFAULT_TERMINATOR)

            self.process_command(command, data)
        else:
            command, dataLength = buffer_data.split(ChannelProtocol.COMMAND_SEPARATOR)

            if dataLength:
                self.mid_command = command
                self.set_terminator(int(dataLength))
            else:
                self.process_command(command)

    def send_command(self, command, data=None):
        if ChannelProtocol.COMMAND_SEPARATOR not in command:
            command += ChannelProtocol.COMMAND_SEPARATOR

        if data is not None:
            pickledData = pickle.dumps(data)
            command += str(len(pickledData))

            logging.debug("Sending command with data: %s." % command)
            self.push(command + ChannelProtocol.DEFAULT_TERMINATOR + pickledData)
        else:
            logging.debug("Sending command: %s." % command)
            self.push(command + ChannelProtocol.DEFAULT_TERMINATOR)

    def process_command(self, command, data=None):
        commands = {"disconnect" : lambda x, y : self.handle_close()}

        if command in commands:
            commands[command](command, data)
        else:
            logging.error("Command does not exist: %s." % command)
            raise NotImplementedError("This command does not exist: %s." % command)

    def handle_close(self):
        self.close()
