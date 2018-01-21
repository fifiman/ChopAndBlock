import asynchat
import logging
import cPickle as pickle


class ChannelProtocol(asynchat.async_chat):
    """
    ChannelProtocol implements underlying structure for client and server channels. In particular, handling of
    buffer terminator cases.

    Commands can contain pickled binary data after them. If data follows a command, a colon will separate the
    command and the length of the following pickled data. In this case, the channel terminator will be changed
    from a newline to 'until this number of bytes have been read'. After the data is read, the terminator will
    return to it's default state.

    Attributes:
        buffer ([str]): Buffer containing blocks of received data.
        mid_command (str/None): Previous command while we are waiting on binary data associated to that command.
    """

    COMMAND_SEPARATOR = ":"
    DEFAULT_TERMINATOR = "\n"

    def __init__(self, connection=None, socket_map=None):
        """
        Initialize channel protocol and it's parent class.

        Args:
            connection (socket): Connect to already existing socket.
            socket_map (map[socket]): Socket map to which to be added to, if exists.
        """
        asynchat.async_chat.__init__(self, sock=connection, map=socket_map)

        self.set_terminator(ChannelProtocol.DEFAULT_TERMINATOR)

        self.buffer = []
        self.mid_command = None

    def collect_incoming_data(self, data):
        """
        Append data to channel protocol buffer once it has been received.

        Args:
            data (str): Incoming data.

        Returns:
            None
        """
        self.buffer.append(data)

    def found_terminator(self):
        """
        Process data in buffer after terminator has been found.

        There are two possible cases for received buffer data:
            1. Either a new command has been received, which may be followed by it's binary data.
            2. Binary data for the previous command.

        Returns:
            None
        """
        buffer_data = ''.join(self.buffer)
        self.buffer[:] = []

        if self.mid_command is not None:
            # Unpickle data and process command
            command = self.mid_command
            data = pickle.loads(buffer_data)

            # Reset channel protocol state.
            self.mid_command = None
            self.set_terminator(ChannelProtocol.DEFAULT_TERMINATOR)

            self.process_command(command, data)
        else:
            command, data_length = buffer_data.split(ChannelProtocol.COMMAND_SEPARATOR)

            if data_length:  # Binary data follows current command.
                self.mid_command = command
                self.set_terminator(int(data_length))
            else:
                self.process_command(command)

    def send_command(self, command, data=None):
        """
        Send command, optionally with according data. Pickling the data in case it exists.

        Args:
            command (str): Command to send.
            data (None/str): Data to follow command, if it exists.

        Returns:
            None
        """
        # Add command separator if does not exist due to how commands are parsed.
        if ChannelProtocol.COMMAND_SEPARATOR not in command:
            command += ChannelProtocol.COMMAND_SEPARATOR

        if data is not None:
            pickled_data = pickle.dumps(data)
            command += str(len(pickled_data))

            logging.debug("Sending command with data: %s." % command)
            self.push(command + ChannelProtocol.DEFAULT_TERMINATOR + pickled_data)
        else:
            logging.debug("Sending command: %s." % command)
            self.push(command + ChannelProtocol.DEFAULT_TERMINATOR)

    def process_command(self, command, data=None):
        """
        Process given command with according optional data.

        Args:
            command (str): Command to process.
            data (None/str): Data to process with command, if exists.

        Returns:
            None or throws NotImplementedError if command does not exist.
        """
        commands = {"disconnect": lambda x, y: self.handle_close()}

        if command in commands:
            commands[command](command, data)
        else:
            logging.error("Command does not exist: %s." % command)
            raise NotImplementedError("This command does not exist: %s." % command)

    def handle_close(self):
        """
        Close channel.

        Returns:
             None
        """
        self.close()
