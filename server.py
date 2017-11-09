import asyncore
import logging
import socket

from server_channel import ServerChannel


class Server(asyncore.dispatcher):

    DEFAULT_PORT = 12345

    def __init__(self):

        self.socket_map = {}
        asyncore.dispatcher.__init__(self, map=self.socket_map)

        # MapReduce Functions
        self.map = None
        self.reduce = None
        self.collect = None

        self.data = None

    def run_server(self, port=DEFAULT_PORT):

        if self.check_server_prerequisites():
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.bind(("", port))
            self.listen(1)

            asyncore.loop(map=self.socket_map)

            logging.info("Server has been started on port %i." % port)
        else:
            logging.warning("Server does not contain all functions and data necessary for MapReduce.")

    def handle_accept(self):
        connection, address = self.accept()
        ServerChannel(connection, self.socket_map, self)

        logging.debug("Server accepted client from address: (%s, %s)." %(address[0], address[1]))

    def check_server_prerequisites(self):

        if self.map is None:
            return False
        if self.reduce is None:
            return False
        if self.data is None:
            return False

        return True
