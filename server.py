import asyncore
import logging
import random
import socket
from server_channel import ServerChannel


class Server(asyncore.dispatcher, object):
    DEFAULT_PORT = 12345

    def __init__(self):
        """
        Initialize Server class and parent class.
        """
        self.socket_map = {}
        asyncore.dispatcher.__init__(self, map=self.socket_map)

        # Set MapReduce functions and data members to None so they can be checked for later on.
        self.map = None
        self.reduce = None
        self.collect = None

        self.__data = None
        self.task_manager = None

    def run_server(self, port=DEFAULT_PORT):
        """
        Run server and listen for connections, if it contains the required data.

        Args:
            port (int): Port number for server to be started on.

        Returns:
            None
        """
        if self.check_server_prerequisites():
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.bind(("", port))
            self.listen(1)

            logging.info("Server has been started on port %i." % port)
            try:
                asyncore.loop(map=self.socket_map)
            except:
                asyncore.close_all()

            return self.task_manager.results
        else:
            logging.warning("Server does not contain all functions and data necessary for MapReduce.")

    def handle_accept(self):
        """
        Accept connection request and add new server channel to socket map.

        Returns:
            None
        """
        connection, address = self.accept()
        ServerChannel(connection, self.socket_map, self)

        logging.debug("Server accepted client from address: (%s, %s)." %(address[0], address[1]))

    def handle_close(self):
        """
        Shut server down.

        Returns:
            None
        """
        logging.info("Server shutting down.")
        self.close()

    def check_server_prerequisites(self):
        """
        Check that required functions and data exist for MapReduce.

        Returns:
            Bool whether server state fulfills requirements.
        """
        if self.map is None:
            return False
        if self.reduce is None:
            return False
        if self.data is None:
            return False

        return True

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):
        self.__data = value
        self.task_manager = TaskManager(self.__data, self)


class TaskManager():

    START = 0
    MAPPING = 1
    REDUCING = 2
    DONE = 3

    def __init__(self, data, parent_server):

        self.data = data
        self.parent_server = parent_server

        self.state = TaskManager.START
        self.results = None

    def get_next_task(self):

        if self.state == TaskManager.START:
            self.map_iterator = self.data.iteritems()

            self.working_maps = {}
            self.map_results = {}
            self.state = TaskManager.MAPPING

        if self.state == TaskManager.MAPPING:
            try:
                map_key, map_data = self.map_iterator.next()
                self.working_maps[map_key] = map_data

                return ("map", (map_key, map_data))
            except StopIteration:
                if len(self.working_maps) > 0:
                    return ("map", random.choice(self.working_maps.items()))

                self.state = TaskManager.REDUCING

                self.reduce_iter = self.map_results.iteritems()
                self.working_reduces = {}
                self.results = {}

        if self.state == TaskManager.REDUCING:
            try:
                reduce_key, reduce_data = self.reduce_iter.next()
                self.working_reduces[reduce_key] = reduce_data

                return ("reduce", (reduce_key, reduce_data))
            except StopIteration:
                if len(self.working_reduces) > 0:
                    return ("reduce", random.choice(self.working_reduces.items()))

                self.state = TaskManager.DONE

        if self.state == TaskManager.DONE:
            self.parent_server.handle_close()
            return ("disconnect", None)


    def map_done(self, data):
        if data[0] not in self.working_maps:
            # This map job is already finished by someone else.
            return

        logging.debug("Map job done: %s." % data[0])

        for key, values in data[1].iteritems():
            if key not in self.map_results:
                self.map_results[key] = []
            self.map_results[key].extend(values)

        del self.working_maps[data[0]]

    def reduce_done(self, data):
        if data[0] not in self.working_reduces:
            # This reduce job has been finished by someone else.
            return

        logging.debug("Reduce job done: %s." % data[0])

        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]
