import asyncore
import logging
import random
import socket
from server_channel import ServerChannel


class Server(asyncore.dispatcher, object):
    """
    Server acts as the coordinator between the MapReduce task and the clients that are to process it. Data and MapReduce
    functions are to assigned to server before running the server. Along with the data, a TaskManager object is created
    to split the data into MapReduce tasks. The server is constantly running and listening for incoming connections.

    Attributes:
        socket_map ([Socket]): List to which created ServerChannel instances should be added to.
        map (Function): Map function.
        reduce (Function): Reduce function.
        collect (Function): Collect function.
        __data (dict): MapReduce data in dictionary format.
        task_manager (TaskManager): TaskManager object associated to data for delegating MapReduce tasks.
    """
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

    def run_server(self, address="", port=DEFAULT_PORT):
        """
        Run server and listen for connections, if it contains the required data and MapReduce functions.

        Args:
            address (str): Address for server to be started on.
            port (int): Port number for server to be started on.

        Returns:
            None
        """
        if self.check_server_prerequisites():
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.bind((address, port))
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
        Accept connection request and create new ServerChannel.

        Returns:
            None
        """
        connection, address = self.accept()
        ServerChannel(connection, self.socket_map, self)

        logging.debug("Server accepted client from address: (%s, %s)." % (address[0], address[1]))

    def handle_close(self):
        """
        Shut server down and log it.

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


class TaskManager(object):
    """
    TaskManager splits the data into MapReduce tasks for clients to process. While in the MAPPING state, data is sent
    to clients to be 'mapped'. After all map tasks have finished, the TaskManager is switched to the REDUCING state
    where data is sent to clients to be 'reduced'. After all reduce states have finished the results are sent back to
    the parent server.

    Attributes:
        data (str): Data to be processed.
        parent_server (Server): Instance of parent Server.
        state ([0|1|2|3]): The current state of data processing. Possible states: START, MAPPING, REDUCING, DONE.
        results (dict): Results of the MapReduce job.
        working_maps (dict): Map tasks that are currently being worked on; sent to clients.
        map_iterator (dict iterator): Iterator over MapReduce data.
        map_results (dict): Data of finished map tasks.
        working_reduces (dict): Reduce tasks that are currently being worked on; sent to clients.
        reduce_iter (dict iterator): Iterator over finished map tasks(map_results).

    """
    START = 0
    MAPPING = 1
    REDUCING = 2
    DONE = 3

    def __init__(self, data, parent_server):

        self.data = data
        self.parent_server = parent_server

        self.state = TaskManager.START
        self.results = None

        self.working_maps = {}
        self.map_iterator = None
        self.map_results = {}

        self.working_reduces = {}
        self.reduce_iter = None

    def get_next_task(self):
        """
        Get next MapReduce task for client to process. May return a map task, a reduce task, or even request to
        disconnect if the MapReduce job is complete.

        Returns:
            (command (str), data (str))
        """
        if self.state == TaskManager.START:
            self.map_iterator = self.data.iteritems()
            self.state = TaskManager.MAPPING

        if self.state == TaskManager.MAPPING:
            try:
                map_key, map_data = self.map_iterator.next()
                self.working_maps[map_key] = map_data

                return "map", (map_key, map_data)
            except StopIteration:  # No more new MapReduce data.

                if len(self.working_maps) > 0:
                    # Restart map task with new client, in case other client has timed out or failed.
                    return "map", random.choice(self.working_maps.items())

                # Switch to REDUCE state.
                self.state = TaskManager.REDUCING

                self.reduce_iter = self.map_results.iteritems()
                self.working_reduces = {}
                self.results = {}

        if self.state == TaskManager.REDUCING:
            try:
                reduce_key, reduce_data = self.reduce_iter.next()
                self.working_reduces[reduce_key] = reduce_data

                return "reduce", (reduce_key, reduce_data)
            except StopIteration:  # No more new map data.

                if len(self.working_reduces) > 0:
                    # Restart reduce task with new client, in case other client has timed out or failed.
                    return "reduce", random.choice(self.working_reduces.items())

                self.state = TaskManager.DONE

        if self.state == TaskManager.DONE:
            self.parent_server.handle_close()
            return "disconnect", None

    def map_done(self, data):
        """
        Handle incoming data from completed Map task.

        Args:
             data (dict): Completed Map task data.

        Returns:
            None
        """
        if data[0] not in self.working_maps:
            # This map job is already finished by someone else. Do nothing.
            return

        logging.debug("Map job done: %s." % data[0])

        # Append current tasks map data to overall map results.
        for key, values in data[1].iteritems():
            if key not in self.map_results:
                self.map_results[key] = []
            self.map_results[key].extend(values)

        # Remove map task from in-progress map tasks.
        del self.working_maps[data[0]]

    def reduce_done(self, data):
        """
        Handle incoming data from completed Reduce task.

        Args:
            data (dict): Completed Reduce task data.

        Returns:
            None

        """
        if data[0] not in self.working_reduces:
            # This reduce job has been finished by someone else.
            return

        logging.debug("Reduce job done: %s." % data[0])

        self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]
