"""
Master provided methods to support distribution of the file onto many nodes.
It implements a RPC server that interacts with the client.
todo:
1. logging
2. Queue?
"""

import configparser
import math
import pickle
import random
import signal
import sys
import traceback
import uuid
import rpyc
import os

from rpyc.utils.server import ThreadedServer


# todo: set logger
# if not os.path.exists("logs"):
#     os.makedirs("logs")
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s',
#                     datefmt='%a, %d %b %Y %H:%M:%S', filename='logs/master.log', filemode='w')
# log = logging.getLogger(__name__)


def int_handler(_signal, _frame):
    # pickle.dump((MasterService.exposed_Master.file_table, MasterService.exposed_Master.block_mapping),
    #             open("fs.img", "wb"))
    sys.exit(0)


def init_config():
    """
    Gets config details from the config file and sets the values in the MasterService class
    accordingly.
    Returns True if successful else False
    """
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read_file(open("config.conf"))
        MasterService.exposed_Master.block_size = int(config_parser.get('master', 'block_size'))
        MasterService.exposed_Master.replication_factor = int(config_parser.get('master', 'replication_factor'))

        minions = config_parser.get('master', 'minions').split(",")
        for mini in minions:
            id, host, port = mini.split(":")
            MasterService.exposed_Master.minions[id] = (host, port)

        # if os.path.isfile("fs.img"):
        #     MasterService.exposed_Master.file_table, MasterService.exposed_Master.block_mapping = \
        #         pickle.load(open("fs.img", "rb"))

        return True
    except Exception as e:
        traceback.print_exc(e)
        return False


class MasterService(rpyc.Service):
    """
    This is a class that is exposed to the outside world using rpc.
    """

    class exposed_Master:
        file_table = dict()
        block_mapping = dict()
        minions = dict()

        block_size = 0
        replication_factor = 0

        def exposed_get_block_size(self):
            return self.block_size

        def exposed_get_replication_factor(self):
            return self.replication_factor

        def exposed_get_minions(self):
            return self.minions

        def exposed_read(self, file_name):
            if self.exposed_file_exists(file_name):
                return self.file_table[file_name]
            return None

        def exposed_write(self, file_name, file_size):
            try:
                # todo
                # can check the hash of the file if it already exists with us, else write to nodes.
                # if writing again, we need to delete the file as well
                if self.exposed_file_exists(file_name):
                    pass

                self.file_table[file_name] = list()
                num_blocks = self.calc_num_of_blocks(file_size)
                blocks = self.map_file_blocks_to_nodes(file_name, num_blocks)
                return blocks
            except Exception as e:
                traceback.print_exc(e)
                return None

        def map_file_blocks_to_nodes(self, file_name, num_blocks):
            try:
                block_nodes_mapping = list()
                for block_index in range(num_blocks):
                    block_uuid = uuid.uuid1()
                    node_ids = random.sample(self.minions.keys(), self.replication_factor)
                    block_nodes_mapping.append((block_uuid, node_ids))
                    self.file_table[file_name].append((block_uuid, node_ids))
                return block_nodes_mapping
            except Exception as e:
                traceback.print_exc(e)
                return list()

        def calc_num_of_blocks(self, file_size):
            return math.ceil(float(file_size) / self.block_size)

        def exposed_file_exists(self, file_name):
            return file_name in self.file_table


if __name__ == "__main__":
    threaded_server = None
    try:
        # init signal handler
        signal.signal(signal.SIGINT, int_handler)

        # init config file
        if not init_config():
            print("Error initializing config file. Exiting...")
            sys.exit(0)

        print("Master started")
        threaded_server = ThreadedServer(MasterService, port='1111')
        threaded_server.start()
    except Exception as e:
        traceback.print_exc(e)
        if threaded_server is not None:
            threaded_server.close()
