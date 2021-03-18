"""
Minion is a node in the network which has blocks of information.
todo:
1. Queue for requests?
2. What happens if this minions fails and restarts?
3. How to handle connection issues
4. Replication policy
"""

import rpyc
import os
import sys
import traceback
import signal

from rpyc.utils.server import ThreadedServer

DATA_DIR = "C:\\Users\\chaitanyad\\Documents\\Personal\\DisFS\\dis-data\\"


def int_handler(_signal, _frame):
    sys.exit(0)


class MinionService(rpyc.Service):
    class exposed_Minion:
        def exposed_put(self, block_uuid, block_data, minions):
            try:
                with open(DATA_DIR + str(block_uuid), 'wb') as f:
                    f.write(block_data)
                if len(minions) > 0:
                    self.forward(block_uuid, block_data, minions)
            except Exception as e:
                traceback.print_exc(e)

        def exposed_get(self, block_uuid):
            try:
                block_address = DATA_DIR + str(block_uuid)
                if not os.path.isfile(block_address):
                    return None
                print("--- Sending block: ", block_uuid)
                with open(block_address, "rb") as f:
                    return f.read()
            except Exception as e:
                traceback.print_exc(e)
                return None

        def forward(self, block_uuid, block_data, minions):
            try:
                print("*** Forwarding block", block_uuid)
                current_minion = minions[0]
                remaining_minions = minions[1:]

                host, port = current_minion
                current_minion_conn = rpyc.connect(host, port=port)
                current_minion_conn_root = current_minion_conn.root.Minion()
                current_minion_conn_root.put(
                    block_uuid, block_data, remaining_minions)
            except Exception as e:
                traceback.print_exc(e)

        def delete_block(self, block_uuid, minions):
            # we can have delayed delete as its not a priority than read/write
            pass


if __name__ == "__main__":
    threaded_server = None
    try:
        # init signal handler
        signal.signal(signal.SIGINT, int_handler)

        # create DATA_DIR
        if not os.path.isdir(DATA_DIR):
            os.mkdir(DATA_DIR)

        # Start RPC service
        print("Minion started on port {0}".format(sys.argv[1]))
        threaded_server = ThreadedServer(MinionService, port=sys.argv[1])
        threaded_server.start()
    except Exception as e:
        traceback.print_exc(e)
        if threaded_server is not None:
            threaded_server.close()
