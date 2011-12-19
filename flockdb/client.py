#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import struct
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

from flockdb.gen import FlockDB
from flockdb.gen import ttypes

class Client(object):

    def __init__(self, host, port, graphs):
        """ Client("server_host", port_number, { "edge_1": 1, "edge_2": 2 })
        """
        self.sock = thrift.transport.TSocket.TSocket(host, port)
        self.transport = thrift.transport.TTransport.TBufferedTransport(
            self.sock)
        self.protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(
            self.transport)
        self.transport.open()
        self.server = FlockDB.Client(self.protocol)
        self.graphs = graphs

    def add(self, source, graph, dest):
        """ client.add(source_id, "graph_string", dest_id || [dest_ids]) """
        return self.server.execute(self._execute_operation(
                source, graph, dest,
                ttypes.ExecuteOperationType.Add))

    def remove(self, source, graph, dest):
        """ client.remove(source_id, "graph_string", dest_id || [dest_ids]) """
        return self.server.execute(self._execute_operation(
                source, graph, dest,
                ttypes.ExecuteOperationType.Remove))

    def _execute_operation(self, source, graph, dest, _type):
        return ttypes.ExecuteOperations(
            operations=[ttypes.ExecuteOperation(
                    operation_type=_type,
                    term=self._query_term(source, graph, dest)
                    )],
            priority=ttypes.Priority.High)

    def get(self, source, graph, dest):
        """ client.get(source_id, "graph"=_string", dest_id || [dest_ids]) """
        return self._unpack(self.server.select2([
                    ttypes.SelectQuery(
                        [ttypes.SelectOperation(
                                operation_type=\
                                    ttypes.SelectOperationType.SimpleQuery,
                                term=self._query_term(source, graph, dest)
                                )
                            ],
                        ttypes.Page(100, -1)
                        )
                    ])[0].ids)

    def _pack(self, l):
        if isinstance(l, int):
            l = [l]
        return struct.pack('Q' * len(l), *l)

    def _unpack(self, s):
        return struct.unpack('Q' * (len(s) / 8), s)

    def _query_term(self, source, graph, dest):
        is_forward = True
        if not source:
            source = dest
            dest = None
            is_forward = False

        return ttypes.QueryTerm(
            source_id=source,
            graph_id=self.graphs.get(graph),
            destination_ids=self._pack(dest) if dest else None,
            is_forward=is_forward
            )


