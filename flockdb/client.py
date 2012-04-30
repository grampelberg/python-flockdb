#
# Copyright(c) 2011, Thomas Rampelberg <thomas@saunter.org>
# All rights reserved
#

import numbers
import struct
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

from flockdb.gen import FlockDB
from flockdb.gen import ttypes

class Client (object):

    max_retries = 10
    page_length = 500

    def __init__(self, host, port, graphs=None):
        """ Client("server_host", port_number, { "edge_1": 1, "edge_2": 2 })
        """
        self.sock = thrift.transport.TSocket.TSocket(host, port)
        self.transport = thrift.transport.TTransport.TFramedTransport(
            self.sock)
        self.protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(
            self.transport)
        self.transport.open()
        self.server = FlockDB.Client(self.protocol)
        self.graphs = graphs

    def get_graph_id(self, graph):
        return self.graphs[graph] if self.graphs else graph

    def transaction(self):
        return Transaction(self)

    def _operation(self, optype, source, graph, dest):
        return ttypes.ExecuteOperation(
            operation_type = optype,
            term = self._query_term(source, graph, dest)
        )

    def _add(self, source, graph, dest):
        return self._operation(ttypes.ExecuteOperationType.Add,
                source, graph, dest)

    def _remove(self, source, graph, dest):
        return self._operation(ttypes.ExecuteOperationType.Remove,
                source, graph, dest)

    def _execute(self, ops):
        op = ttypes.ExecuteOperations(
            operations = ops,
            priority = ttypes.Priority.High
        )
        return self.server.execute(op)

    def add(self, source, graph, dest):
        """
        client.add(source_id, "graph_string", dest_id || [dest_ids])
        => None
        """
        self._execute([self._add(source, graph, dest)])

    def remove(self, source, graph, dest):
        """
        client.remove(source_id, "graph_string", dest_id || [dest_ids])
        => None
        """
        self._execute([self._remove(source, graph, dest)])

    def get(self, source, graph, dest):
        """
        client.get(source_id, "graph_string", dest_id || [dest_ids])
        => (edges,)
        """
        return self.get_all([(source, graph, dest)])[0]

    def get_all(self, queries):
        """
        client.get_all([source_id, "graph_string", dest_id || [dest_ids]])
        => [ result_1, result_2 ]
        """
        req = [self._select_query(*q) for q in queries]

        for i in xrange(self.max_retries):
            try:
                resp = self.server.select2(req)
            except ttypes.FlockException:
                if i == self.max_retries-1:
                    raise
            else:
                break

        return [self._unpack(x.ids) for x in resp]

    def _select_query(self, *args):
        op = ttypes.SelectOperation(
            operation_type = ttypes.SelectOperationType.SimpleQuery,
            term = self._query_term(*args)
        )
        page = ttypes.Page(self.page_length, -1)
        return ttypes.SelectQuery([op], page)

    def get_metadata(self, source, graph):
        """
        client.get_metadata(source,_id, "graph_string")
        => obj.source_id/state_id/count/updated_at
        """
        return self.server.get_metadata(source, self.get_graph_id(graph))

    def batch(self):
        return

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def _pack(self, l):
        # Note that it is very important to use `q` instead of `Q`. The binary
        # protocol assumes signed and will choke on unsigned.
        if isinstance(l, numbers.Integral):
            l = [l]
        return struct.pack('q' * len(l), *l)

    def _unpack(self, s):
        return struct.unpack('q' * (len(s) / 8), s)

    def _query_term(self, source, graph, dest):
        is_forward = True
        if not source:
            source = dest
            dest = None
            is_forward = False

        return ttypes.QueryTerm(
            source_id=source,
            graph_id=self.get_graph_id(graph),
            destination_ids=self._pack(dest) if dest else None,
            is_forward=is_forward
        )

class Transaction (object):

    def __init__(self, client):
        self.client = client
        self.operations = []

    def __enter__(self):
        return self

    def add(self, source, graph, dest):
        op = self.client._add(source, graph, dest)
        self.operations.append(op)

    def remove(self, source, graph, dest):
        op = self.client._remove(source, graph, dest)
        self.operations.append(op)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.operations and not exc_type:
            self.client._execute(self.operations)
