# Usage

Create a new client:

    import flockdb
    client = flockdb.Client("localhost", 7915, {
        "follows": 1,
        "blocks": 2,
        })

Edge manipulation:

    client.add(1, "follows", 2)
    client.remove(1, "follows", 2)

Perform a query:

    client.get(1, "follows", None)
    => (2,)
    client.get(None, "follows", 2)
    => (1,)
    client.get_all([(1, "follows", 2), (2, "follows", 3)])
    => [(2,), ()]
