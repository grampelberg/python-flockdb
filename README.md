# Usage

Create a new client:

    import flockdb.client
    client = flockdb.client.Client("localhost", 7915", {
        "follow": 1,
        "blocks": 2,
        })

Edge manipulation:

    client.add(1, "follows", 2)

Perform a query:

    client.get(1, "follows", None)
    => (2,)
    client.get(None, "follows", 2)
    => (1,)
