import os
import neo4j
from neo4j import GraphDatabase
import neo4j.spatial

# connect to the Neo4j database
uri = os.environ.get('NEO4J_URI', 'bolt://localhost:7687')
username = os.environ.get('NEO4J_USER', 'neo4j')
password = os.environ.get('NEO4J_PASSWORD', 'neo4j')

driver = GraphDatabase.driver(uri, auth=(username, password))

# SESSIONS

# open a new session
with driver.session() as session:
    pass

# open a new session with a specific database
with driver.session(database="people") as session:


# TRANSACTION FUNCTIONS

    # auto-commit transaction
    session.run(
        "MATCH (p:Person {name: $name}) RETURN p", # Query
        name="Tom Hanks" # Named parameters referenced
    )                    # in Cypher by prefixing with a $

    # warning: only use this for one-off queries (because it's not transactional)


    # read transaction

    # Define a Unit of work to run within a Transaction (`tx`)
    def get_movies(tx, title):
        return tx.run("""
            MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
            WHERE m.title = $title // (1)
            RETURN p.name AS name
            LIMIT 10
        """, title=title)

    # Execute get_movies within a Read Transaction
    session.execute_read(get_movies,
        title="Arthur" # (2)
    )


    # write transaction

    # Call tx.run() to execute the query to create a Person node
    def create_person(tx, name):
        return tx.run(
            "CREATE (p:Person {name: $name})",
            name=name
        )


    # Execute the `create_person` "unit of work" within a write transaction
    session.execute_write(create_person, name="Michael")

    
# manual transaction

with session.begin_transaction() as tx:
    # Run queries by calling `tx.run()`

    try:
        # Run a query
        tx.run(query, **params)

        # Commit the transaction
        tx.commit()
    except:
        # If something goes wrong in the try block,
        # then rollback the transaction
        tx.rollback()

# Close the session
session.close()


# PROCESSING RESULTS

# Unit of work
def get_actors(tx, movie): # (1)
    result = tx.run("""
        MATCH (p:Person)-[:ACTED_IN]->(:Movie {title: $title})
        RETURN p
    """, title=movie)

    # Access the `p` value from each record
    return [ record["p"] for record in result ]

# Open a Session
with driver.session() as session:
    # Run the unit of work within a Read Transaction
    actors = session.execute_read(get_actors, movie="The Green Mile") # (2)
    # this returns a Result object

    for record in actors:
        print(record["p"])

    session.close()

result = actors

# Check the first record without consuming it
peek = result.peek()
print(peek)

# Get all keys available in the result
print(result.keys()) # ["p", "roles"]

def get_actors_single(tx, movie):
    result = tx.run("""
        MATCH (p:Person)-[:ACTED_IN]->(:Movie {title: $title})
        RETURN p
    """, title=movie)

    # Return the first record
    return result.single()


def get_actors_values(tx, movie):
    result = tx.run("""
        MATCH (p:Person)-[r:ACTED_IN]->(m:Movie {title: $title})
        RETURN p.name AS name, m.title AS title, r.roles AS roles
    """, title=movie)

    # extract a single value from the result
    return result.value("name", False)
    # Returns the `name` value, or False if unavailable


def get_actors_values(tx, movie):
    result = tx.run("""
        MATCH (p:Person)-[r:ACTED_IN]->(m:Movie {title: $title})
        RETURN p.name AS name, m.title AS title, r.roles AS roles
    """, title=movie)

    # extract more than one value from the result
    return result.values("name", "title", "roles")


def get_actors_consume(tx, name):
    result = tx.run("""
        MERGE (p:Person {name: $name})
        RETURN p
    """, name=name)

    # consume the records, return a ResultSummary
    info = result.consume()

    # The time it took for the server to have the result available. (milliseconds)
    print(info.result_available_after)

    # The time it took for the server to consume the result. (milliseconds)
    print(info.result_consumed_after)

    print("{0} nodes created".format(info.counters.nodes_created))
    print("{0} properties set".format(info.counters.properties_set))


# loop through the records
for record in result:
    print(record["p"]) # Person Node

# Get all keys available in the result
print(result.keys()) # ["p", "roles"]



# NEO4J TYPES

result = tx.run("""
MATCH path = (person:Person)-[actedIn:ACTED_IN]->(movie:Movie {title: $title})
RETURN path, person, actedIn, movie
""", title=movie)

for record in result:

    # nodes
    node = record["movie"]

    print(node.id)              # (1)
    print(node.labels)          # (2)
    print(node.items())         # (3)

    # (4)
    print(node["name"])
    print(node.get("name", "N/A"))

    # relationships
    acted_in = record["actedIn"]

    print(acted_in.id)         # (1)
    print(acted_in.type)       # (2)
    print(acted_in.items())    # (3)

    # 4
    print(acted_in["roles"])
    print(acted_in.get("roles", "(Unknown)"))

    print(acted_in.start_node) # (5)
    print(acted_in.end_node)   # (6)

    # paths
    path = record["path"]

    print(path.start_node)  # (1)
    print(path.end_node)    # (2)
    print(len(path))  # (1)
    print(path.relationships)  # (1)


    # path segments
    for rel in iter(path):
        print(rel.type)
        print(rel.start_node)
        print(rel.end_node)


# temporal types

# Create a DateTime instance using individual values
datetime = neo4j.time.DateTime(year, month, day, hour, minute, second, nanosecond)

#  Create a DateTime  a time stamp (seconds since unix epoch).
from_timestamp = neo4j.time.DateTime(1609459200000) # 2021-01-01

# Get the current date and time.
now = neo4j.time.DateTime.now()

print(now.year) # 2022


# spatial types

# Using X and Y values
twoD=neo4j.spatial.CartesianPoint((1.23, 4.56))
print(twoD.x, twoD.y)

# Using X, Y and Z
threeD=neo4j.spatial.CartesianPoint((1.23, 4.56, 7.89))
print(threeD.x, threeD.y, threeD.z)

# Using longitude and latitude
london=neo4j.spatial.WGS84Point((-0.118092, 51.509865))
print(london.longitude, london.latitude)

# Using longitude, latitude and height
the_shard=neo4j.spatial.WGS84Point((-0.086500, 51.504501, 310))
print(the_shard.longitude, the_shard.latitude, the_shard.height)

# distance between two points
tx.run("""
WITH point({x: 1, y:1}) AS one,
     point({x: 10, y: 10}) AS two

RETURN point.distance(one, two) // 12.727922061357855
""")