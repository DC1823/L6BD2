import neo4j
from neo4j import GraphDatabase

uri = "neo4j+s://db6b2032.databases.neo4j.io"
user = "neo4j"
password = "9qZTOGNPyXwXazSTSbIM0QVOfyQpawll1S98vMOtvnI"

# Función para conectarse a la base de datos
def connect_to_database(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

# Función para crear un nodo en la base de datos
def create_node(tx, label, properties):
    query = (
        f"CREATE (n:{label} {properties})"
        "RETURN n"
    )
    result = tx.run(query)
    return result.single()[0]

# Función para crear una relación entre nodos en la base de datos
def create_relationship(tx, start_node, end_node, rel_type, properties):
    query = (
        f"MATCH (a), (b)"
        f"WHERE a.name = '{start_node}' AND b.title = '{end_node}'"
        f"CREATE (a)-[r:{rel_type} {properties}]->(b)"
        "RETURN r"
    )
    result = tx.run(query)
    return result.single()[0]

# Conectarse a la base de datos
driver = connect_to_database(uri, user, password)

# Crear nodos de usuarios y películas
with driver.session() as session:
    # Crear nodos de usuarios
    users = ["Alice", "Bob", "Charlie", "David", "Eve"]
    for user in users:
        session.write_transaction(create_node, "User", f"{{name: '{user}', userId: '{user.lower()}123'}}")

    # Crear nodo de película
    session.write_transaction(create_node, "Movie", "{title: 'Inception', movieID: '456', Year: 2010, plot: 'Dream within a dream'}")
    session.write_transaction(create_node, "Movie", "{title: 'The Matrix', movieID: '789', Year: 1999, plot: 'Reality vs Simulation'}")

    # Crear relaciones de rating entre usuarios y películas
    session.write_transaction(create_relationship, "Alice", "Inception", "RATED", "{rating: 5}")
    session.write_transaction(create_relationship, "Bob", "Inception", "RATED", "{rating: 4}")
    session.write_transaction(create_relationship, "Charlie", "Inception", "RATED", "{rating: 3}")
    session.write_transaction(create_relationship, "David", "Inception", "RATED", "{rating: 4}")
    session.write_transaction(create_relationship, "Eve", "Inception", "RATED", "{rating: 5}")
    session.write_transaction(create_relationship, "Alice", "The Matrix", "RATED", "{rating: 4}")
    session.write_transaction(create_relationship, "Bob", "The Matrix", "RATED", "{rating: 5}")
    session.write_transaction(create_relationship, "Charlie", "The Matrix", "RATED", "{rating: 4}")
    session.write_transaction(create_relationship, "David", "The Matrix", "RATED", "{rating: 3}")
    session.write_transaction(create_relationship, "Eve", "The Matrix", "RATED", "{rating: 4}")
    
# Cerrar la conexión al finalizar
driver.close()