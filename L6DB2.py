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

# Función para buscar una película por título
def find_movie(tx, title):
    query = (
        "MATCH (m:Movie {title: $title})"
        "RETURN m"
    )
    result = tx.run(query, title=title)
    return result.single()

# Función para buscar un usuario por nombre
def find_user(tx, name):
    query = (
        "MATCH (u:User {name: $name})"
        "RETURN u"
    )
    result = tx.run(query, name=name)
    return result.single()

# Función para obtener la relación de rating entre un usuario y una película
def get_rating_relationship(tx, user_name, movie_title):
    query = (
        "MATCH (u:User {name: $user_name})-[r:RATED]->(m:Movie {title: $movie_title})"
        "RETURN r"
    )
    result = tx.run(query, user_name=user_name, movie_title=movie_title)
    return result.single()
# Conectarse a la base de datos
driver = connect_to_database(uri, user, password)

# Crear nodos de usuarios y películas
with driver.session() as session:
    user = session.execute_read(find_user, "Alice")
    movie = session.execute_read(find_movie, "Inception")
    rating_relationship = session.execute_read(get_rating_relationship, "Alice", "Inception")

    if user:
        user_node = user
        nombre = user_node['u'].get('name')
    else:
        print("Usuario no encontrado")
    
    if movie:
        movie_node = movie
        titulo = movie_node['m'].get('title')
        print(f"Película encontrada: {titulo}")
    else:
        print("Película no encontrada")
    
    if rating_relationship:
        print("Relación de rating encontrada")
    else:
        print("Relación de rating no encontrada")

# Cerrar la conexión al finalizar
driver.close()