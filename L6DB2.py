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
def create_relationship(tx, label_a, value_a, label_b, value_b, relationship, properties):
    query = (
        f"MATCH (a), (b)"
        f"WHERE a.{label_a} = '{value_a}' AND b.{label_b} = '{value_b}'"
        f"CREATE (a)-[r:{relationship} {properties}]->(b)"
        "RETURN r"
    )
    print(query)
    result = tx.run(query)
    return result.single()

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
# Crear nodos
with driver.session() as session:
    # Nodos Person
    session.write_transaction(create_node, "Person", "{name: 'Roberto Martínez', tmdbId: 334455, born: '1972-04-14T00:00:00Z', died: null, url: 'http://www.imdb.com/name/nm334455/', imdbId: 556677, bio: 'Un talentoso artista conocido tanto por su dirección de aclamadas películas como por sus memorables actuaciones.', poster: 'http://www.images.com/roberto_martinez.jpg'}")
    session.write_transaction(create_node, "Person", "{name: 'Juan Pérez', tmdbId: 123456, imdbId: 78910, born: '1980-05-10T00:00:00Z', died: '', url: 'http://www.imdb.com/name/nm1234567/', bio: 'Un actor reconocido por su versatilidad en el cine y la televisión.', poster: 'http://www.images.com/juan_perez.jpg'}")
    session.write_transaction(create_node, "Person", "{name: 'Maria Gómez', tmdbId: 112233, imdbId: 445566, born: '1975-09-15T00:00:00Z', died: '', url: 'http://www.imdb.com/name/nm1122334/', bio: 'Directora galardonada con múltiples premios por su visión única en el cine.', poster: 'http://www.images.com/maria_gomez.jpg'}")

    # Nodo Movie
    session.write_transaction(create_node, "Movie", "{title: 'Aventura Espacial', tmdbId: 998877, released: '2024-12-20T00:00:00Z', imdbRating: 8.7, movieId: 554433, year: 2024, runtime: 150, imdbId: 102030, countries: ['USA', 'Canada'], imdbVotes: 120000, url: 'http://www.imdb.com/title/tt9988776/', revenue: 500000000, plot: 'Una emocionante odisea por el cosmos, llena de descubrimientos y peligros.', poster: 'http://www.images.com/aventura_espacial.jpg', budget: 200000000, languages: ['English', 'Spanish']}")

    # Nodo Genre
    session.write_transaction(create_node, "Genre", "{name: 'Ciencia Ficción'}")

    # Nodo User
    session.write_transaction(create_node, "User", "{name: 'Alejandra Ruiz', userId: 667788}")

    #Nodo Relacion
    session.write_transaction(create_relationship, "name", "Roberto Martínez", "title", "Aventura Espacial", "DIRECTED", "{roles: ['Director']}")
    session.write_transaction(create_relationship, "name", "Roberto Martínez", "title", "Aventura Espacial", "ACTED_IN", "{roles: ['Piloto']}")
    session.write_transaction(create_relationship, "name", "Juan Pérez", "title", "Aventura Espacial", "ACTED_IN", "{roles: ['Capitán']}")
    session.write_transaction(create_relationship, "name", "Maria Gómez", "title", "Aventura Espacial", "DIRECTED", "{roles: ['Director']}")
    session.write_transaction(create_relationship, "name", "Alejandra Ruiz", "title", "Aventura Espacial", "RATED", "{rating: 9, timestamp: '2025-01-01T00:00:00Z'}")
    session.write_transaction(create_relationship, "title", "Aventura Espacial", "name", "Ciencia Ficción", "IN_GENRE", "{group: 'Principal'}")


# Cerrar la conexión al finalizar
driver.close()

# Cerrar la conexión al finalizar
driver.close()