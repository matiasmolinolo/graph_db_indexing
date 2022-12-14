from neo4j import GraphDatabase
import timeit
from indexes.avl import AVLTreeIndex
from indexes.faiss import FaissIndex

class GraphDatabaseIndexing:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.avl_index = {}
        self.faiss_index = {}

    def getAll(self):
        with self.driver.session() as session:
            result = session.execute_read(self._execute_get_all)
            return result

    def getNodeWithProperty(self, property, property_value):
        with self.driver.session() as session:
            result = session.execute_read(self._execute_get_node_with_property, property, property_value)
            return result

    def getEntityNodes(self, entity):
        with self.driver.session() as session:
            result = session.execute_read(self._execute_get_entity_nodes, entity)
            return result

    @staticmethod
    def _execute_get_entity_nodes(tx, entity):
        results = tx.run(f"MATCH (n:{entity}) RETURN n")
        entity_nodes = [result.values() for result in results]
        return entity_nodes
    
    @staticmethod
    def _execute_get_node_with_property(tx, property, property_value):
        results = tx.run(f"MATCH (n) WHERE n.{property} = $property RETURN n", property=property_value)
        nodes_with_property = [result.values() for result in results]
        return nodes_with_property

    @staticmethod
    def _execute_get_all(tx):
        results = tx.run("MATCH (n) RETURN n")
        nodes = [result.values() for result in results]
        return nodes

    def createIndex(self, index_name, index_type):
        if index_type == "avl":
            self.avl_index[index_name] = AVLTreeIndex(name=index_name)
        if index_type == "faiss":
            self.faiss_index[index_name] = FaissIndex(name=index_name)


def avl_index_queries(db):
    start = timeit.default_timer()
    all_nodes = db.getAll()
    stop = timeit.default_timer()
    print('Time to get all: ', stop - start)
    print('All nodes: ', len(all_nodes))
    for res in all_nodes[:10]:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    node_with_name = db.getNodeWithProperty("born", 1961)
    stop = timeit.default_timer()
    print('Time to get nodes with year 1961 (NO INDEX): ', stop - start)
    print('Nodes: ')
    for res in node_with_name:
        print(res[0])

    print()

    start = timeit.default_timer()
    node_with_title = db.getNodeWithProperty("released", 2003)
    stop = timeit.default_timer()
    print('Time to get nodes with year 2003 (NO INDEX): ', stop - start)
    print('Nodes: ',)
    for res in node_with_title:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    person_nodes = db.getEntityNodes("Person")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Person nodes: ', len(person_nodes))
    for res in person_nodes[:5]:
        print(res[0])
    
    print()

    start = timeit.default_timer()
    movie_nodes = db.getEntityNodes("Movie")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Movie nodes: ', len(movie_nodes))
    for res in movie_nodes[:5]:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    db.createIndex('general', 'avl')
    start = timeit.default_timer()
    for node in all_nodes:
        if list(node[0].labels)[0] == 'Person':
            db.avl_index['general'].insert(node[0].get('born', 0), node[0])
        elif list(node[0].labels)[0] == 'Movie':
            db.avl_index['general'].insert(node[0].get('released', 0), node[0])
    stop = timeit.default_timer()
    print('Time to create AVL index: ', stop - start)

    print()
    print('--------------------------------------------------')
    print()
    start = timeit.default_timer()
    nodes_with_year = db.avl_index['general'].find(1961)
    stop = timeit.default_timer()
    print('Time to get nodes with year 1961 (AVL): ', stop - start)
    print('Nodes with year 1961 (AVL): ', len(nodes_with_year.values))
    for res in nodes_with_year.values:
        print(res)

    print()

    start = timeit.default_timer()
    nodes_with_year = db.avl_index['general'].find(2003)
    stop = timeit.default_timer()
    print('Time to get nodes with year 2003 (AVL): ', stop - start)
    print('Nodes with year 2003 (AVL): ', len(nodes_with_year.values))
    for res in nodes_with_year.values:
        print(res)


def faiss_index_queries(db):
    start = timeit.default_timer()
    all_nodes = db.getAll()
    stop = timeit.default_timer()
    print('Time to get all: ', stop - start)
    print('All nodes: ', len(all_nodes))
    for res in all_nodes[:10]:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    node_with_name = db.getNodeWithProperty("born", 1961)
    stop = timeit.default_timer()
    print('Time to get nodes with year 1961 (NO INDEX): ', stop - start)
    print('Nodes: ')
    for res in node_with_name:
        print(res[0])

    print()

    start = timeit.default_timer()
    node_with_title = db.getNodeWithProperty("released", 2003)
    stop = timeit.default_timer()
    print('Time to get nodes with year 2003 (NO INDEX): ', stop - start)
    print('Nodes: ',)
    for res in node_with_title:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    person_nodes = db.getEntityNodes("Person")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Person nodes: ', len(person_nodes))
    for res in person_nodes[:5]:
        print(res[0])
    
    print()

    start = timeit.default_timer()
    movie_nodes = db.getEntityNodes("Movie")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Movie nodes: ', len(movie_nodes))
    for res in movie_nodes[:5]:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    db.createIndex('general', 'faiss')
    start = timeit.default_timer()
    db.faiss_index['general'].create_index(all_nodes)
    stop = timeit.default_timer()

    print('Time to create FAISS index: ', stop - start)

    print()
    print('--------------------------------------------------')
    print()
    start = timeit.default_timer()
    nodes_with_year = db.faiss_index['general'].find(1961, 5)
    stop = timeit.default_timer()
    print('Time to get nodes with year 1961 (FAISS): ', stop - start)
    print('Nodes with year 1961 (FAISS): ', len(nodes_with_year))
    for res in nodes_with_year:
        print(res)

    print()

    start = timeit.default_timer()
    nodes_with_year = db.faiss_index['general'].find(2003, 5)
    stop = timeit.default_timer()
    print('Time to get nodes with year 2003 (FAISS): ', stop - start)
    print('Nodes with year 2003 (FAISS): ', len(nodes_with_year))
    for res in nodes_with_year:
        print(res)


        


if __name__ == "__main__":
    db = GraphDatabaseIndexing("bolt://localhost:7687", "neo4j", "graph_index")
    
    print("----------------------")
    print("AVL INDEX")
    print()
    avl_index_queries(db)
    print()
    print("----------------------")
    print()
    print("FAISS INDEX")
    print()
    faiss_index_queries(db)
    print()
    print("----------------------")