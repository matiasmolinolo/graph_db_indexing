from neo4j import GraphDatabase
import timeit
from indexes.avl import AVLTreeIndex

class GraphDatabaseIndexing:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.mst_index = {}
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
            self.mst_index[index_name] = AVLTreeIndex(name=index_name)
        if index_type == "faiss":
            pass
            #self.faiss_index['general'] = FaissIndex()


if __name__ == "__main__":
    db = GraphDatabaseIndexing("bolt://localhost:7687", "neo4j", "pasantia")
    
    start = timeit.default_timer()
    all_nodes = db.getAll()
    stop = timeit.default_timer()
    print('Time to get all: ', stop - start)
    print('All nodes: ', len(all_nodes))
    for res in all_nodes:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    node_with_name = db.getNodeWithProperty("name", "Meg Ryan")
    stop = timeit.default_timer()
    print('Time to get one node: ', stop - start)
    print('Node: ', node_with_name[0])

    print()

    start = timeit.default_timer()
    node_with_title = db.getNodeWithProperty("title", "Speed Racer")
    stop = timeit.default_timer()
    print('Time to get one node: ', stop - start)
    print('Node: ', node_with_title[0])

    print()
    print('--------------------------------------------------')
    print()

    start = timeit.default_timer()
    person_nodes = db.getEntityNodes("Person")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Person nodes: ', len(person_nodes))
    for res in person_nodes:
        print(res[0])
    
    print()

    start = timeit.default_timer()
    movie_nodes = db.getEntityNodes("Movie")
    stop = timeit.default_timer()
    print('Time to get all nodes of an entity: ', stop - start)
    print('Movie nodes: ', len(movie_nodes))
    for res in movie_nodes:
        print(res[0])

    print()
    print('--------------------------------------------------')
    print()

    db.createIndex('general', 'avl')
    for node in all_nodes:
        if list(node[0].labels)[0] == 'Person':
            db.mst_index['general'].insert(node[0].get('born', 0), node[0])
        elif list(node[0].labels)[0] == 'Movie':
            db.mst_index['general'].insert(node[0].get('released', 0), node[0])
    
    start = timeit.default_timer()
    nodes_with_year = db.mst_index['general'].find(1961)
    stop = timeit.default_timer()
    print('Time to get nodes with year 1961: ', stop - start)
    print('Nodes with year 1961: ', len(nodes_with_year.values))
    for res in nodes_with_year.values:
        print(res)

    start = timeit.default_timer()
    nodes_with_year = db.mst_index['general'].find(2003)
    stop = timeit.default_timer()
    print('Time to get nodes with year 2003: ', stop - start)
    print('Nodes with year 2003: ', len(nodes_with_year.values))
    for res in nodes_with_year.values:
        print(res)