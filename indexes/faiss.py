import os
import faiss
import spacy
import pandas as pd
import numpy as np

os.environ['KMP_DUPLICATE_LIB_OK']='True'

class FaissIndex():
    def __init__(self, name):
        self.name = name
        self.index = None
        self.no_dup_nodes = None
        self.nlp = spacy.load("en_core_web_sm")

    def create_index(self, nodes):
        nodes = [node[0]._properties for node in nodes]
        self.no_dup_nodes = pd.DataFrame(nodes).drop_duplicates()
        vectors = []
        for node in self.no_dup_nodes.values:
            node_vec = self.nlp(str(node))
            vectors.append(node_vec.vector.tolist())
        vecs = np.array(vectors)
        self.index = faiss.IndexFlatL2(vecs.shape[1])
        self.index.add(vecs.astype('float32'))

    def find(self, query, k):
        query_vec = np.array(self.nlp(str(query)).vector).reshape(1, -1)
        results = self.index.search(query_vec, k)
        return self.no_dup_nodes.iloc[results[1][0]].values.tolist()

