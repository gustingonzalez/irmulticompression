#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexqueryanalyzer.py
- Descripción: tester de queries a directorio de índices.
- Autor: Agustín González
- Modificado: 10/04/18
'''

import sys
import multiprocessing
from os import path, walk

try:
    sys.path.append('..')
    from lib.index.index import Index
    from lib.indexquerytester import IndexQueryTester
except:
    raise Exception("Imposible cargar librerías.")

# Ins
QUERIES_PATH = "../resources/queries/AOL-200k-queries.txt"
IDX_VOC_PATH = "../output/test/index-1/"
IDXS_DIR = "../output/test/"

# Outs
STATS_BASE_DIROUT = "../output/stats/test/queries"

# Cantidad de queries a utilizar.
QUERIES_COUNT = 3000

# Cantidad de veces que se repetirá la prueba para un índice: tener en cuenta
# que se realiza una iteración extra a modo de 'warm-up'.
ITERATIONS = 2


def main(args):
    '''Punto de entrada.'''
    # Lectura de queries.
    print(">> Cargando queries...")
    queries = []
    for line in open(QUERIES_PATH, "r"):
        line = line[:-1]
        queries.append(line)

    # Lectura de vocabulario.
    print(">> Cargando vocabulario...")
    index = Index(IDX_VOC_PATH)
    index.load(chunks_info_in_memory=False)
    vocabulary = index.get_vocabulary().keys()
    del index

    # 2 threads simultáneos máximo.
    pool = multiprocessing.Pool(2)

    # Directorios a evaluar.
    idxdirs = [path.join(IDXS_DIR, x) for x in sorted(next(walk(IDXS_DIR))[1])]

    queryev = IndexQueryTester(STATS_BASE_DIROUT, ITERATIONS)
    queryev.set_queries(queries, vocabulary, QUERIES_COUNT)
    queryev.set_indexes(idxdirs)

    # Testeo de índices.
    # 1. Test in memory.
    pool.apply_async(queryev.test_indexes, {True})

    # 2. Test in disk.
    pool.apply_async(queryev.test_indexes, {False})

    # Espera de finalización de subprocesos.
    pool.close()
    pool.join()

# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
