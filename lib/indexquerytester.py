#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexquerytester.py
- Descripción: tester automático de consultas (queries) a índices.
- Autor: Agustín González
- Modificado: 10/04/18
'''

from os import path
from lib.index.tokenizer import Tokenizer
from lib.index.index import Index

from lib.browser import Browser, BrowseType

try:
    from lib.other import utils
except:
    raise Exception("Imposible cargar librerías.")


class IndexQueryTester(object):
    '''Tester automático de queries a índices.'''

    def __init__(self, base_dirout, iterations):
        '''Inicializa tester.

        Args:
            base_dirout (string): directorio de salida de resultados.
            iterations (int): cantidad de testeos por índice (al finalizar,
                los resultados se promedian). Tener en cuenta que se realiza
                una iteración adicional a modo de 'warm-up'.
        '''
        self.__base_dirout = base_dirout

        # Conjunto de queries a evaluar.
        self.__queries = []

        # Directorio de índices a evaluar.
        self.__dirindexes = []

        # Iteraciones
        self.__iterations = iterations

    def __check_queries_load(self):
        '''Verifica si las queries se encuentran cargadas. Retorna una excepción
        en caso de que no.'''
        if not self.__queries:
            info = "Es necesario especificar las queries con anterioridad "
            info += "utilizando el método 'set_queries()'."
            raise Exception(info)

    def set_queries(self, queries, vocabulary, max_queries):
        '''Filtra las queries cuyos términos están presentes en el vocabulario
        dado. Además, tokeniza cada consulta (query).

        Args:
            queries (string list): listado de consultas.
            vocabulary (string list): vocab. utilizado para filtrar queries.
            max_queries (int): cantidad tope de queries a seleccionar.
        '''
        # Queries filtradas.
        filtered_queries = []

        for query in queries:
            tokenized_query = [Tokenizer.convert2term(x) for x in query.split(" ")]
            tokenized_query = [x.strip() for x in tokenized_query if x]

            # Delete de repetidos y sort.
            tokenized_query = list(set(tokenized_query))
            tokenized_query = sorted(tokenized_query)

            # Si todos los términos de los tokens pertenecen al vocabulario...
            if all((x in vocabulary) for x in tokenized_query):
                if tokenized_query and tokenized_query not in filtered_queries:
                    filtered_queries.append(tokenized_query)

        filtered_queries = filtered_queries[:max_queries]
        for tokenized_query in filtered_queries:
            # Stringify.
            query = " ".join(tokenized_query)
            self.__queries.append(query)

        info = ">> Queries resultantes: {0}/{1} queries (tope: {2})"
        info = info.format(len(self.__queries), len(queries), max_queries)
        print(info)

    def set_indexes(self, dirindexes):
        '''Establece los índices a evaluar.

        Args:
            dirindexes (string list): directorios de índices a testear.
        '''
        self.__dirindexes = dirindexes

    def __execute_query_test(self, dirindex, chunks_info_in_memory):
        '''Ejecuta query test del índice especificado.

        Args:
            dirindex (string): directorio de índice.
            chunks_info_in_memory (bool): indica si la información de chunks se
                debe cargar en RAM.

        Returns:
            index (Index): índice al que pertenecen las estadísticas.
            min_times (double list): listado de tiempo mínimo de query.
            avg_times (double list): listado de tiempo promedio de query.
            max_times (double list): listado de tiempo máximo de query.
        '''
        index = Index(dirindex)
        self.__check_queries_load()

        browser = Browser(index, BrowseType.Boolean)

        # Listados de stats.
        min_times = []
        avg_times = []
        max_times = []

        index.load(chunks_info_in_memory)
        index_name = path.basename(dirindex)
        evtype = "inmemory" if chunks_info_in_memory else "indisk"
        queries_count = len(self.__queries)

        iterations = self.__iterations+1
        for i in range(0, iterations):
            info = "{0} ({1}) - evaluación nro. {2}/{3}"
            print(info.format(index_name, evtype, i, iterations))
            min_time = None
            sum_time = 0
            max_time = 0
            for query in self.__queries:
                # Obtención de posting list.
                browser.browse(query)

                # Obtención de benchmark de última búsqueda.
                time = browser.get_benchmark()

                sum_time += time

                if time > max_time:
                    max_time = time

                if not min_time or time < min_time:
                    min_time = time

            avg_time = sum_time/queries_count

            # Append de times (si no es 'warm-up').
            if i > 0:
                min_times.append(min_time*1000)
                avg_times.append(avg_time*1000)
                max_times.append(max_time*1000)

        return index, min_times, avg_times, max_times

    def test_index(self, dirindex, chunks_info_in_memory):
        '''Realiza testeo sobre el índice del directorio dado e imprime los
        resultados en la salida especificada en el constructor.

        Args:
            dirindex (string): directorio de índice.
            chunks_info_in_memory (bool): indica si la información de chunks se
                debe cargar en RAM.
        '''
        evtype = "inmemory" if chunks_info_in_memory else "indisk"
        stat_path = path.join(self.__base_dirout, evtype)
        stat_path = path.join(stat_path, path.basename(dirindex))
        stat_path = stat_path + "-stats.txt"

        if path.exists(stat_path):
            index_name = path.basename(dirindex)
            print("{0} ({1}) ya evaluado.".format(index_name, evtype))
            return

        r = self.__execute_query_test(dirindex, chunks_info_in_memory)
        index = r[0]
        stats = r[1:]

        stat_writter = open(stat_path, "w")
        info = "Índice: {0}".format(path.basename(dirindex))

        docs_count = len(index.get_collection())
        info += "\nCantidad de documentos: {0}".format(docs_count)

        terms_count = len(index.get_vocabulary())
        info += "\nCantidad de términos: {0}".format(terms_count)

        r = index.get_chunks_info_in_memory_count()
        cinfo_in_memory_count, cinfo_in_memory, cvector_size = r

        aux = "\n\nInformación de chunks cargada en memoria: {0}"
        info += aux.format("si" if cinfo_in_memory else "no")

        aux = "\nCantidad de vectores de info. de chunks en memoria: {0}"
        info += aux.format(cinfo_in_memory_count)

        aux = "\nCantidad de elementos por c/vector de chunk: {0}"
        info += aux.format(cvector_size)

        info += "\n\nQueries evaluadas: {0} ({1} pasadas)".format(len(self.__queries),
                                                                  self.__iterations)

        query_lens = [len(x.split(" ")) for x in self.__queries]
        avg_query_size = round(sum(query_lens)/len(self.__queries), 2)
        info += "\nTamaño promedio de query: {0}".format(avg_query_size)

        min_avg = round(sum(stats[0])/len(stats[0]), 2)
        info += "\n\nTiempo mínimo de response (ms): {0}".format(min_avg)

        avg = round(sum(stats[1])/len(stats[1]), 2)
        info += "\nTiempo promedio de response (ms): {0}".format(avg)

        max_avg = round(sum(stats[2])/len(stats[2]), 2)
        info += "\nTiempo máximo de response (ms): {0}".format(max_avg)

        stat_writter.write(info)
        stat_writter.close()

    def test_indexes(self, chunks_info_in_memory=False):
        '''Testea los índices especificados.

        Args:
            chunks_info_in_memory (bool): indica si la información de chunks se
                debe cargar en RAM.
        '''
        self.__check_queries_load()

        # Creación de directorio de out.
        dirout = self.__base_dirout + "/indisk/"
        if chunks_info_in_memory:
            dirout = self.__base_dirout + "/inmemory/"

        utils.makedirs(dirout)

        for dirindex in self.__dirindexes:
            self.test_index(dirindex, chunks_info_in_memory)
