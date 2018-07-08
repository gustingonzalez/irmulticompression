#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexer.py
- Descripción: contiene las clases 'IndexerStatusTypes' 'Indexer' y 'ChildIndexer'
que permiten la indexación de un corpus.
- Autor: Agustín González
- Modificado: 18/03/18
'''

import re
import os
import math
import time
import multiprocessing
from enum import Enum
from collections import Counter

from lib.index.index import Index
from lib.index.tokenizer import Tokenizer
from lib.other import utils
from lib.index.compression.ircodecs import EncodeTypes, gapsencoder as gapsenc
from lib.index.compression.indexstream import IndexStreamWriter
from lib.index.compression.indexstream import IndexStreamReader

# Cantidad máxima de indexadores hijos.
MAX_CHILD_INDEXERS = 4

# Máxima cantidad de documentos en memoria de un archivo TREC (al sobrepasarla
# se realiza flush).
MAX_TREC_DOCS_IN_MEMORY = 1500000

# Factor de recursos a utilizar para la indexación y el merge. El valor debe
# ser > 0 y <= 1. En el caso de los indexadores, se utiliza para calcular la
# cant. máx. de subprocesos a utilizar para generar los archivos hijos. Para
# el caso del merge, indica el factor de índices de los que se cargarán datos
# de chunks a RAM. Ej. (para el caso de indexación): si MAX_CHILD_INDEXERS es 6
# y RESOURCES_FACTOR es 0.70, luego round(6*0.70, 0) es 4, por lo que se usarán
# 4 subprocesos para generar 6 archivos. Se recomienda un valor de 0.7, para
# archivos de texto y html. Para TREC se sugiere que mientras más alto sea
# el valor de MAX_CHILD_INDEXERS y MAX_TREC_DOCS_IN_MEMORY, menor sea el de
# RESOURCES_FACTOR (rango [0.3-0.7]).
RESOURCES_FACTOR = 0.5


class IndexerStatusTypes(Enum):
    '''Tipos de estado de Indexer.'''
    Already_Indexed = 0
    Collection_Non_Existent = 1
    Ok = 2


class PostingFieldTypes(Enum):
    '''Tipos de archivo de estadísticas de codificación automática.'''
    Docs = 0
    Freqs = 1


class CorpusTypes(Enum):
    '''Tipos de corpus.'''
    Text = 1
    Html = 2
    Trec = 3


class Indexer(object):
    '''Permite indexación de corpus.'''

    def __init__(self, dirin, corpus_type, reuse_tmp=True):
        '''Inicializa instancia de indexer.

        Args:
            dirin (string): directorio de corpus.
            corpus_type (CorpusTypes): tipo de corpus.
            reuse_tmp (bool): indica si para construir el índice se deberán
                re-utilizar archivos temporales creados por los indexadores
                hijos en anteriores indexaciones.
        '''
        self.index = None
        self.doc_encode = EncodeTypes.VariableByte
        self.freq_encode = EncodeTypes.VariableByte
        self._multiencode = False

        self._dirout = ""
        self.__dirin = os.path.normpath(dirin)
        self.__corpus_type = corpus_type
        self.__reuse_tmp = reuse_tmp

        # Tamaño de chunks de índice.
        self._chunk_size = 0

        # Archivo de estadísticas de codificación automática.
        self._has_to_write_multiencode_stats = False
        self._multiencode_stats_docs_file = None
        self._multiencode_stats_freqs_file = None

        # Callers de funciones de escritura (evita ifs innecesarios dentro de
        # las funciones de escritura)
        self._write_docs_caller = None
        self._write_freqs_caller = None

        # Writers
        self._pwriter = None  # writer de postings.
        self._cwriter = None  # writer de chunks info.
        self._vwriter = None  # writer de vocabulario.

        # Directorio de archivos temporales.
        self.__dirtmp = "output//tmp//" + os.path.basename(self.__dirin)

        # Índices hijos.
        self.__child_indexes = []

        # Generación de listas de archivos para subindexadores.
        self.__compute_child_indexes_files()

    def __compute_child_indexes_files(self):
        '''Computa los archivos a procesar por los indexadores hijos.'''
        fnames = []

        # Walk en filenames según directorio de In.
        for root, _, dirfnames in os.walk(self.__dirin):
            for filename in dirfnames:
                fnames.append(os.path.join(root, filename))

        # fnames = fnames[0:35]

        # Mínimo entre cantidad de archivos a procesar y cant. de child idxs.
        child_indexers_count = min(len(fnames), MAX_CHILD_INDEXERS)
        chunk_files_size = math.ceil(len(fnames)/child_indexers_count)

        self.__child_indexers_files = []
        for i in range(0, len(fnames), chunk_files_size):
            j = i+chunk_files_size
            chunk = fnames[i:j]

            # Range de files_id: se toma el rango start_fid a end_fid, ya que
            # los ids se manejan de forma global (y no por c/ subíndice hijo),
            # lo que luego facilita el merge.
            start_fid = i+1  # +1 ya que i es índice.
            end_fid = j+1    # +1 ya que j es índice.
            fids = range(start_fid, end_fid+1)

            # Mapeo chunks, ids.
            chunk = dict(zip(fids, chunk))
            self.__child_indexers_files.append(chunk)

    def __create_child_indexes(self):
        '''Crea (vía threads) los índices hijos basados en los archivos
        computados por el método '__compute_child_indexers_files()'.

        Returns:
            indexes (Indexer list): listado de índices creados.
        '''
        # Archivos que deberán indexar los indexadores hijos.
        child_indexers_files = self.__child_indexers_files

        # Pool y cantidad de subprocesos (files*RFACTOR, trade-off mem/proc).
        child_count = len(child_indexers_files)
        pool_size = int(round(child_count*RESOURCES_FACTOR, 0))

        print("Cantidad de subprocesos:", pool_size)
        pool = multiprocessing.Pool(pool_size)

        results = []    # Results de subprocesos.
        idx_id = 1      # id de subindexer.

        for fnames in self.__child_indexers_files:
            dirout = self.__dirtmp + "//" + str(idx_id)

            indexer = ChildIndexer(idx_id, fnames, dirout, self.__corpus_type)
            result = pool.apply_async(indexer.create_index)
            results.append(result)
            # self.__child_indexes.extend(indexer.create_index())
            idx_id += 1
        # Espera de finalización de subprocesos.
        pool.close()
        pool.join()

        # Obtención de índices.
        results = [r.get() for r in results]

        for indexes in results:
            self.__child_indexes.extend([index for index in indexes])

        del pool

    def __merge_child_collections(self):
        '''Realiza merge de colecciones de docs de índices hijos.'''
        merged_collection = []
        print("\nMergeando colecciones...")
        start = time.time()
        merged_collection = {}
        for index in self.__child_indexes:
            collection_by_index = index.get_collection()
            merged_collection.update(collection_by_index)

        self._dump_docs_dict(merged_collection, self.index)
        end = time.time()
        print("Tiempo total de merge de colecciones:", round(end-start, 2))

        # Liberación de memoria.
        del merged_collection

    def clear_temporal_indexes(self):
        '''Elimina archivos temporales creados por los subíndices hijos.'''
        # Eliminación de archivos individuales de índice.
        for index in self.__child_indexes:
            os.remove(index.collection_path)
            os.remove(index.vocabulary_path)
            os.remove(index.postings_path)
            os.rmdir(os.path.dirname(index.collection_path))

        # Eliminación de carpeta tmp.
        if os.path.exists(self.__dirtmp):
            os.rmdir(self.__dirtmp)

    def __create_vocabulary_by_child_indexes(self):
        '''Retorna un diccionario de términos, donde cada key contiene a una
        lista que indica en qué índices es posible encontrar dicho término.

        Returns:
            terms (dict): diccionario de terms e índices.
        '''
        terms = {}
        indexes = self.__child_indexes

        terms = {}
        for i in range(0, len(indexes)):
            terms_by_index = indexes[i].get_vocabulary()

            for term in terms_by_index:
                if term not in terms.keys():
                    terms[term] = []
                terms[term].append(i)
        return terms

    def __print_merge_progress(self, term_id, terms_size):
        '''Imprime en pantalla el progreso de merge de términos.'''
        print("Merged {0}/{1}".format(term_id, terms_size))

    def _write_encode_and_chunk_info(self):
        '''Escribe el tamaño de particionado (chunk) utilizado y los tipos de
        codificación (monoencode) usados en los docs y las freqs.'''
        # Se asume doc_encode y freq_encode como multicompression.
        doc_encode = 0
        freq_encode = 0
        if not self._multiencode:
            doc_encode = self.doc_encode.value
            freq_encode = self.freq_encode.value

        encode_info = (doc_encode << 4) + freq_encode

        self._cwriter.begin_block(use_gaps=False)
        etype = EncodeTypes.ByteBlocks

        # Escritura de chunk size.
        self._cwriter.write([self._chunk_size], etype=etype, block_size=4)

        # Escritura de encode info.
        self._cwriter.write([encode_info], etype=etype, block_size=1)
        self._cwriter.close_block()

    def __merge_child_postings(self):
        '''Mergea postings de índices hijos.'''
        print("\nMergeando postings...")

        indexes = self.__child_indexes

        indexes_by_terms = self.__create_vocabulary_by_child_indexes()

        # Archivos de postings list (perm. lectura secuencial, evitando seeks).
        pfiles = [IndexStreamReader(i.postings_path) for i in indexes]

        # Writer de merge de posting.
        self._pwriter = IndexStreamWriter(self.index.postings_path)

        # Writer de merge de chunks data.
        self._cwriter = IndexStreamWriter(self.index.chunksinfo_path)

        # Writer de merge de vocabulario: buffer de escritura de 5 MiB.
        vocabulary_path = self.index.vocabulary_path
        self._vwriter = open(vocabulary_path, "w", buffering=5*(1024**2))

        # Escritura de info de encode.
        self._write_encode_and_chunk_info()

        term_id = 1
        terms = sorted(indexes_by_terms.keys())
        for term in terms:
            if term_id % 25000 == 0:
                self.__print_merge_progress(term_id, len(terms))

            merged = Counter()

            # Nota: indexes_by_terms[term] contiene lista de índices en donde
            # se puede encontrar el término.
            for index in indexes_by_terms[term]:
                pointer = indexes[index].get_posting_pointer_by_term(term)
                for c in pointer.chunks_info:
                    # Nota: se lee directamente en vb (método en el que indexan
                    # los subindexers) para evitar branchs de la función read()
                    # en la verificación del encode especificado.

                    # Lectura de docs y gapsdecode.
                    docs = gapsenc.decode(pfiles[index].read_vb(c.docs_size))

                    # Lectura de freqs (no gapsdecode).
                    freqs = pfiles[index].read_vb(c.freqs_size)

                    # Merge de chunks.
                    merged.update(dict(zip(docs, freqs)))

            # Eliminación de lista leída ¿liberación de memoria?
            del indexes_by_terms[term]

            self._append_to_vocabulary_and_posting(term_id, term, merged)
            term_id += 1

        self.__print_merge_progress(len(terms), len(terms))

        # Close de archivo de vocabulario.
        self._vwriter.close()
        self._vwriter = None

        # Flush y close de writer de posting.
        self._pwriter.flush()
        self._pwriter.close()
        self._pwriter = None

        self._cwriter.flush()
        self._cwriter.close()
        self._cwriter = None

        # Close de archivo de estadísticas de codificación.
        if self._multiencode_stats_docs_file:
            self._multiencode_stats_docs_file.close()
            self._multiencode_stats_docs_file = None

        if self._multiencode_stats_freqs_file:
            self._multiencode_stats_freqs_file.close()
            self._multiencode_stats_freqs_file = None

        # Liberación de memoria.
        del indexes_by_terms
        del terms

    def __merge_child_indexes(self):
        '''Mergea los índices creados por los indexadores hijos.'''
        utils.makedirs(self._dirout)

        # Cálculo de cantidad de índices de los que se cargarán la información
        # de chunks a RAM.
        child_count = len(self.__child_indexes)
        cdata_indexes_to_load = int(round(child_count*RESOURCES_FACTOR, 0))

        print("\nMergeando...")
        info = "Total de subíndices: {0} ({1}/{0} con chunks info en RAM)."
        print(info.format(child_count, cdata_indexes_to_load))
        print("Cargando subíndices...")

        # Load de indexadores hijos.
        for i in range(0, len(self.__child_indexes)):
            index = self.__child_indexes[i]

            if not index.is_loaded():
                index.load(chunks_info_in_memory=cdata_indexes_to_load > 0)
                cdata_indexes_to_load -= 1
                info = "Subíndice {0}/{1} cargado."
                print(info.format(i+1, child_count))

        self.__merge_child_collections()
        self.__merge_child_postings()

    def _write_multiencode_stats(self, term, ftype, etype, numbers):
        '''Escribe estadísticas de codificación de postings.

        Args:
            term (string): término al que corresponde la lista de números.
            ftype (PostingFieldTypes): tipo de archivo de stats a escribir
                (docs o freqs).
            etype (EncodeTypes): codificación utilizada.
            numbers (int list): números escritos con la codificación dado.
        '''
        writer = None

        if ftype == PostingFieldTypes.Docs:
            writer = self._multiencode_stats_docs_file
            if not writer:
                path = self._dirout + "//other//encoder_docs_statistics.txt"
                utils.makedirs(os.path.dirname(path))
                writer = open(path, "w")
                self._multiencode_stats_docs_file = writer
        else:
            writer = self._multiencode_stats_freqs_file
            if not writer:
                path = self._dirout + "//other//encoder_freqs_statistics.txt"
                utils.makedirs(os.path.dirname(path))
                writer = open(path, "w")
                self._multiencode_stats_freqs_file = writer

        data = "Term: " + term + "; "
        data += "EncodeType: " + str(etype.name) + "; "
        data += "Numbers: " + str(numbers) + "\n"
        writer.write(data)

    def _write_multiencode_block(self, term, pftype, numbers, etypes):
        '''Escribe un bloque de números utilizando multiencode.

        Args:
            term (string): término al que corresponde la lista de números.
            pftype (PostingFieldTypes): tipo de posting field (docs o freqs).
            numbers (int list): lista de números a escribir.
            etypes (EncodeTypes list): posibles codificaciones a utilizar.

        Returns:
            size (int): tamaño (en bytes) de la escritura realizada.
            etype (int): codificación utilizada.
        '''
        # Utilización de gaps sólo en caso de docs.
        use_gaps = pftype == PostingFieldTypes.Docs
        start_byte = self._pwriter.begin_block(use_gaps=use_gaps)

        etype = self._pwriter.multiencode_write(numbers, etypes)
        end_byte, _ = self._pwriter.close_block()
        size = end_byte - start_byte

        if self._has_to_write_multiencode_stats:
            self._write_multiencode_stats(term, pftype, etype, numbers)

        return size, etype

    def _write_monoencode_block(self, _, pftype, numbers, etype):
        '''Escribe un bloque de números utilizando una única codificación.

        Args:
            _ (string): paramétro dummy (term), útil para que los métodos de
                escritura mono y multi encode tengan la misma cantidad de
                parámetros, preservando la invocación vía caller.
            pftype (PostingFieldTypes): tipo de posting field (docs o freqs).
            numbers (int list): lista de números a escribir.
            encodes (EncodeTypes list): posibles codificaciones a utilizar.

        Returns:
            size (int): tamaño (en bytes) de la escritura realizada.
            etype (int): codificación utilizado (dummy).
        '''
        use_gaps = pftype == PostingFieldTypes.Docs
        start_byte = self._pwriter.begin_block(use_gaps=use_gaps)
        self._pwriter.write(numbers, etype)
        end_byte, _ = self._pwriter.close_block()

        size = end_byte - start_byte

        enc = self.doc_encode
        if pftype == PostingFieldTypes.Freqs:
            enc = self.freq_encode

        return size, enc

    def _append_to_vocabulary_and_posting(self, term_id, term, docs):
        '''Agrega vocabulario y posting a los archivos correspondientes.

        Args:
            term_id (int): identificador de término.
            term (string): termino (literal).
            docs (dict): dict de docs del término y sus freqs.
        '''
        chunk_size = len(docs) if self._chunk_size == 0 else self._chunk_size
        doc_keys = sorted(list(docs))
        freqs = [docs[doc_id] for doc_id in doc_keys]

        # Escritura de info de chunks.
        cinfo_start_byte = self._cwriter.begin_block(use_gaps=False)

        # Nota. se supone inicio en byte = 0, por anterior end de otro bloque.
        pstart = self._pwriter.tell()[0]

        pcount = len(doc_keys)-1
        self._cwriter.write([pstart, pcount], etype=EncodeTypes.VariableByte)

        # Escritura de posting en chunks, según chunk_size.
        for i in range(0, len(doc_keys), chunk_size):
            j = i + chunk_size

            # Escritura de documentos.
            pftype = PostingFieldTypes.Docs
            returned = self._write_docs_caller(term, pftype, doc_keys[i:j],
                                               self.doc_encode)
            docs_size, doc_enc = returned

            # Escritura de frecuencias.
            pftype = PostingFieldTypes.Freqs
            returned = self._write_freqs_caller(term, pftype, freqs[i:j],
                                                self.freq_encode)
            freqs_size, freq_enc = returned

            # En caso de esquema multicompresión...
            if self._multiencode:
                # Información para descompresión del chunk.
                encode_info = (doc_enc.value << 4) + freq_enc.value
                etype = EncodeTypes.ByteBlocks
                self._cwriter.write([encode_info], etype=etype, block_size=1)

            self._cwriter.write([docs_size, freqs_size],
                                etype=EncodeTypes.VariableByte)

        cinfo_end_byte = self._cwriter.close_block()[0]
        cinfo_size = cinfo_end_byte - cinfo_start_byte

        vocabulary = str(term_id) + "\t" + term + "\t"
        vocabulary += str(cinfo_start_byte) + "\t" + str(cinfo_size) + "\n"
        self._vwriter.write(vocabulary)

    def _dump_docs_dict(self, docs, index):
        '''Vuelca a disco la colección de documentos pasada por parámetro.

        Args:
            docs (dict): hash de documentos.
            index (Index): índice donde se deberán volcar los docs.
        '''
        writer = open(index.collection_path, "w")

        for doc_id in docs:
            # doc_id, doc_name
            writer.write(str(doc_id) + "\t" + str(docs[doc_id]) + "\n")

        writer.close()

    def __try_load_child_indexes(self):
        '''Verifica si existen archivos temporales de índices hijos creados en
        anteriores indexaciones. En caso de ser así, los carga.'''
        if not os.path.isdir(self.__dirtmp):
            return

        child_dirs = next(os.walk(self.__dirtmp))[1]

        if not self.__child_indexes:
            for i in child_dirs:
                full_dir = self.__dirtmp + "//" + i
                index = Index(full_dir)
                self.__child_indexes.append(index)
        return self.__child_indexes

    def create_index(self, dirout, overwrite=False, chunk_size=0):
        '''Realiza indexación en base a directorio de in.

        Args:
            dirout (string): directorio de salida.
            overwrite (bool): indica si se debe sobrescribir el índice en caso
                              de que ya exista.
            chunk_size (int): tamaño máximo de chunks de postings. Si es 1, la
                posting se almacenará se almacenará 'contigua'.

        Returns:
            index (Index): índice generado.
            status (IndexStatusTypes): cód. de estado respecto a la generación
                del índice (ya indexado, directorio de in no existente, ok).
        '''
        print("Indexación en progreso...")

        # Info de multicompresión.
        doc_multiencode = isinstance(self.doc_encode, list)
        freq_multiencode = isinstance(self.freq_encode, list)
        self._multiencode = doc_multiencode or freq_multiencode

        # Selección de caller de writer de encoded.
        if doc_multiencode:
            self._write_docs_caller = self._write_multiencode_block
        else:
            self._write_docs_caller = self._write_monoencode_block

        if freq_multiencode:
            self._write_freqs_caller = self._write_multiencode_block
        else:
            self._write_freqs_caller = self._write_monoencode_block

        # Escritura de stats en caso de multiencode en docs y freqs.
        if doc_multiencode and freq_multiencode:
            self._has_to_write_multiencode_stats = True

        # Directorio de salida de índice.
        self._dirout = dirout

        # Tamaño de chunk.
        self._chunk_size = chunk_size

        # Índice.
        self.index = Index(self._dirout)

        # Si ya existe el índice y no se permite sobre-escritura...
        if self.index.exists() and not overwrite:
            print("Ya indexado...\n")
            # Retorno de índice + error de ya indexado.
            return self.index, IndexerStatusTypes.Already_Indexed

        # Si no existe dirin...
        if not os.path.exists(self.__dirin):
            print("Directorio de colección inexistente...\n")
            # Retorna de null + error de colección no existente.
            return self.index, IndexerStatusTypes.Collection_Non_Existent

        # Si se deben reusar archivos temporales anteriores...
        if self.__reuse_tmp:
            self.__try_load_child_indexes()

        if not self.__child_indexes:
            print("Creando subíndices temporales...")
            start = time.time()
            self.__create_child_indexes()
            end = time.time()
            print("Tiempo de creación de subíndices temporales:", round(end-start, 2))
        else:
            print("Archivos de subíndices temporales existentes.")

        start = time.time()
        if self.__child_indexes:
            self.__merge_child_indexes()
            end = time.time()
        else:
            ex = "Imposible realizar merge de índice (not exists childs)"
            raise Exception(ex)

        info = ">> Tiempo de merge de índice: " + str(round(end-start, 2))
        info += "\n\n>> Tamaño de archivos: "
        info += "\nCollection: {0} MiB"
        info += "\nChunks info: {1} MiB"
        info += "\nVocabulary: {2} MiB"
        info += "\nPostings: {3} MiB"

        coll_size = ((os.path.getsize(self.index.collection_path))/1024)/1024
        chin_size = ((os.path.getsize(self.index.chunksinfo_path))/1024)/1024
        voca_size = ((os.path.getsize(self.index.vocabulary_path))/1024)/1024
        post_size = ((os.path.getsize(self.index.postings_path))/1024)/1024
        info = info.format(round(coll_size, 1), round(chin_size, 1),
                           round(voca_size, 1), round(post_size, 1))

        print("\n" + info + "\n")
        utils.makedirs(self._dirout + "//other/")
        fstatus = open(self._dirout + "//other/status.txt", "w")
        fstatus.write(info)
        fstatus.close()

        # Retorno de index y status = ok.
        return self.index, IndexerStatusTypes.Ok


class ChildIndexer(Indexer):

    def __init__(self, indexer_id, fnames, dirout, corpus_type):
        '''Inicializa subindexador.
        Args:
            indexer_id (int): identificador de sub-indexador.
            fnames (string list): lista de archivos a procesar.
            dirout (string): directorio de salida.
            corpus_type (CorpusTypes): tipo de corpus.
        '''
        # Lista de índices generados.
        self.indexes = []

        # Los childs codifican los subíndices en VB, ya que posee un tiempo de
        # escritura/lectura bastante rápido en relación a otras codificaciones.
        self.doc_encode = EncodeTypes.VariableByte
        self.freq_encode = EncodeTypes.VariableByte

        # Writer callers.
        self._multiencode = False
        self._write_docs_caller = self._write_monoencode_block
        self._write_freqs_caller = self._write_monoencode_block

        self._dirout = dirout
        self._chunk_size = 0
        self.__indexer_id = indexer_id
        self.__corpus_type = corpus_type
        self.__fnames = fnames

        # Tag remover (para corpus html).
        self.__html_tag_remover = re.compile(r'<[^>]+>')

        # Var. que permite medir el tiempo transcurrido cada vez que se invoca
        # al método '__print_indexer_info()'.
        self.__start_bench_info_date = None

        # Archivo de estadísticas de encode (dummy: 'super' utiliza esta var).
        self._multiencode_stats_docs_file = None
        self._multiencode_stats_freqs_file = None

    def __remove_tags(self, text):
        '''Elimina tags html de texto especificado.

        Args:
            text (string): texto del que se requieren eliminar tags html.

        Returns:
            parsed (string): texto sin tags html.
        '''
        return self.__html_tag_remover.sub('', text)

    def __check_term(self, terms, doc_id, token):
        '''Verifica si un token es término y, en caso positivo, lo agrega al
        hash de términos.

        Args:
            terms (dict): hash de términos.
            doc_id (int): id de documento.
            token (string): token a verificar.
        '''
        if token:
            # Conversión de token a término.
            term = Tokenizer.convert2term(token)

            if term is not None:
                # Agregación de término (SPIMI - paso 1)
                if term not in terms:
                    terms[term] = Counter()

                # Agregación directa a posting (SPIMI - paso 2)
                terms[term][doc_id] += 1

    def __print_indexation_progress(self, doc_number, total_docs=None):
        '''Imprime en pantalla el progreso de indexación.'''
        indexer_id = self.__indexer_id
        end_date = time.time()
        elapsed = round(end_date-self.__start_bench_info_date, 2)

        # Si no se conoce el total de docs a procesar.
        if not total_docs:
            total_docs = "?"

        info = "Indexador {0}: {1}/{2} docs procesados. Tiempo: {3}..."
        info = info.format(indexer_id, doc_number, total_docs, elapsed)
        print(info)
        self.__start_bench_info_date = time.time()

    def _dump_terms_dict(self, terms, index):
        '''Vuelca a disco las postings pasadas por parámetro.

        Args:
            terms (dict): hash de términos.
            index (Index): índice donde se deberán volcar los terms.
        '''
        self._pwriter = IndexStreamWriter(index.postings_path)
        self._cwriter = IndexStreamWriter(index.chunksinfo_path)

        # Writer: buffer de escritura de 5 MiB.
        buffering = 5*(1024**2)
        self._vwriter = open(index.vocabulary_path, "w", buffering=buffering)

        # Escritura de info de compresión.
        self._write_encode_and_chunk_info()

        term_id = 1
        for term in sorted(terms):
            # Documentos.
            docs = terms[term]

            # Postings.
            super()._append_to_vocabulary_and_posting(term_id, term, docs)
            term_id += 1

        self._pwriter.flush()
        self._pwriter.close()
        self._pwriter = None

        self._cwriter.flush()
        self._cwriter.close()
        self._cwriter = None

        self._vwriter.close()
        self._vwriter = None
        terms = {}

    def _save(self, docs, terms, dirout):
        '''Genera índice en base a dicts de docs y terms pasados por parámetro.

        Args:
            docs (dict): hash de documentos.
            terms (dict): hash de términos.
            dirout (string): directorio de salida de índice.

        Returns:
            index (Index): término generado.
        '''
        utils.makedirs(dirout)

        index = Index(dirout)
        super()._dump_docs_dict(docs, index)
        self._dump_terms_dict(terms, index)

        return index

    def __write_indexation_info(self, dirout):
        '''Almacena información de subindexación en el dir de salida dado.

        Args:
            dirout (string): directorio de salida.
        '''
        utils.makedirs(dirout + "/other")
        fstatus = open("{0}/status.txt".format(dirout + "/other"), "w")

        info = "Creación de subíndice finalizada correctamente."
        fstatus.write(info)
        fstatus.close()

    def process_file_corpus(self, docs, terms):
        '''Procesa un corpus y agrega el índice generado a la lista correspondiente.

        Args:
            docs (dict): hash de documentos.
            terms (dict): has de términos.
        '''
        self.__start_bench_info_date = time.time()
        docs_processed = 0
        total_docs = len(self.__fnames)

        for fid in self.__fnames:
            if docs_processed % 5000 == 0:
                self.__print_indexation_progress(docs_processed, total_docs)

            # Docname, dado por basename de fid.
            docname = os.path.basename(self.__fnames[fid])
            docs[fid] = docname

            document_file = open(self.__fnames[fid])
            for line in document_file:
                if self.__corpus_type == CorpusTypes.Html:
                    line = self.__remove_tags(line)

                tokens = line.split(" ")

                for token in tokens:
                    self.__check_term(terms, fid, token)
            document_file.close()

            docs_processed += 1

        self.__print_indexation_progress(docs_processed, total_docs)

        # Volcado de índice a disco.
        index = self._save(docs, terms, self._dirout)
        self.indexes = [index]

        # Escritura de información de indexación.
        self.__write_indexation_info(self._dirout)

    def __flush_subindex(self, docs, terms, subindex_id):
        '''Vuelca subíndice hijo a disco.

        Args:
            docs (dict): hash de documentos.
            terms (dict): has de términos.
            subindex_id (int): identificador de subíndice a crear.
        '''
        info = "Indexador {0}: Dumping {1} docs, {2} terms..."
        info = info.format(self.__indexer_id, len(docs), len(terms))
        print(info)
        dirout = self._dirout + "-" + str(subindex_id)

        self.__write_indexation_info(dirout)
        index = self._save(docs, terms, dirout)
        self.indexes.append(index)

    def process_trec_corpus(self, docs, terms):
        '''Procesa archivos en formato TREC y agrega los índices creados a la
        lista correspondiente.

        Args:
            docs (dict): hash de documentos.
            terms (dict): has de términos.
        '''
        self.__start_bench_info_date = time.time()

        # Documentos procesados.
        docs_processed = 0

        # Identificador de documento.
        doc_id = -1

        # Flag que indica si se está leyendo el inicio de un documento.
        is_start_doc = False

        # Flag que indica si el documento leído es nuevo dentro del docs dict.
        # Nota: está flag es necesaria, pues se han detectado DOCNOs repetidos.
        is_new_doc = False

        # Indentificador de subíndice/s generado/s.
        subindex_id = 1

        for fid in self.__fnames:
            trec_file = open(self.__fnames[fid])
            for line in trec_file:
                line = line[:-1]

                # Lectura DOCNO en caso de que sea inicio documento...
                if is_start_doc:
                    # Obtención de doc_id
                    doc_id = int(self.__html_tag_remover.sub('', line))
                    is_start_doc = False

                    # El documento es nuevo.
                    if doc_id not in docs:
                        # Agreación a listado y set de flag en true...
                        docs[doc_id] = str(doc_id)
                        is_new_doc = True
                    continue
                # Si se trata de la lectura de un nuevo documento.
                elif line == "<DOC>":
                    is_start_doc = True
                    continue
                # Si ha finalizado procesamiento de un documento...
                elif line == "</DOC>":
                    if docs_processed % 50000 == 0:
                        self.__print_indexation_progress(docs_processed)

                        # Flush al superar la cantidad máx de docs en mem.
                        if len(docs) >= MAX_TREC_DOCS_IN_MEMORY:
                            self.__flush_subindex(docs, terms, subindex_id)
                            subindex_id += 1
                            del docs, terms
                            docs = {}
                            terms = {}

                    # Si el documento procesado es nuevo en el dict.
                    if is_new_doc:
                        docs_processed += 1
                        is_new_doc = False
                    continue

                # Verificación de tokes.
                tokens = line.split(" ")

                for token in tokens:
                    self.__check_term(terms, doc_id, token)

        self.__print_indexation_progress(docs_processed, docs_processed)

        # Si quedan docs o terms por grabar en los hash.
        if docs or terms:
            self.__flush_subindex(docs, terms, subindex_id)

        trec_file.close()

    def create_index(self):
        '''Realiza indexación en base a directorio de in.

        Returns:
            indexes (Index list): índices generados por el indexador hijo.
        '''
        # Hash de términos y documentos.
        terms = {}
        docs = {}

        if self.__corpus_type == CorpusTypes.Trec:
            self.process_trec_corpus(terms, docs)
        else:
            self.process_file_corpus(terms, docs)

        # Liberación de memoria.
        del docs, terms

        # Retorno de índices generados
        return self.indexes
