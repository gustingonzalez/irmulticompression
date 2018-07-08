#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: index.py
- Descripción: contiene las clases 'ChunkInfo', 'PostingPointer' e 'Index',
que permiten gestionar un corpus indexado.
- Autor: Agustín González
- Modificado: 18/04/18
'''

import os
import gc
import math
from array import array

from lib.index.compression.ircodecs import EncodeTypes
from lib.index.compression.ircodecs import vbencoder as vbenc
from lib.index.compression.indexstream import IndexStreamReader


class ChunkInfo(object):
    '''Información de chunk (partición) de posting list de término.'''

    def __init__(self, number=0):
        '''Inicializa información de chunk de posting.

        Args:
            number (int): número de chunk info. De utilidad para identificación
                 dentro de la lista de chunks de la clase 'PostingPointer'.
        '''
        self.number = number

        # Tamaño de chunk.
        self.chunk_size = 0

        # Encode de documento (EncodeTypes).
        self.docs_encode = None

        # Tamaño en bytes del bloque de docs.
        self.docs_size = 0

        # Encode de freqs (EncodeTypes).
        self.freqs_encode = None

        # Tamaño en bytes del bloque de freqs.
        self.freqs_size = 0


class PostingPointer(object):
    '''Puntero a posting list.'''

    def __init__(self, term_id, posting_start, posting_count):
        '''Inicializa puntero.

        Args:
            term_id (int): identificador de término.
            posting_start (int): byte de inicio de posting list.
            posting_count (int): cantidad de elementos de la posting list.
        '''
        self.term_id = term_id
        self.posting_start = posting_start
        self.posting_count = posting_count
        self.chunks_info = []

    def extend_chunks_info(self, chunks_info):
        '''Agrega una lista de información de chunks al puntero. Nota: este
        método sólo tiene fines de debug. A los efectos de obtener mayor
        eficiencia, la agregación de listados de chunks info, se realiza
        directamente con el método append() correspondiente.

        Args:
            chunks_info (ChunkInfo list): información de chunks de postings.
        '''
        # Validación
        numbers = [p.number for p in chunks_info]

        ex = "No pueden existir dos números de chunk info iguales."
        # Si el len es distinto, hay al menos dos números repetidos.
        if len(set(numbers)) != len(numbers):
            raise Exception(ex)

        # Sort y add.
        chunks_info.sort(key=lambda x: x.number)
        self.chunks_info = chunks_info


class Index(object):
    '''Índice.'''

    def __init__(self, dirindex):
        '''Inicializa clase.

        Args:
            dirindex (string): directorio de índice.
        '''
        # Path de colección.
        self.collection_path = dirindex + "/collection.txt"

        # Path de vocabulario.
        self.vocabulary_path = dirindex + "/vocabulary.txt"

        # Path de info de chunks.
        self.chunksinfo_path = dirindex + "/chunksinfo.bin"

        # Path de postings.
        self.postings_path = dirindex + "/postings.bin"

        # Indica si el índice ha sido cargado.
        self.__is_loaded = False

        # Tamaño de chunk utilizado.
        self.__chunk_size = 0

        # Indica si debe cargarse la información de chunks en memoria.
        self.__chunks_info_in_memory = False

        # Contador de chunks info en mem. (utilizado para fines estadísticos)
        self.__chunks_info_in_memory_count = 0

        # Colección (cargada en 'load()')
        self.__collection = {}

        # Vocabulario (cargado en 'load()')
        self.__vocabulary = {}

        # Datos de encode.
        self.__multiencode = False
        self.__doc_encode = 0
        self.__freq_encode = 0

        # Caller de parser de chunk info. Su valor dependerá de si el índice es
        # o no multiencode.
        self.__parse_raw_chunk_info_caller = None

        # Caller que permite computar los tamaños de chunks del índice.
        self.__compute_chunk_sizes_caller = None

        # Caller de get de chunks info.
        self.__get_raw_chunks_info_caller = None

    def exists(self):
        '''Verifica si existe el índice.

        Returns:
            True si existe el índice, False en caso contrario.
        '''
        exists = os.path.exists(self.collection_path)
        exists = exists and os.path.exists(self.vocabulary_path)
        exists = exists and os.path.exists(self.chunksinfo_path)
        exists = exists and os.path.exists(self.postings_path)

        return exists

    def is_multiencode(self):
        '''Indica si el índice es de tipo multiencode.

        Returns:
            True, si el índice es multiencode, False en caso contrario.
        '''
        self.__check_load()
        return self.__multiencode

    def get_chunks_info_in_memory_count(self):
        '''Retorna la cantidad de chunks en memoria.

        Returns:
            chunks_info_in_memory_count (int): cantidad de chunks info en mem.
                Es 0 (cero) si la variable 'chunks_info_in_memory' es False.
            chunks_info_in_memory (bool): flag que indica si la información de
                chunks ha sido cargada en memoria.
            chunk_vector_size: tamaño de cada uno de los vectores de chunks. 3
                si es multiencode, 2 si es monoencode.
        '''
        self.__check_load()

        # Tamaño de vector de chunk.
        # Presunción de multiencode (3 enteros).
        cvector_size = 3

        # Si el índice no es multiencode...
        if not self.is_multiencode():
            # Eliminación de entero de encode.
            cvector_size -= 1

        cinfo_in_memory_count = self.__chunks_info_in_memory_count
        cinfo_in_memory = self.__chunks_info_in_memory

        return cinfo_in_memory_count, cinfo_in_memory, cvector_size

    def __check_load(self):
        '''Verifica si el índice se encuentra cargado. Retorna una excepción
        en caso de que no.'''
        if self.__is_loaded:
            return

        ex = "El índice no se encuentra cargado (es necesario invocar el "
        ex += "método 'load()' previamente)."
        raise Exception(ex)

    def __load_collection(self):
        '''Carga colección de documentos en memoria.'''
        if self.__collection:
            return

        fcollection = open(self.collection_path)
        for line in fcollection:
            # -1 elimina \n
            spplited = line[:-1].split("\t")
            # id, nombre.
            self.__collection[int(spplited[0])] = spplited[1]

        fcollection.close()

    def __parse_raw_multiencode_chunk_info(self, to_parse, chunk_size, number=0):
        '''Parsea una lista de 4 enteros (chunk info 'crudo') a un obj. de tipo
        'ChunkInfo'.

        Args:
            number (int): número de chunk info.
            chunk_size (int): tamaño de chunk.
            to_parse (int list): puntero a parsear. Contiene, en el siguiente
                orden: encode types, docs size y freqs size.

        Returns:
            chunk_info(ChunkInfo): información de chunks parseada.
        '''
        chunk_info = ChunkInfo(number)
        chunk_info.chunk_size = chunk_size
        chunk_info.docs_size = to_parse[1]
        chunk_info.freqs_size = to_parse[2]

        # Descompresión de encode_types
        etypes = self.__parse_compressed_etypes(to_parse[0])
        chunk_info.docs_encode = EncodeTypes(etypes[0])
        chunk_info.freqs_encode = EncodeTypes(etypes[1])

        return chunk_info

    def __parse_raw_monoencode_chunk_info(self, to_parse, chunk_size, number=0):
        '''Parsea una lista de 3 enteros (chunk info 'crudo') a un obj. de tipo
        'ChunkInfo'.

        Args:
            number (int): número de chunk info.
            chunk_size (int): tamaño de chunk.
            to_parse (int list): puntero a parsear. Contiene, en el siguiente
                orden: docs size y freqs size.

        Returns:
            chunk_info(ChunkInfo): información de chunks parseada.
        '''
        chunk_info = ChunkInfo(number)
        chunk_info.chunk_size = chunk_size
        chunk_info.docs_size = to_parse[0]
        chunk_info.freqs_size = to_parse[1]

        # Información de tipos de encodes utilizados.
        chunk_info.docs_encode = self.__doc_encode
        chunk_info.freqs_encode = self.__freq_encode

        return chunk_info

    def __get_raw_multiencode_chunks_info(self, reader, size):
        '''Obtiene una lista de chunks info donde cada elemento es un array de
        3 enteros.

        Args:
            reader (IndexStreamReader): reader de chunks info.
            size (int): cantidad de bytes a leer.

        Returns:
            posting_start (int): puntero al primer chunk info de posting list.
            posting_count (int): cantidad de elementos de la posting list.
            raw_chunks_info (list array int): lista de raw chunks donde cada
                elemento contiene, en el sig. orden: encode types, docs size y
                freqs size.
        '''
        raw = reader.raw_read(size)

        # Extracción de posting start y posting count.
        pstart, offset = vbenc.decode_number(raw)
        pcount, offset = vbenc.decode_number(raw, offset)

        # Decodificación de raw.
        raw_index = offset >> 3  # offset/8
        decoded_raw = []
        while raw_index < len(raw):
            # Encode types
            decoded_raw += [raw[raw_index]]
            offset += 8

            # 2, 3: {docs_size, freqs_size}
            for _ in range(0, 2):
                returned = vbenc.decode_number(raw, offset)
                decoded_raw += [returned[0]]
                offset = returned[1]
            raw_index = offset >> 3  # int(offset/8)

        raw_chunks_info = []
        for i in range(0, len(decoded_raw), 3):
            etypes = decoded_raw[i]
            docs_size = decoded_raw[i+1]
            freqs_size = decoded_raw[i+2]

            # Array de ints de máximo 31 bits por elemento.
            chunk_info = array('i')
            chunk_info.extend([etypes, docs_size, freqs_size])

            raw_chunks_info.append(chunk_info)

        return pstart, pcount, raw_chunks_info

    def __get_raw_monoencode_chunks_info(self, reader, size):
        '''Obtiene una lista de chunks info donde cada elemento es un array de
        2 enteros.

        Args:
            reader (IndexStreamReader): reader de chunks info.
            size (int): cantidad de bytes a leer.

        Returns:
            posting_start (int): puntero al primer chunk info de posting list.
            posting_count (int): cantidad de elementos de la posting list.
            raw_chunks_info (list array int): lista de raw chunks donde cada
                elemento contiene, en el sig. orden: docs size y freqs size.
        '''
        raw = reader.read(size, etype=EncodeTypes.VariableByte, use_gaps=False)

        # Extracción de posting start.
        pstart = raw.pop(0)

        # Decodificación de cantidad de elementos de posting.
        pcount = raw.pop(0)

        raw_chunks_info = []

        for i in range(0, len(raw), 2):
            docs_size = raw[i]
            freqs_size = raw[i+1]

            chunk_info = array('i')
            chunk_info.extend([docs_size, freqs_size])

            raw_chunks_info.append(chunk_info)

        return pstart, pcount, raw_chunks_info

    def __parse_compressed_etypes(self, etypes):
        '''Parsea los tipos de codificación comprimidos a EncodeTypes.

        Args:
            etypes (byte): tipos de codificación (doc y freq) comprimidos.

        Returns
            doc_encode (EncodeTypes): codificación de bloques de docs.
            freq_encode (EncodeTypes): codificación de bloques de freqs.
        '''
        doc_encode = EncodeTypes((etypes & 0b11110000) >> 4)
        freq_encode = EncodeTypes(etypes & 0b0001111)

        return doc_encode, freq_encode

    def __load_index_data(self, reader):
        '''Carga el tamaño de chunk, doc y freq encode del índice, asignando
        los callers de parseo y obtención de información de chunks apropiados
        (métodos que se comportan diferente según se trate de un índice mono
        o multiencode). También asigna el caller que permite calcular la
        cantidad de elementos a leer de una posting, ya que se pueden utilizar
        o no chunks.

        Args:
            reader (IndexStreamReader): reader de chunks info.
        '''
        reader.seek(0)
        etype = EncodeTypes.ByteBlocks
        self.__chunk_size = reader.read(4, etype=etype, block_size=4)[0]
        etypes = reader.read(1, etype=etype, block_size=1)[0]

        # Nota: la asignación de callers permite evitar branchs ifs.
        # 1. Asignación de caller de parser de información de chunk.
        self.__multiencode = True
        caller = self.__parse_raw_multiencode_chunk_info

        # Si los tipos de encode están definidos en el encabezado...
        if etypes:
            self.__multiencode = False
            returned = self.__parse_compressed_etypes(etypes)
            self.__doc_encode, self.__freq_encode = returned
            caller = self.__parse_raw_monoencode_chunk_info

        self.__parse_raw_chunk_info_caller = caller

        # 2. Asignación de caller de getter de chunk info.
        caller = self.__get_raw_monoencode_chunks_info
        if self.__multiencode:
            caller = self.__get_raw_multiencode_chunks_info

        self.__get_raw_chunks_info_caller = caller

        # 3. Asignación de caller para computar los tamaños de chunks.
        caller = self.__compute_chunk_size_if_not_chunks
        if self.__chunk_size != 0:
            caller = self.__compute_chunk_sizes_if_chunks
        self.__compute_chunk_sizes_caller = caller

    def __compute_chunk_size_if_not_chunks(self, posting_count):
        '''Método dummy que tiene el objetivo de preservar el llamado a
        'compute_chunk_sizes_caller', para evitar branchs (ifs), cuando no se
        utilizan chunks (es decir, cuando el 'chunk size' del índice es 0).

        Args:
            posting_count (int): cantidad de elementos de la posting list.

        Returns:
            chunk_size (int list): tamaño de chunk (igual a 'posting count').
        '''
        return [posting_count]

    def __compute_chunk_sizes_if_chunks(self, posting_count):
        '''Calcula los tamaños de chunk en base a la cantidad de elementos de
        la posting list pasada por parámetro. Nota: invocar la función sólo en
        caso de que el 'chunk size' del índice sea mayor a 0. En cualquier otro
        es necesario invocar el método 'compute_chunk_size_if_not_chunks'.

        Args:
            posting_count (int): cantidad de elementos de la posting list.

        Returns:
            chunk_sizes (int): tamaños de cada uno de los chunks de la posting.
        '''
        csize = self.__chunk_size
        chunk_sizes = [csize] * math.ceil(posting_count/csize)

        # Si posting count no es divisible por chunk size...
        mod = posting_count % csize
        if mod:
            chunk_sizes[-1] = mod

        return chunk_sizes

    def __load_vocabulary(self):
        '''Carga vocabulario en memoria.'''
        if self.__vocabulary:
            return

        fvocabulary = open(self.vocabulary_path)

        # Reader secuencial.
        reader = IndexStreamReader(self.chunksinfo_path)

        # Carga de encodes y chunk size de índice.
        self.__load_index_data(reader)

        for line in fvocabulary:
            # -1 elimina \n
            spplited = line[:-1].split("\t")

            term_id = int(spplited[0])
            literal = spplited[1]
            cstart = int(spplited[2])
            csize = int(spplited[3])

            if self.__chunks_info_in_memory:
                # Carga de chunks info en memoria.
                r = self.__get_raw_chunks_info_caller(reader, csize)
                self.__vocabulary[literal] = [term_id, r[0], r[1], r[2]]
                self.__chunks_info_in_memory_count += len(r[2])
            else:
                reader.seek(cstart)
                # Carga de puntero a chunks info.
                self.__vocabulary[literal] = [term_id, cstart, csize]

        reader.close()
        fvocabulary.close()
        gc.collect()

    def is_loaded(self):
        '''Indica si el índice está cargado.

        Returns:
            True, si el índice se encuentra cargado, False en caso contrario.
        '''
        return self.__is_loaded

    def load(self, chunks_info_in_memory=False):
        '''Carga la colección y el vocabulario en memoria.

        Args:
            chunks_info_in_memory (bool): indica si la información de chunks se
                debe cargar en RAM. Por omisión, en False.
        '''
        if not self.__is_loaded:
            self.__chunks_info_in_memory = chunks_info_in_memory
            self.__load_collection()
            self.__load_vocabulary()
            self.__is_loaded = True
        else:
            raise Exception("El índice ya se encuentra cargado.")

    def get_collection(self):
        '''Retorna la colección de documentos del índice.

        Returns:
            collection (dict): colección de docs (doc_id, doc_name).
        '''
        return self.__collection

    def get_doc_by_id(self, doc_id):
        '''Retorna nombre de documento según id.

        Args:
            doc_id (int): identificador de documento.

        Returns:
            doc (string): nombre de documento.
        '''
        self.__check_load()
        return self.__collection[doc_id]

    def get_vocabulary(self):
        '''Retorna vocabulario.

        Returns:
            vocabulary (dict): colección de términos (term, list).
        '''
        return self.__vocabulary

    def __get_posting_from_chunks_info(self, posting_start, chunks_info):
        '''Retorna posting list en base al byte de inicio de posting e info de
            chunks dados.

        Args:
            posting_start (int): byte de inicio de chunks de postings.
            chunks_info (ChunkInfo list): listado de info de chunks.

        Returns:
            posting (dict): posting list desde el inicio de posting dado.
        '''
        # Nota: ver verificación de elementos repetidos al final del método.
        # total_docs = []
        # Seek de reader en inicio de posting.
        reader = IndexStreamReader(self.postings_path)
        reader.seek(posting_start)

        posting = {}
        for c in chunks_info:
            docs = reader.read(c.docs_size, c.chunk_size, c.docs_encode)
            freqs = reader.read(c.freqs_size, c.chunk_size,
                                c.freqs_encode, use_gaps=False)

            posting.update(dict(zip(docs, freqs)))
            # total_docs += docs
        reader.close()

        # if len(total_docs) != len(set(total_docs)):
        #    ex = "Atención: los chunks tienen elementos repetidos entre sí. "
        #    ex += "Docs: " + str(total_docs)
        #    raise Exception(ex)
        return posting

    def get_posting_pointer_by_term(self, term):
        '''Retorna puntero a posting en base a término literal dado por param.

        Args:
            literal (string): término literal.

        Returns:
            pointer (PostingPointer): puntero a posting list de término.
        '''
        # self.__check_load()
        pinfo = self.__vocabulary.get(term, None)

        if not pinfo:
            return []
        elif not self.__chunks_info_in_memory:
            # Obtención de info de posting chunk desde disco.
            reader = IndexStreamReader(self.chunksinfo_path)
            reader.seek(pinfo[1])
            returned = self.__get_raw_chunks_info_caller(reader, pinfo[2])
            pstart, pcount, raw_chunks_info = returned
        else:
            # Obtención de info de posting chunk desde memoria.
            pstart = pinfo[1]
            pcount = pinfo[2]
            raw_chunks_info = pinfo[3]

        # Add de 1 a posting count ya que, durante el write, se le sustrae 1.
        pcount += 1

        # Pointer: term_id, posting_start.
        pointer = PostingPointer(pinfo[0], pstart, pcount)

        # Parseo de chunks info.
        parsed_chunks_info = []

        # Cálculo de tamaños de chunks.
        chunk_sizes = self.__compute_chunk_sizes_caller(pointer.posting_count)

        number = 1
        for i in range(0, len(raw_chunks_info)):
            to_parse = raw_chunks_info[i]
            chunk_size = chunk_sizes[i]
            parsed = self.__parse_raw_chunk_info_caller(to_parse, chunk_size,
                                                        number)
            parsed_chunks_info.append(parsed)
            number += 1

        pointer.chunks_info.extend(parsed_chunks_info)
        return pointer

    def get_posting_by_term(self, term):
        '''Retorna la posting list del término pasado por parámetro.

        Args:
            term (string): término del que se requiere la posting list.

        Returns:
            posting (int list): posting list del término.
        '''
        # if not term:
        #    return {}

        pointer = self.get_posting_pointer_by_term(term)
        if not pointer:
            return {}

        # Posting list.
        posting = self.__get_posting_from_chunks_info(pointer.posting_start,
                                                      pointer.chunks_info)

        return posting
