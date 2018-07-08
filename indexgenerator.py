#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexgenerator.py
- Descripción: generador de índices. El script puede ser utilizado de 2 formas:
    1. Invocandólo directamente con los argumentos necesarios (ver docstring de
    procedimiento 'create_index_by_args').
    2. Vía archivo de config 'config/xxxx.ini', que permite indexar una serie
    de índices, uno tras otro.
- Autor: Agustín González
- Modificado: 19/02/17
'''

import sys
from configparser import ConfigParser
from lib.index.indexer import Indexer, CorpusTypes
from lib.index.compression.encoder import EncodeTypes


def parse_encode(plain_encodes):
    '''Parsea configuración de codificaciones.

    Args:
        plain_encodes (string): lista de codificación/es en formato plano.

    Returns:
        encode/s (EncodeTypes o EncodeTypes list): tipo de codificación o lista
            de codificaciones a utilizar, según corresponda.
    '''
    encodes = [x.strip() for x in plain_encodes.split(",")]

    if len(encodes) == 1:
        return EncodeTypes[encodes[0]]

    parsed_encodes = []
    for encode in encodes:
        parsed_encodes.append(EncodeTypes[encode])
    return parsed_encodes


def create_index_by_args(args):
    '''Permite generar un índice en base a los argumentos de inicio de script.

    Args:
        args[1]: directorio de colección
        args[2]: directorio de salida.
        args[3]: tamaño de chunks a utilizar para el índice.
        args[4]: encode/s de documentos a utilizar.
        args[5]: encode/s de frecuencias a utilizar.
        args[6]: tipo de corpus (Plain/Html/Trec).
    '''
    dirin = args[1]
    dirout = args[2]
    chunk_size = int(args[3])
    docs_encode = parse_encode(args[4])
    freqs_encode = parse_encode(args[5])
    corpus_type = CorpusTypes[args[6]]

    indexer = Indexer(dirin, corpus_type, reuse_tmp=True)
    indexer.doc_encode = docs_encode
    indexer.freq_encode = freqs_encode

    indexer.create_index(dirout, chunk_size=chunk_size)


def parse_indexes_to_generate(config):
    '''Retorna los índices a generar.

    Args:
        config (ConfigParser): objeto de archivo de configuración.

    Returns:
        indexes (list dict): índices a generar.
    '''
    cindexes = config.get("General", "Indexes")
    cindexes = [i.strip() for i in cindexes.split(",")]
    dirout = config.get("General", "DirOut")
    indexes = []
    for section in cindexes:
        doc_encode = config.get(section, "DocEncode")
        freq_encode = config.get(section, "FreqEncode")
        names = [x.strip() for x in config.get(section, "Names").split(",")]
        chunk_sizes = [x for x in config.get(section, "ChunkSizes").split(",")]

        if len(names) != len(chunk_sizes):
            ex = "La cantidad de names y chunk sizes debe coincidir."
            raise Exception(ex)

        for i in range(0, len(names)):
            index = {}
            index["DirOut"] = dirout + "/" + names[i]
            index["ChunkSize"] = chunk_sizes[i]
            index["FreqEncode"] = freq_encode
            index["DocEncode"] = doc_encode
            indexes.append(index)
    return indexes


def create_index_by_config(config_file):
    '''Parsea el archivo de config dado y vuelve a invocar el script con los
    parámetros necesarios para la creación de c/u de los índices.

    Args:
        config_file (string): archivo de configuración de índices a generar.
    '''
    config = ConfigParser()
    config.read(config_file)

    # Directorio de colección
    dirin = config.get("General", "DirIn")

    # Tipo de corpus.
    corpus_type = config.get("General", "CorpusType")

    # Generación de índices en base a configuración.
    indexes = parse_indexes_to_generate(config)

    for i in indexes:
        dirout = i["DirOut"]
        chunk_size = i["ChunkSize"]
        doc_encode = i["DocEncode"]
        freq_encode = i["FreqEncode"]

        # Construcción y ejecución de comando para generar índice.
        print("Generando {0}...".format(dirout))
        # cmd = "python3 indexgenerator.py {0} {1} {2} {3} {4} {5}"
        # cmd = cmd.format(dirin, dirout, chunk_size, doc_encode,
        #                 freq_encode, corpus_type)
        # os.system(cmd)
        args = [None, dirin, dirout, chunk_size, doc_encode,
                freq_encode, corpus_type]

        create_index_by_args(args)


def main(args):
    '''Punto de entrada de app.'''
    if len(args) > 2:
        create_index_by_args(args)
    elif len(args) == 2:
        create_index_by_config(args[1])
    else:
        print("Es necesario especificar al menos 1 (un) argumento.")


# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
