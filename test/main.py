#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: main.py
- Descripción: test rápido de indexación y recuperación.
- Autor: Agustín González
- Modificado: 18/03/17
'''

import sys

try:
    sys.path.append('..')
    from lib.browser import Browser, BrowseType
    from lib.index.indexer import Indexer, CorpusTypes, IndexerStatusTypes
    from lib.index.compression.ircodecs import EncodeTypes
    from lib.other import utils
except:
    raise Exception("Imposible cargar librerías.")

# Directorio de colección.
DIRIN = "../resources/collections/T12012-gr/"
# DIRIN = "resources/collections/wiki-large"
# DIRIN = "resources/collections/web-base"

# Directorio de salida de índice.
# INDEX1_DIROUT = "output/T12012-gr-mix"
INDEX1_DIROUT = "../output/test/index-1"
INDEX2_DIROUT = "../output/test/index-2"


def main(args):
    '''Punto de entrada de app.'''
    utils.clearscreen()

    indexer = Indexer(DIRIN, CorpusTypes.Text, reuse_tmp=True)

    # Generación de índice 1.
    index1, index1_error = indexer.create_index(INDEX1_DIROUT, False, 64)

    # Generacón de índice 2
    indexer.doc_encode = [EncodeTypes.Unary, EncodeTypes.VariableByte,
                          EncodeTypes.VariableByte, EncodeTypes.EliasFano,
                          EncodeTypes.Simple16, EncodeTypes.BitPacking,
                          EncodeTypes.PForDelta]

    indexer.freq_encode = [EncodeTypes.Unary, EncodeTypes.VariableByte]

    index2, index2_error = indexer.create_index(INDEX2_DIROUT, False, 128)

    if index1_error == IndexerStatusTypes.Already_Indexed:
        print("Indice 1 ya indexado.")
    elif index1_error == IndexerStatusTypes.Collection_Non_Existent:
        print("Directorio de colección de índice 1 inexistente.")
        return

    if index2_error == IndexerStatusTypes.Already_Indexed:
        print("Indice 2 ya indexado.")
    elif index2_error == IndexerStatusTypes.Collection_Non_Existent:
        print("Directorio de colección de índice 2 inexistente.")
        return

    print("Cargando índice 1...")
    index1.load(chunks_info_in_memory=True)
    print("Cargando índice 2...")
    index2.load()

    # Browser en índice resultante.
    index1_browser = Browser(index1, BrowseType.Boolean)
    index2_browser = Browser(index2, BrowseType.Boolean)

    while True:
        text = input("\nBúsqueda: ").lower().strip()

        for i in range(0, 2):
            if i == 0:
                print("Búsqueda en índice 1:")
                doc_ids = index1_browser.browse(text)
                doc_ids1 = doc_ids
            else:
                print("")
                print("Búsqueda en índice 2:")
                doc_ids = index2_browser.browse(text)

                if doc_ids != doc_ids1:
                    print("")
                    print(doc_ids)
                    print(doc_ids1)
                    input("ATENCIÓN: Las postings son distintas ({0})...".format(text))
            doc_number = 1
            if not doc_ids:
                msg = "Ningún documento coincide con la búsqueda."
                print(msg)

            for doc_id in doc_ids:
                if i == 0:
                    doc = index1.get_doc_by_id(int(doc_id))
                else:
                    doc = index2.get_doc_by_id(int(doc_id))

                sdoc = str(doc_number) + ". " + doc + " ({0})"
                print(sdoc.format(doc_id))
                doc_number += 1

            print("")
            if i == 0:
                print("Benchmark:", index1_browser.get_benchmark(), "segundos")
            else:
                print("Benchmark:", index2_browser.get_benchmark(), "segundos")

        input("\nPresione una tecla para continuar...")
        utils.clearscreen()

# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
