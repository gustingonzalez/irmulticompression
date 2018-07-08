#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: simplebrowsetest.py
- Descripción: prueba de búsquedas en índice.
- Autor: Agustín González.
- Modificado: 28/01/18
'''

import sys

try:
    sys.path.append('..')
    from lib.browser import Browser, BrowseType
    from lib.index.indexer import Index
    from lib.other import utils
except:
    raise Exception("Imposible cargar librerías.")


def main(args):
    '''Punto de entrada de app.'''

    utils.clearscreen()

    index = Index("../output/test/index-1/")

    print("Cargando índice...")
    index.load(chunks_info_in_memory=False)

    # Browser en índice resultante.
    browser = Browser(index, BrowseType.Boolean)

    while True:
        text = input("\nBúsqueda: ").lower().strip()

        print("")
        doc_ids = browser.browse(text)
        print("Búsqueda en índice (total {0}):".format(len(doc_ids)))

        doc_number = 1
        if not doc_ids:
            msg = "Ningún documento coincide con la búsqueda."
            print(msg)
        else:
            # Impresión de primeros 25 docs.
            for doc_id in doc_ids[0:25]:
                doc = index.get_doc_by_id(int(doc_id))

                sdoc = str(doc_number) + ". " + doc + " ({0})"
                print(sdoc.format(doc_id))
                doc_number += 1

        print("")

        print("Benchmark:", browser.get_benchmark(), "segundos")

    input("\nPresione una tecla para continuar...")
    utils.clearscreen()

# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
