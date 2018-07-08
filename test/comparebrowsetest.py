#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: comparebrowsetest.py
- Descripción: compara los resultados de búsqueda de dos índices, verificando
si se cumple la igualdad en la respuesta. Útil para comparar dos índices que
utilizan distintas codificaciones, pero que son iguales en términos de corpus.
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

    index1 = Index("../output/test/index-1/")
    index2 = Index("../output/test/index-2/")

    print("Cargando índice 1...")
    index1.load()
    print("Cargando índice 2...")
    index2.load()

    # Browser en índice resultante.
    index1_browser = Browser(index1, BrowseType.Boolean)
    index2_browser = Browser(index1, BrowseType.Boolean)

    while True:
        text = input("\nBúsqueda: ").lower().strip()

        for i in range(0, 2):
            if i == 0:
                doc_ids = index1_browser.browse(text)
                print("Búsqueda en índice 1 (total {0}):".format(len(doc_ids)))
                doc_ids1 = doc_ids
            else:
                print("")
                doc_ids = index2_browser.browse(text)
                print("Búsqueda en índice 2 (total {0}):".format(len(doc_ids)))

                if doc_ids != doc_ids1:
                    input("ATENCIÓN: Las postings son distintas...")

            doc_number = 1
            if not doc_ids:
                msg = "Ningún documento coincide con la búsqueda."
                print(msg)
            else:
                # Impresión de primeros 25 docs.
                for doc_id in doc_ids[0:25]:
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
