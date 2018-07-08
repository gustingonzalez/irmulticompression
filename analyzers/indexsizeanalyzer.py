#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexsizeanalyzer.py
- Descripción: analizador de tamaño de índices.
- Autor: Agustín González
- Modificado: 15/03/18
'''

import os
import sys

DIROUT = "../output/stats/test/"
INDEXES_DIR = "../output/test/"

FILES = {"Collection": "collection.txt",
         "Vocabulary": "vocabulary.txt",
         "ChunksInfo": "chunksinfo.bin",
         "Posting": "postings.bin"}


def main(args):
    if not os.path.exists(DIROUT):
        os.makedirs(DIROUT)

    writter = open(DIROUT + "/indexsizeanalyzer.txt", "w")

    dirs = sorted(next(os.walk(INDEXES_DIR))[1])

    # Walk en filenames según directorio de in.
    for dir_index in dirs:
        dir_index = os.path.join(INDEXES_DIR, dir_index)

        writter.write("Índice: {0}\n".format(dir_index))
        for ftype in FILES:
            path = os.path.join(dir_index, FILES[ftype])
            size = round(((os.path.getsize(path)/1024)/1024), 2)
            writter.write("{0}: {1} GiB\n".format(FILES[ftype], size))
        writter.write("\n")
    writter.close()

# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
