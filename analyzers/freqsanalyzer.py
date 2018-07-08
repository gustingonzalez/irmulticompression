#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: freqsanalyzer.py
- Descripción: analizador de distribución de frecuencias. Genera dos archivos:
uno con la cantidad de frecuencias promedio (computada por posting) y otro con
las frecuencias promedio escalada a log2.
- Autor: Agustín González
- Modificado: 26/06/18
'''

import os
import sys
import ast
import math
from collections import Counter

try:
    sys.path.append('..')
    from lib.other import utils
except:
    raise Exception("Imposible cargar librerías.")

# Path de archivo a analizar.
FILE_TO_ANALYZE = "../output/test/index-2/other/encoder_freqs_statistics.txt"

# Output de análisis.
OUTPUT_DIR = "../output/stats/index-2/"


def analyze():
    '''Ejecuta análisis.'''
    posting_count = 0
    freq_avg_counter = Counter()

    reader = open(FILE_TO_ANALYZE)
    for line in reader:
        spplited = line[:-1].split(";")
        posting = spplited[2].split(":")[1].strip()

        posting_count += 1

        # Literal eval de posting.
        posting = ast.literal_eval(posting)

        avg = round(sum(posting)/len(posting), 0)
        freq_avg_counter[avg] += 1

    # Conteo de frecuencias escalado a log2.
    freq_avg_counter_log2 = Counter()

    to_write = "Total de postings: {0} (Archivo analizado: {1})\n\n"
    to_write = to_write.format(posting_count, FILE_TO_ANALYZE)
    to_write_log2 = str(to_write)

    to_write += "Frecuencia promedio\tCantidad de postings\n"
    to_write_log2 += "Frecuencia promedio (log2)\tCantidad de postings\n"

    # Original.
    for f in sorted(freq_avg_counter):
        to_write += "{0}\t{1}\n".format(int(f), freq_avg_counter[f])

        # To log2.
        log2 = int(round(math.log(f, 2), 0))
        freq_avg_counter_log2[log2] += freq_avg_counter[f]

    # log2.
    for f in sorted(freq_avg_counter_log2):
        to_write_log2 += "{0}\t{1}\n".format(int(f), freq_avg_counter_log2[f])

    # Archivo con datos 'originales'.
    writer = open(OUTPUT_DIR + "/freqsanalyzer.txt", "w")
    writer.write(to_write)

    # Archivo con frecuencias escaladas a log2.
    writer = open(OUTPUT_DIR + "/freqsanalyzer_log2.txt", "w")
    writer.write(to_write_log2)


def main(args):
    '''Punto de entrada.'''
    utils.makedirs(OUTPUT_DIR)
    analyze()


# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
