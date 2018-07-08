#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: multiencodeanalyzer.py
- Descripción: analizador de estadísticas de documentos y frecuencias.
- Autor: Agustín González
- Modificado: 25/04/18
'''

import sys
import ast
from enum import Enum
from collections import Counter

try:
    sys.path.append('..')
    from lib.other import utils
except:
    raise Exception("Imposible cargar librerías.")


DIRIN = "../output/web-base/me-me"

FILES_TO_ANALYZE = {"DOCS0": DIRIN + "/other/encoder_docs_statistics.txt",
                    "DOCS64": DIRIN + "64/other/encoder_docs_statistics.txt",
                    "DOCS128": DIRIN + "128/other/encoder_docs_statistics.txt",
                    "DOCS256": DIRIN + "256/other/encoder_docs_statistics.txt",
                    "FREQS0": DIRIN + "/other/encoder_freqs_statistics.txt",
                    "FREQS64": DIRIN + "64/other/encoder_freqs_statistics.txt",
                    "FREQS128": DIRIN + "128/other/encoder_freqs_statistics.txt",
                    "FREQS256": DIRIN + "256/other/encoder_freqs_statistics.txt"}

OUTPUT = "../output/stats/web-base/multiencode/"


class StatTypes(Enum):
    '''Tipo de estadísticas.'''
    Distance = 0,
    FirstNumber = 1,
    PostingLen = 2


def check_dict_stats(number, encode, min_by_enc, sum_by_enc, max_by_enc):
    '''Verifica si el número dado es el nuevo mínimo o máximo, además de
    añadirlo a la sumatoria de elementos.

    Args:
        number (int): número a verificar.
        encode (string): encode a verificar en dicts.
        min_by_enc (dict): dict de min por encode.
        sum_by_enc (Counter): dict de sum por encode.
        max_by_enc (dict): dict de max por encode.
    '''
    # Verificación de mínimo.
    if encode not in min_by_enc or number < min_by_enc[encode]:
        min_by_enc[encode] = number

    # Add a total.
    sum_by_enc[encode] += number

    # Verificación de máximo.
    if encode not in max_by_enc or number > max_by_enc[encode]:
        max_by_enc[encode] = number


def print_dict_stats(writer, stype, pcount_by_enc, min_by_enc,
                     sum_by_enc, max_by_enc):
    '''Imprime estadísticas de diccionarios.

    Args:
        writer (file): archivo de salida.
        stype (StatTypes): tipo de estadística (distance, firstnum, plen).
        pcount_by_enc (Counter): dict de cantidad de postings por encode.
        min_by_enc (dict): dict de min (stype) por encode.
        sum_by_enc (Counter): dict de sum (stype) por encode.
        max_by_enc (dict): dict de max (stype) por encode.
    '''
    info = ""
    if stype == StatTypes.Distance:
        info += "\n>> Estadísticas de distancia entre elem de postings:"
    elif stype == StatTypes.FirstNumber:
        info += "\n>> Estadísticas de primer número de posting:"
    else:
        info += "\n>> Estadísticas de tamaño de posting:"

    for encode in sorted(pcount_by_enc):
        avg = sum_by_enc[encode] / pcount_by_enc[encode]
        info += "\nEncode: {0}".format(encode)
        info += "\n\t- Mínimo: {0}".format(int(round(min_by_enc[encode], 0)))
        info += "\n\t- Promedio: {0}".format(int(round(avg, 0)))
        info += "\n\t- Máximo: {0}\n".format(int(round(max_by_enc[encode], 0)))

    # print(info)
    writer.write(info)


def print_posting_stats(writer, encodes, min_posting_by_enc,
                        max_posting_by_enc):
    '''Imprime estadísticas de posting.

    Args:
        writer (file): archivo de salida.
        encodes (encodes): encodes utilizados para postings.
        min_by_enc (dict): dict de min (posting) por encode.
        max_by_enc (dict): dict de max (posting) por encode.
    '''
    info = ""
    for encode in sorted(encodes):
        info += "\nEncode: {0}".format(encode)
        info += "\n\t- Mínimo: {0}".format(min_posting_by_enc[encode])
        info += "\n\t- Máximo: {0}\n".format(max_posting_by_enc[encode])

    writer.write(info)


def print_posting_stats_by_encode(writer, pcount_by_enc):
    '''Imprime estadísticas de posting por encode.

    Args:
        writer (file): archivo de salida.
        pcount_by_enc (dict): diccionario de postings por encode.'''
    info = "\n>> Postings por encode"
    for encode in sorted(pcount_by_enc):
        info += "\n\t- Encode {0}: {1}".format(encode, pcount_by_enc[encode])

    # print(info)
    writer.write(info)


def analyze(fid, file_to_analyze):
    '''Ejecuta análisis.

    Args:
        fid (string): identificador de archivo a analizar (para output).
        file_to_analyze (string): path de archivo a analizar.
    '''
    reader = open(file_to_analyze)

    info = "Estadísticas {0}".format(fid.lower())

    # Posting count.
    pcount_by_enc = Counter()

    # Deltas según encodes.
    min_delta_by_enc = {}
    sum_delta_by_enc = Counter()
    max_delta_by_enc = {}

    # 1st números según encodes.
    min_1stnum_by_enc = {}
    sum_1stnum_by_enc = Counter()
    max_1stnum_by_enc = {}

    # Lens según encodes.
    min_len_by_enc = {}
    sum_len_by_enc = Counter()
    max_len_by_enc = {}

    min_posting_size = None
    sum_posting_size = 0
    max_posting_size = 0
    max_posting_term = "None"

    # Contador de postings.
    posting_32_count = 0
    posting_64_count = 0
    posting_128_count = 0
    posting_256_count = 0
    posting_512_count = 0

    # Sumas de distancias promedio de postings agrupadas según el tamaño.
    dprom_sum = 0       # Todas las postings.
    dprom_32_sum = 0    # >= 32 y < 64
    dprom_64_sum = 0    # >= 64 y < 128
    dprom_128_sum = 0   # >= 128 y < 256
    dprom_256_sum = 0   # >= 256 y < 512
    dprom_512_sum = 0   # >= 512

    posting_count = 0
    for line in reader:
        spplited = line[:-1].split(";")
        term = spplited[0].split(":")[1].strip()
        encode = spplited[1].split(":")[1].strip()
        posting = spplited[2].split(":")[1].strip()

        posting_count += 1

        # Literal eval de posting.
        posting = ast.literal_eval(posting)

        # Count de postings por encode.
        pcount_by_enc[encode] += 1

        # Verificación de first nros.
        check_dict_stats(posting[0], encode, min_1stnum_by_enc,
                         sum_1stnum_by_enc, max_1stnum_by_enc)

        # Verificación de len.
        plen = len(posting)
        check_dict_stats(plen, encode, min_len_by_enc,
                         sum_len_by_enc, max_len_by_enc)

        # Verificación de estadísticas de postings.
        if min_posting_size is None or plen < min_posting_size:
            min_posting_size = plen

        sum_posting_size += plen

        if plen > max_posting_size:
            max_posting_size = plen
            max_posting_term = term

        # Verificación de distancia promedio.
        dprom = posting[-1]/len(posting)

        check_dict_stats(dprom, encode, min_delta_by_enc, sum_delta_by_enc,
                         max_delta_by_enc)

        # Estadísticas de postings.
        dprom_sum += dprom
        if len(posting) >= 32 and len(posting) < 64:
            dprom_32_sum += dprom
            posting_32_count += 1
        elif len(posting) >= 64 and len(posting) < 128:
            dprom_64_sum += dprom
            posting_64_count += 1
        elif len(posting) >= 128 and len(posting) < 256:
            dprom_128_sum += dprom
            posting_128_count += 1
        elif len(posting) >= 256 and len(posting) < 512:
            dprom_256_sum += dprom
            posting_256_count += 1
        elif len(posting) >= 512:
            dprom_512_sum += dprom
            posting_512_count += 1

    # Estadísticas de tamaño de posting.
    info += "\n\n>> Estadísticas de posting:"
    info += "\nCantidad de postings: {0}".format(posting_count)
    dprom = round(dprom_sum/posting_count, 2)
    info += "\nDistancia promedio (total): {0}".format(dprom)
    info += "\nTamaño de posting:"
    info += "\n\t- Mínimo: {0}".format(min_posting_size)
    avg_posting_size = int(round(sum_posting_size/posting_count, 0))
    info += "\n\t- Promedio: {0}".format(avg_posting_size)
    info += "\n\t- Máximo: {0} - Término: {1}".format(max_posting_size,
                                                      max_posting_term)

    # Estadísticas de postings de más de 64, 128 y 256 elementos.
    info += "\n\n>> Estadísticas de posting según cantidad de elementos:"

    divisor = 1 if posting_32_count == 0 else posting_64_count
    dprom = round(dprom_32_sum/divisor, 2)
    info += "\n>= 32 y < 64:"
    info += "\n\t- Cantidad de postings: {0}".format(posting_32_count)
    info += "\n\t- Suma de distancias: {0}".format(round(dprom_32_sum, 2))
    info += "\n\t- Distancia promedio: {0}\n".format(dprom)

    divisor = 1 if posting_64_count == 0 else posting_64_count
    dprom = round(dprom_64_sum/divisor, 2)
    info += "\n>= 64 y < 128:"
    info += "\n\t- Cantidad de postings: {0}".format(posting_64_count)
    info += "\n\t- Suma de distancias: {0}".format(round(dprom_64_sum, 2))
    info += "\n\t- Distancia promedio: {0}\n".format(dprom)

    divisor = 1 if posting_128_count == 0 else posting_128_count
    dprom = round(dprom_128_sum/divisor, 2)
    info += "\n>= 128 y < 256:"
    info += "\n\t- Cantidad de postings: {0}".format(posting_128_count)
    info += "\n\t- Suma de distancias: {0}".format(round(dprom_128_sum, 2))
    info += "\n\t- Distancia promedio: {0}\n".format(dprom)

    divisor = 1 if posting_256_count == 0 else posting_256_count
    dprom = round(dprom_256_sum/divisor, 2)
    info += "\n>= 256 y < 512:"
    info += "\n\t- Cantidad de postings: {0}".format(posting_256_count)
    info += "\n\t- Suma de distancias: {0}".format(round(dprom_256_sum, 2))
    info += "\n\t- Distancia promedio: {0}\n".format(dprom)

    divisor = 1 if posting_512_count == 0 else posting_512_count
    dprom = round(dprom_512_sum/divisor, 2)
    info += "\n>= 512:"
    info += "\n\t- Cantidad de postings: {0}".format(posting_512_count)
    info += "\n\t- Suma de distancias: {0}".format(round(dprom_512_sum, 2))
    info += "\n\t- Distancia promedio: {0}\n".format(dprom)

    writer = open(OUTPUT + fid.lower() + ".txt", "w")
    writer.write(info)

    # Print de stats de distance.
    print_dict_stats(writer, StatTypes.Distance, pcount_by_enc,
                     min_delta_by_enc, sum_delta_by_enc, max_delta_by_enc)

    # Print de stats de 1st number.
    print_dict_stats(writer, StatTypes.FirstNumber, pcount_by_enc,
                     min_1stnum_by_enc, sum_1stnum_by_enc, max_1stnum_by_enc)

    # Print de stats de len.
    print_dict_stats(writer, StatTypes.PostingLen, pcount_by_enc,
                     min_len_by_enc, sum_len_by_enc, max_len_by_enc)

    print_posting_stats_by_encode(writer, pcount_by_enc)

    reader.close()
    writer.close()


def main(args):
    '''Punto de entrada.'''
    utils.makedirs(OUTPUT)

    for fid in sorted(FILES_TO_ANALYZE):
        print("Analizando {0}...".format(fid))
        analyze(fid, FILES_TO_ANALYZE[fid])


# Entrada de aplicación.
if __name__ == "__main__":
    sys.exit(main(sys.argv))
