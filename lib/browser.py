#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: browser.py
- Descripción: contiene la clase Browser, que permite realizar búsquedas de
diversos tipos ('BrowseType'), en un índice determinado. Nota: sólo se ha
implementado la búsqueda booleana de tipo AND.
- Autor: Agustín González
- Modificado: 08/04/18
'''

import time
from enum import Enum


class BrowseType(Enum):
    '''Tipos de búsqueda de Browser.'''
    Boolean = 1


class Browser(object):
    '''Permite realizar búsquedas en el índice especificado.'''

    def __init__(self, index, browse_type):
        '''Inicializador de clase.

        Args:
            index (Index): índice donde se realizarán la búsquedas.
            browse_type (BrowseType): tipo de búsqueda.
        '''
        self.__index = index
        self.__browse_type = browse_type

        # Fecha de inicio y finalización de última query (para benchmark).
        self.__last_browse_start_date = None
        self.__last_browse_end_date = None

    def get_benchmark(self):
        '''Retorna la cantidad de seg. en los que se realizó la última búsqueda.

        Returns:
            seconds (float): diferencia entre fecha de fin y de inicio de la
                última búsqueda realizada.
        '''
        return self.__last_browse_end_date - self.__last_browse_start_date

    def browse(self, text):
        '''Busca, en el índice dado, el texto especificado.

        Args:
            text (string): texto a buscar.

        Returns:
            docs (int list): ids de documentos con los que hay match.
        '''
        # Split de términos.
        terms = text.split(" ")

        # Sanitización: trim, lower de términos y eliminación de repetidos.
        terms = list(set([t.strip().lower() for t in terms]))

        # Documentos recuperados.
        docs = []

        self.__last_browse_start_date = time.time()

        # if self.__browse_type == BrowseType.Boolean:
        docs = self.__browse_boolean(terms)

        self.__last_browse_end_date = time.time()

        return docs

    def __browse_boolean(self, terms):
        '''Realiza búsqueda booleana (AND), en base a términos especificados.

        Args:
            terms (string list): términos sanitizados.

        Returns:
            docs (int list): ids de documentos con los que hay match.
        '''
        # Obtención de posting de primer término.
        docs = set(self.__index.get_posting_by_term(terms.pop(0)).keys())

        # Recuperación de postings de términos restantes.
        for term in terms:
            # Intersección de docs y term_docs.
            term_docs = self.__index.get_posting_by_term(term)
            docs = set.intersection(docs, term_docs)

        return sorted(list(docs))
