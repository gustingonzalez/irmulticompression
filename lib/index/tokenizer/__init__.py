#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: __init__.py (tokenizer)
- Descripción: contiene la clase 'Tokenizer' (ver docstring).
- Autor: Agustín González
- Modificado: 09/03/18
'''

import os
import re


class Tokenizer(object):
    '''Tokenizador de términos.'''

    # Translator de carácteres especiales.
    __acutes = u"óíáéúüàèìòùâêîôûäëïöåãõ"
    __no_acutes = "oiaeuuaeiouaeiouaeioaao"
    __translator = str.maketrans(__acutes, __no_acutes)

    # Regex que permite eliminar carácteres no alfabéticos.
    __no_alpha_regex = re.compile("[^a-zA-Zñ]")

    # Letras del básicas del alfabeto.
    __letters = 'abcdefghijklmnñopqrstuvwxyz'

    # Lista de 3 letras iguales y consecutivas de c/letra del alfabeto (evita
    # cálculo por c/llamado a función de 'remove_consecutive_letters').
    __3_equal_consecutive_letters = set("".join([i] * 3) for i in __letters)

    @staticmethod
    def get_stopwords():
        '''Retorna stopwords en base al archivo 'stopwords.txt'.

        Returns:
            stopwords (string list) ubicadas en archivo 'stopwords.txt'.
        '''
        # Si está definido el parámetro Tokenizer.stopwords.
        try:
            return Tokenizer.__stopwords
        # No está definido el parámetro Tokenizer.stopwords.
        except Exception:
            Tokenizer.__stopwords = []

            path = os.path.dirname(__file__) + "//stopwords.txt"
            fstopwords = open(path)
            for line in fstopwords:
                # Agregar line.decode('utf-8') en python2.
                Tokenizer.__stopwords.append(line[:-1].lower())

            # Eliminación de repetidos: además, iterar en un set, es más rápido
            # que hacerlo sobre un list.
            Tokenizer.__stopwords = set(Tokenizer.__stopwords)
            return Tokenizer.__stopwords

    @staticmethod
    def __remove_consecutive_letters(token):
        '''Elimina las secuencias de tres letras iguales de un token, en caso
        de que este posea una longitud mayor a 4. Esto último permite tener en
        cuenta palabras como IEEE (sigla) o AAAA (tipo de pila).

        Args:
            token (string): token a verificar.

        Returns:
            token (string): token (si corresponde) convertido.
        '''
        if len(token) < 4:
            return token

        changed = True

        # Mientras haya cambios (eliminaciones) en el token...
        while changed:
            changed = False
            for sequence in Tokenizer.__3_equal_consecutive_letters:
                if sequence in token:
                    token = token.replace(sequence, "")
                    changed = True
        return token

    @staticmethod
    def convert2term(token):
        '''Convierte token a término. Notas:
        - Como tamaño mínimo de palabra se toma la que supere las 2 letras (se
        tienen en cuenta las de 3, para aquellas como red, ave, año, etc.).
        - Como tamaño máximo de palabra, se toma la que no supere las 24 letras
        para contemplar palabras largas, aunque raramente utilizadas, como por
        ej. 'electroencefalografista' (la más larga del idioma español).

        Args:
            token (string): token a convertir.

        Returns:
            token (string): token convertido, o null si no es un término.
        '''
        # token a utoken (no necesario en python 3)
        # token = token.decode('utf-8')

        # Minúsculas, antes de translation.
        token = token.lower().strip()

        # Eliminación de tildes y carácteres varios.
        # acutes = u"óíáéúüÓÍÁÉÚÜàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛäëïöÄËÏÖåÅãÃõÕ"
        # no_acutes = "oiaeuuOIAEUUaeiouAEIOUaeiouAEIOUaeioAEIOaAaAoO"
        # for i in range(0, len(acutes)):
        #    token = token.replace(acutes[i], no_acutes[i])
        token = token.translate(Tokenizer.__translator)

        # Sólo letras a-z y ñ (español)
        # token = "".join([i for i in token if i.isalpha()])
        token = Tokenizer.__no_alpha_regex.sub("", token)

        # Normalización de tags (acutes y raquo) HTML.
        if "acute" in token:
            token = token.replace("acute", "")

        if "raquo" in token:
            token = token.replace("raquo", "")

        # Eliminación de letras consecutivas.
        token = Tokenizer.__remove_consecutive_letters(token)

        # Si el token es una stopword...
        if token in Tokenizer.get_stopwords():
            token = None
        # Si el token tiene un tamaño menor a 3 o mayor a 24...
        elif len(token) < 3 or len(token) > 24:
            token = None

        return token
