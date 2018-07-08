#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: utils.py
- Descripcion: Contiene strings y funciones de utilidad.
- Autor: Agustín González
- Modificado: 01/10/17
'''

import os


def clearscreen():
    '''Limpia pantalla de forma estándar.'''
    os.system("cls" if os.name == "nt" else "clear")


def makedirs(dir):
    '''Crea el conjunto de directorios especificado sólo si es necesario.'''
    if not os.path.exists(dir):
        os.makedirs(dir)


def waitexit():
    '''Deja consola en modo espera.'''
    print("Presione CTRL+C para salir...")

    while True:
        pass
