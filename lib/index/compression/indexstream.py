#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
- Nombre: indexstream.py
- Descripción: contiene las clases 'IndexStreamReader' e 'IndexStreamWriter',
las cuales permiten gestionar el stream de datos que representa un índice.
- Autor: Agustín González
- Modificado: 17/04/18
'''

from .ircodecs import EncodeTypes
from .ircodecs import vbencoder as vbenc
from .ircodecs import unaryencoder as unaryenc
from .ircodecs import gammaencoder as gammaenc
from .ircodecs import eliasfanoencoder as efenc
from .ircodecs import bitpackingencoder as bpenc
from .ircodecs import simple16encoder as s16enc
from .ircodecs import pforencoder as pfdenc
from .ircodecs import gapsencoder as gapsenc
from .ircodecs.bitbytearray import BitByteArray


class IndexStreamReader(object):
    '''Reader de stream binario de índice.'''

    def __init__(self, path):
        '''Inicializa clase.

        Args:
            path (string): archivo a leer.
        '''
        # Puntero de byte en stream actual.
        self.__byte_pointer = 0

        # Puntero de bit en último byte del stream.
        self.__bit_pointer = 0

        # Stream.
        self.__file = open(path, "rb")

    def seek(self, offset):
        '''Establece el puntero de lectura en el byte especificado.

        Args:
            offset (int): nueva posición del puntero relativa al byte 0 del
                stream.
        '''
        self.__byte_pointer = offset
        self.__file.seek(self.__byte_pointer)

    def tell(self):
        '''Retorna el puntero de lectura.

        Returns:
            pointer (int): posición del puntero relativa al byte 0 del stream.
        '''
        return self.__byte_pointer

    def __barray_to_iarray(self, barray):
        '''Convierte la secuencia de bytes dada en un array de enteros.

        Args:
            barray (byte list): array de bytes.

        Returns:
            iarray (int list): array de enteros.
        '''
        iarray = []
        for i in range(0, len(barray), 4):
            iarray.append(int.from_bytes(barray[i:i+4], byteorder="big"))
        return iarray

    def read_byteblock(self, size, block_size):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada como bloques de octetos.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            block_size (int): tamaño de bloque.
        '''
        bytes_readed = self.raw_read(size)

        numbers = []
        for i in range(0, len(bytes_readed), block_size):
            encoded_number = bytes_readed[i:i+block_size]
            number = int.from_bytes(encoded_number, byteorder="big")
            numbers.append(number)
        return numbers

    def read_gamma(self, size, nums):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en Gamma.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            nums (int): cantidad de números a decodificar.
        '''
        bytes_readed = self.raw_read(size)
        numbers = gammaenc.decode(bytes_readed, nums=nums)
        return numbers

    def read_unary(self, size, nums):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en Unario.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            nums (int): cantidad de números a decodificar.
        '''
        bytes_readed = self.raw_read(size)
        numbers = unaryenc.decode(bytes_readed, nums=nums, is_optimized=True)
        return numbers

    def read_vb(self, size):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en Variable Byte.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
        '''
        bytes_readed = self.raw_read(size)
        numbers = vbenc.decode(bytes_readed)
        return numbers

    def read_binary(self, size, nums):
        '''Lee la cantidad de bytes especificada y la interpreta como bloques
        binarios.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            nums (int): cantidad de números a decodificar.
        '''
        bytes_readed = self.raw_read(size)
        numbers = bpenc.decode(bytes_readed, nums)
        return numbers

    def read_eliasfano(self, size, nums):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en Elias Fano.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            nums (int): cantidad de números a decodificar.
        '''
        bytes_readed = self.raw_read(size)
        numbers = efenc.decode(bytes_readed, nums)
        return numbers

    def read_simple16(self, size):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en Simple16.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
        '''
        bytes_readed = self.raw_read(size)
        integers = self.__barray_to_iarray(bytes_readed)
        numbers = s16enc.decode(integers)
        return numbers

    def read_pfd(self, size, nums):
        '''Lee la cantidad de bytes especificada y la interpreta como una
        secuencia codificada en PForDelta.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
        '''
        bytes_readed = self.raw_read(size)
        integers = self.__barray_to_iarray(bytes_readed)
        numbers = pfdenc.decode(integers, nums)
        return numbers

    def raw_read(self, size):
        '''Lee, del stream, la cantidad de bytes especificada e incrementa el
        offset (byte pointer) en 'size' bytes.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
        '''
        bytes_readed = self.__file.read(size)
        self.__byte_pointer += size
        return bytes_readed

    def read(self, size, nums=-1, etype=EncodeTypes.ByteBlocks, block_size=-1,
             use_gaps=True):
        '''Lee la cantidad de bytes especificada en el formato indicado e
        incrementa el offset (byte pointer) del stream en 'size' bytes.

        Args:
            size (int): cantidad de bytes a leer desde el offset actual.
            nums (int): cant. de nros. a decodificar de los bytes leídos.
            etype (EncodeTypes): codificación a utilizar para interpretar la
                secuencia de bytes leída.
            block_size (int): tamaño de bloque para la lectura con codificación
                de tipo 'ByteBlocks'.
            use_gaps (bool): indica si se debe aplicar decode de gaps. Nota: el
                parámetro no aplica si la codificación es Elias Fano.
        '''
        numbers = []
        if etype == EncodeTypes.BitPacking:
            numbers = self.read_binary(size, nums)
        elif etype == EncodeTypes.ByteBlocks:
            numbers = self.read_byteblock(size, block_size)
        elif etype == EncodeTypes.Unary:
            numbers = self.read_unary(size, nums)
        elif etype == EncodeTypes.Gamma:
            numbers = self.read_gamma(size, nums)
        elif etype == EncodeTypes.EliasFano:
            numbers = self.read_eliasfano(size, nums)
        elif etype == EncodeTypes.Simple16:
            numbers = self.read_simple16(size)
        # Nota: los bloques PFD menores a 64 elementos se almacenan en VB.
        elif etype == EncodeTypes.PForDelta and nums >= 64:
            numbers = self.read_pfd(size, nums)
        # PFD < 64 elementos (o VByte encode, por descarte).
        else:
            numbers = self.read_vb(size)

        if use_gaps and etype != EncodeTypes.EliasFano:
            numbers = gapsenc.decode(numbers)

        return numbers

    def close(self):
        '''Cierra lectura de stream.'''
        self.__file.close()


class IndexStreamWriter(object):
    '''Writer de stream binario de índice.'''

    def __init__(self, path, auto_flush=True):
        '''Inicializa clase.

        Args:
            path (string): archivo de volcado de stream.
            auto_flush (bool): indica si se debe realizar auto-volcado a disco
                cuando el stream alcanza el tamaño de 5 MiB.
        '''
        # Stream.
        self.__stream = BitByteArray()

        # Puntero de bytes escritos.
        self.__byte_pointer = 0

        # Path de almacenamiento.
        self.__path = path

        # Flag que indica si hay un bloque abierta en curso.
        self.__block_is_open = False

        # Flag que indica si se deberá aplicar gaps a las escrituras del actual
        # bloque abierto.
        self.__use_gaps = False

        # Archivo en disco.
        self.__file = None

        # Flag de autoflush.
        self.__auto_flush = auto_flush

    def flush(self):
        '''Realiza volcado a disco del array (y lo vacía).'''
        if self.__file is None:
            self.__file = open(self.__path, "wb")
            self.__file = open(self.__path, "ab")

        self.__file.write(self.__stream.to_bytearray())
        self.__stream.clear()

    def __verify_autoflush(self):
        '''Realiza volcado a disco del array (y lo vacía) en caso de que supere
        los 5 MiB.'''
        if self.__auto_flush:
            mbytes = int((len(self.__stream)/1024)/1024)
            if mbytes >= 5:
                self.flush()

    def tell(self):
        '''Retorna el puntero de escritura y padding del último byte del stream.

        Returns:
            pointer (int): puntero al último byte del stream.
            padding (int): relleno (en bits) del último byte del stream.
        '''
        return self.__byte_pointer, self.__stream.padding()

    def begin_block(self, use_gaps=True):
        '''Inicializa un nuevo bloque de escritura.

        Args:
            use_gaps (bool): indica si (exceptuando a Elias Fano), previo a la
                codificación, se deberá aplicar gaps a las listas de números.

        Returns:
            pointer (int): puntero al último byte del stream.
        '''
        if self.__block_is_open:
            raise Exception("Ya existe un bloque iniciado.")

        # Establecimiento de bloque como abierto.
        self.__block_is_open = True
        self.__use_gaps = use_gaps
        return self.__byte_pointer

    def __check_if_block_is_open(self):
        if not self.__block_is_open:
            ex = "No ha sido iniciado ningún bloque con anterioridad."
            raise Exception(ex)

    def close_block(self):
        '''Cierra un bloque de escritura abierto.

        Returns:
            pointer (int): puntero al último byte agregado en el stream.
            padding (int): relleno (en bits) del último byte del stream.
        '''
        self.__check_if_block_is_open()

        # Establecimiento de bloque como cerrado.
        self.__block_is_open = False

        # Close de último byte agregado.
        old_bit_pointer = self.__stream.close_byte()

        # Padding calculado.
        padding = (8 - old_bit_pointer) if old_bit_pointer != 0 else 0

        # Verificación de autoflush.
        self.__verify_autoflush()

        return self.__byte_pointer, padding

    def __iarray_to_barray(self, iarray):
        '''Convierte la secuencia de enteros dada en un array de bytes.

        Args:
            iarray (int list): array de enteros.

        Returns:
            barray (byte list): array de bytes.
        '''
        barray = []
        for integer in iarray:
            barray += integer.to_bytes(4, byteorder="big")
        return barray

    def __write_byteblock(self, number, block_size):
        '''Escribe el número dado utilizando bloques fijos de octetos.

        Args:
            numbers (int): número a escribir.
            block_size (int): tamaño de bytes por número.
        '''
        bytes_to_write = number.to_bytes(block_size, byteorder="big")
        self.raw_write(bytes_to_write, 0)

    def __write_gamma(self, number):
        '''Escribe el número dado utilizando Gamma.

        Args:
            number (int): número a escribir.
        '''
        bytes_to_write, padding = gammaenc.encode(number)
        self.raw_write(bytes_to_write, padding)

    def __write_unary(self, number):
        '''Escribe el número dado utilizando Unario.

        Args:
            number (int): número a escribir.
        '''
        bytes_to_write, padding = unaryenc.encode(number, optimize=True)
        self.raw_write(bytes_to_write, padding)

    def __write_vb(self, number):
        '''Escribe el número dado utilizando Variable Byte.

        Args:
            number (int): número a escribir.
        '''
        bytes_to_write = vbenc.encode(number)
        self.raw_write(bytes_to_write)

    def __write_bitpacking(self, numbers):
        '''Escribe la lista de números dada utilizando Bit Packing.

        Args:
            numbers (int list): números a escribir.
        '''
        bytes_to_write, padding = bpenc.encode(numbers)
        self.raw_write(bytes_to_write, padding)

    def __write_pfd(self, numbers):
        '''Escribe la lista de números dada utilizando PForDelta.

        Args:
            numbers (int list): números a escribir.
        '''
        ints_to_write = pfdenc.encode(numbers)
        self.raw_write(self.__iarray_to_barray(ints_to_write))

    def __write_simple16(self, numbers):
        '''Escribe la lista de números dada utilizando Simple16.

        Args:
            numbers (int list): números a escribir.
        '''
        ints_to_write = s16enc.encode(numbers)
        self.raw_write(self.__iarray_to_barray(ints_to_write))

    def __write_eliasfano(self, numbers):
        '''Escribe la lista de números dada utilizando Elias Fano.

        Args:
            numbers (int list): números a escribir.
        '''
        bytes_to_write, padding = efenc.encode(numbers)
        self.raw_write(bytes_to_write, padding)

    def raw_write(self, bytes_to_write, padding=0):
        '''Escribe los bytes especificados en el stream.

        Args:
            bytes_to_write (bytes): bytes a escribir.
            padding (int): relleno (en bits) del último byte.
        '''
        if not self.__block_is_open:
            raise Exception("Debe invocar al método de inicio de bloque.")

        # Padding antes de la escritura.
        old_padding = self.__stream.padding()

        # Escritura de bytes.
        self.__stream.extend(bytes_to_write, padding)

        # Offset de bit anterior a la escritura.
        old_bit_index = 8-old_padding-1

        # Indica si se han escrito todos los bytes, ya que puede suceder que se
        # escriba 1 byte 'menos' si la diferencia entre el bit pointer antes de
        # la escritura y el padding del elemento agregado es menor a 0. Ej.:
        # si se requiere escribir un byte con padding 7 (por tanto, con carga
        # útil de 1 bit) y si el bit pointer actual es 1, entonces se escribe
        # la carga útil (1 bit) en ese byte, pero no se agrega uno nuevo.
        all_bytes_writed = (old_bit_index - padding) >= 0

        # Incremento de bit pointer (menos 1 byte, si así fuera necesario).
        self.__byte_pointer += len(bytes_to_write)-int(not(all_bytes_writed))

    def write(self, numbers, etype, block_size=-1):
        '''Escribe, en el stream, la lista de números especificada, con el tipo
        de codificación especificada.

        Args:
            numbers (int list): lista de números a agregar al stream.
            etype (EncodeTypes): codificación a utilizar para la escritura.
            block_size (int): tamaño de bloque para la escritura con encode de
                tipo 'ByteBlocks'.
        '''
        self.__check_if_block_is_open()

        # Gaps de numbers (si corresponde).
        gaps = gapsenc.encode(numbers) if self.__use_gaps else numbers

        if etype == EncodeTypes.BitPacking:
            self.__write_bitpacking(gaps)
        if etype == EncodeTypes.PForDelta:
            # Escritura en PFOR sólo en caso de que el len a escribir sea mayor
            # a 64 (esto evita ineficiencias en la compresión).
            if len(numbers) >= 64:
                self.__write_pfd(gaps)
            else:
                for gap in gaps:
                    self.__write_vb(gap)
        elif etype == EncodeTypes.Simple16:
            self.__write_simple16(gaps)
        # Elias Fano: ¡NO GAPS!
        elif etype == EncodeTypes.EliasFano:
            self.__write_eliasfano(numbers)
        else:
            # Encoders 'parameter free'.
            if etype == EncodeTypes.VariableByte:
                for gap in gaps:
                    self.__write_vb(gap)
            elif etype == EncodeTypes.ByteBlocks:
                for gap in gaps:
                    self.__write_byteblock(gap, block_size)
            elif etype == EncodeTypes.Unary:
                for gap in gaps:
                    self.__write_unary(gap)
            elif etype == EncodeTypes.Gamma:
                for gap in gaps:
                    self.__write_gamma(gap)

    def __eval_determistic_encoders(self, numbers, etypes):
        '''Retorna el encode que genera menor cantidad de bits para la lista de
        nros. dada. El tamaño no se computa realizando la codificación (lo cual
        sería costoso), sino que según las ecuaciones determinísticas de los
        métodos de compresión Unario, Gamma, BitPacking y VariableByte.

        Args:
            numbers (int list): números a codificar.
            etypes (list EncodeTypes): lista de encodes a probar.

        Returns:
            etype (EncodeTypes): encode a utilizar para la lista de nros. dada.
        '''
        gaps = gapsenc.encode(numbers) if self.__use_gaps else numbers
        unary_size = 0  # Unario
        gamma_size = 0  # Gamma
        bp_size = 0     # Bit packing
        vb_size = 0     # Variable Byte

        if EncodeTypes.Unary in etypes:
            unary_size = unaryenc.compute_encoded_size(gaps)

        if EncodeTypes.Gamma in etypes:
            gamma_size = gammaenc.compute_encoded_size(gaps)

        if EncodeTypes.BitPacking in etypes:
            bp_size = bpenc.compute_encoded_size(gaps)

        if EncodeTypes.VariableByte in etypes:
            vb_size = vbenc.compute_encoded_size(gaps)

        encoded_sizes = [unary_size, gamma_size, bp_size, vb_size]

        # Mínimo, excluyendo valores ceros.
        min_size = min([x for x in encoded_sizes if x != 0])

        for i in range(0, len(encoded_sizes)):
            size = encoded_sizes[i]

            if size == 0:
                continue
            elif min_size == size:
                # Índices mapeables con el orden de la lista 'encoded_sizes'.
                if i == 0:
                    return EncodeTypes.Unary, min_size
                elif i == 1:
                    return EncodeTypes.Gamma, min_size
                elif i == 2:
                    return EncodeTypes.BitPacking, min_size
                elif i == 3:
                    return EncodeTypes.VariableByte, min_size

    def multiencode_write(self, numbers, etypes):
        '''Escribe la lista de números dada con el encode que genere la menor
        cantidad de bits.

        Args:
            numbers (int list): números a codificar.
            etypes (list EncodeTypes): lista de encodes a probar.

        Returns:
            etype (EncodeTypes): encode utilizado para la lista de nros.
        '''
        if len(etypes) < 2:
            ex = "Deben especificarse al menos 2 (dos) métodos de compresión."
            raise Exception(ex)

        bencoded = []    # Best encoded.
        bencoder = None  # Best encoder.
        bpadding = 0     # Best padding.
        bsize = None     # Best size.

        gaps = gapsenc.encode(numbers) if self.__use_gaps else numbers

        # 1. PFOR test (sólo en caso de que lista de nums sea >= 64).
        if EncodeTypes.PForDelta in etypes and len(numbers) >= 64:
            bencoded = self.__iarray_to_barray(pfdenc.encode(gaps))
            bencoder = EncodeTypes.PForDelta
            bsize = len(bencoded)*8

        # 2. Simple16 test.
        if EncodeTypes.Simple16 in etypes:
            s16encoded = self.__iarray_to_barray(s16enc.encode(gaps))
            if not bsize or len(s16encoded)*8 < bsize:
                bencoded, bencoder = s16encoded, EncodeTypes.Simple16
                bsize = len(s16encoded)*8

        # 3. Deterministic (estimated) size encoders test.
        eencoder, esize = self.__eval_determistic_encoders(numbers, etypes)
        if not bsize or esize < bsize:
            bencoder = eencoder
            bsize = esize

            bbarray = BitByteArray()
            if bencoder == EncodeTypes.BitPacking:
                bencoded, bpadding = bpenc.encode(gaps)
            else:
                if bencoder == EncodeTypes.Unary:
                    for gap in gaps:
                        encoded, padding = unaryenc.encode(gap, optimize=True)
                        bbarray.extend(encoded, padding)
                elif bencoder == EncodeTypes.Gamma:
                    for gap in gaps:
                        encoded, padding = gammaenc.encode(gap)
                        bbarray.extend(encoded, padding)
                elif bencoder == EncodeTypes.VariableByte:
                    for gap in gaps:
                        encoded = vbenc.encode(gap)
                        bbarray.extend(encoded, 0)

                bencoded = bbarray.to_bytearray()
                bpadding = bbarray.padding()

        # 4. EF Test. Nota: se evalua por último para evitar prioridad sobre
        # VByte, ya que EF utiliza dicho encode para las secuencias de long. 1.
        if EncodeTypes.EliasFano in etypes:
            encoded, padding = efenc.encode(numbers)

            if (len(encoded)*8)-padding < bsize:
                bencoded, bencoder = encoded, EncodeTypes.EliasFano
                bpadding = padding

        # Raw write y return de best encoded.
        self.raw_write(bencoded, bpadding)
        return bencoder

    def close(self):
        '''Cierra lectura de stream.'''
        self.__file.close()
        self.__stream.clear()
        self.__byte_pointer = 0
