# -*- coding: utf-8 -*-

import struct


class CreateDiff:
    def __init__(self, iffd, dffd, offd, block_size=8192, block_counter_size_bytes=64):
        self.block_size = block_size
        self.block_counter_size_bytes = block_counter_size_bytes
        self.iffd = iffd
        self.dffd = dffd
        self.offd = offd
        self.ifgen = self.__read_file_generator(self.iffd)
        self.dfgen = self.__read_file_generator(self.dffd)
        self.header = ''

    def __read_file_generator(self, fd):
        while True:
            chunk = fd.read(self.block_size)
            if chunk:
                yield chunk
            else:
                break

    def __write_file(self, block_number, data):
        block_number_bytes = struct.pack('@Q', block_number)
        self.offd.write(block_number_bytes)
        self.offd.write(data)

    @staticmethod
    def __compare_data(ifdata, dfdata):
        if dfdata is None:
            return None
        elif ifdata != dfdata:
            return dfdata
        else:
            return ifdata

    # Reserved header for further use
    def __write_header(self, header_string):
        data = struct.pack('@32768s', header_string.encode())
        self.offd.write(data)

    def start(self):
        self.__write_header(self.header)
        block_counter = 0
        while True:
            try:
                ifdata = next(self.ifgen)
            except StopIteration:
                ifdata = None
            try:
                dfdata = next(self.dfgen)
            except StopIteration:
                dfdata = None
            data = self.__compare_data(ifdata, dfdata)
            if data is None:
                self.__write_file(block_counter, bytes())
                break
            else:
                self.__write_file(block_counter, data)
                block_counter += 1


class RestoreDiff:
    def __init__(self, iffd, dffd, offd, block_size=8192):
        self.block_size = block_size
        self.iffd = iffd
        self.dffd = dffd
        self.offd = offd
        self.ifgen = self.__read_file_generator(self.iffd, block_size=self.block_size)
        self.dfgen = self.__read_file_generator(self.dffd, block_size=self.block_size + 8)
        self.header = self.__read_header()

    @staticmethod
    def __read_file_generator(fd, block_size):
        while True:
            chunk = fd.read(block_size)
            if chunk:
                yield chunk
            else:
                break

    def __write_file(self, data):
        self.offd.write(data)

    def __read_header(self):
        header_bin = self.dffd.read(32768)
        header = (struct.unpack('@32768s', header_bin)[0]).decode()
        return header

    def start(self):
        block_counter = 0
        dfdata = next(self.dfgen)
        while True:
            try:
                ifdata = next(self.ifgen)
            except StopIteration:
                ifdata = None
            dfdata_block_number = struct.unpack('@Q', dfdata[0:8])[0]
            dfdata = dfdata[8:]

            if (len(dfdata) == 0) and (dfdata_block_number == block_counter):
                break
            elif dfdata_block_number == block_counter:
                data = dfdata
                dfdata = next(self.dfgen)
            else:
                data = ifdata
            self.__write_file(data)
            block_counter += 1

