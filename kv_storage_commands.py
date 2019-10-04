#!/usr/bin/env python3

import os
import os.path
import struct
import re
from collections import namedtuple
import csv
import sys


class InvalidCsvFileError(Exception):
    def __init__(self, file):
        self.message = f'File {file} is not valid csv file'

    def __str__(self):
        return self.message


class NotDataFileError(Exception):
    def __init__(self, file):
        self.message = f'File {file} is not data file'

    def __str__(self):
        return self.message


class DataFileExistenceError(Exception):
    def __init__(self, file):
        self.message = f'File with name {file} already exists'

    def __str__(self):
        return self.message


class FileFailureError(Exception):
    def __init__(self, file):
        self.message = f"File '{file}' doesn't exist"

    def __str__(self):
        return self.message


class UsedKeyError(Exception):
    def __init__(self, key):
        self.message = f'The key {key} is already used'

    def __str__(self):
        return self.message


class FullDataFileError(Exception):
    def __init__(self, file):
        self.message = f'The data file {file} is full'

    def __str__(self):
        return self.message


class LackOfMemoryError(Exception):
    def __init__(self, file):
        self.message = (f'There is no memory for your data in '
                        f'data file {file} now.\n'
                        f'Delete something to add your data')

    def __str__(self):
        return self.message


class BigDataError(Exception):
    def __init__(self):
        self.message = 'The data is too big to store even in empty data file'

    def __str__(self):
        return self.message


class NoSuchKeyError(Exception):
    def __init__(self, file, key):
        self.message = (f'There is no data with the '
                        f'key {key} in data file {file}')

    def __str__(self):
        return self.message


Cell = namedtuple('Cell',
                  ['cell_len', 'key_type_len', 'key_type',
                   'key_len', 'key',
                   'value_type_len', 'value_type',
                   'value_len', 'value'])


class KVStorage:

    LINKS_AND_CHECKSUMS_BOUNDARY = 1048576
    CHECKSUMS_AND_DATA_BOUNDARY = 1048648
    FULL_CAPACITY = 26214400
    ERASED_ELEMENT_NUMBER = 0
    MAX_TREE_IND = 2 ** 18 - 2
    MAX_TREE_HEIGHT = 17
    LINKS_START = 4
    TYPE_DATA = 'data'
    TYPE_FILE = 'file'

    def __init__(self, data_file_name):
        self._data_file_name = data_file_name
        if not self._is_file_existing(data_file_name):
            f = open(self._data_file_name, 'wb')
            f.close()
        self._data_file = open(self._data_file_name, 'r+b')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._data_file.close()

    def init(self):
        if not self._is_file_existing(self._data_file_name):
            raise FileFailureError(self._data_file_name)
        self._data_file.seek(0)
        self._data_file.write(
            struct.pack('>l', self.CHECKSUMS_AND_DATA_BOUNDARY))
        for i in range((self.FULL_CAPACITY - 4) // 4):
            self._data_file.write(struct.pack('>l', 0))
        for i in range((self.FULL_CAPACITY - 4) % 4):
            self._data_file.write(b'0')

    def add(self, key, value):
        self._is_it_valid_data_file()
        old_key, old_value = key, value
        type_of_key, key = self._get_type_and_correct_value(key)
        type_of_value, value = self._get_type_and_correct_value(value)
        if self._find_position_of_link_of_key(old_key)[0]:
            raise UsedKeyError(key)
        cell = self._create_cell_of_data(type_of_key, key,
                                         type_of_value, value)
        self._add_data(cell)

    def add_file(self, key, path_to_file):
        self._is_it_valid_data_file()
        old_key = key
        type_of_key, key = self._get_type_and_correct_value(key)
        type_of_value = 'file'
        if not self._is_file_existing(path_to_file):
            raise FileFailureError(path_to_file)
        if self._find_position_of_link_of_key(old_key)[0]:
            raise UsedKeyError(key)
        if (os.path.getsize(path_to_file) > self.FULL_CAPACITY -
                self.CHECKSUMS_AND_DATA_BOUNDARY):
            raise BigDataError()
        data_from_file = self._read_file(path_to_file)
        cell = self._create_cell_of_file(type_of_key, key,
                                         type_of_value, data_from_file)
        self._add_data(cell)

    def get(self, key):
        self._is_it_valid_data_file()
        old_key = key
        key = self._get_type_and_correct_value(key).correct_value
        is_in_storage = self._find_position_of_link_of_key(old_key)
        if not is_in_storage[0]:
            raise NoSuchKeyError(self._data_file_name, key)
        position_of_link = is_in_storage[1]
        self._data_file.seek(position_of_link)
        link = struct.unpack('>l', self._data_file.read(4))[0]
        self._data_file.seek(link, 0)
        cell_size = struct.unpack('>l', self._data_file.read(4))[0]
        self._data_file.seek(link, 0)
        cell = self._data_file.read(cell_size)
        value = self._parse_cell(cell).value
        if type(value) is bytes:
            value = value.decode('utf-8')
        return value

    def get_file(self, key, path_to_inp_file):
        old_key = key
        value = self.get(old_key)
        if hasattr(value, 'encode'):
            value = value.encode()
        if isinstance(value, int):
            value = struct.pack('>l', value)
        with open(path_to_inp_file, 'wb') as inp_file:
            inp_file.write(value)

    def contains(self, key):
        old_key = key
        self._is_it_valid_data_file()
        is_in_storage = self._find_position_of_link_of_key(old_key)
        return is_in_storage[0]

    def erase(self, key):
        def find_next_tree_ind(direction):
            nonlocal cur_tree_ind, self, cur_link_position
            opp_dir = 3 - direction
            dir_tree_ind = 2 * cur_tree_ind + direction
            last_tree_ind = dir_tree_ind
            while True:
                nxt_tree_ind = last_tree_ind * 2 + opp_dir
                if nxt_tree_ind > self.MAX_TREE_IND:
                    break
                nxt_link_position = self.LINKS_START + 4 * nxt_tree_ind
                self._data_file.seek(nxt_link_position)
                nxt_link = struct.unpack('>l', self._data_file.read(4))[0]
                if nxt_link == 0:
                    break
                last_tree_ind = nxt_tree_ind
            res_link_position = self.LINKS_START + 4 * last_tree_ind
            self._data_file.seek(res_link_position)
            new_link = self._data_file.read(4)
            self._data_file.seek(cur_link_position)
            self._data_file.write(new_link)
            if 2 * last_tree_ind + direction > self.MAX_TREE_IND:
                self._data_file.seek(res_link_position)
                self._data_file.write(struct.pack('>l', 0)[0:4])
                return -1
            last_dir_link_position = self.LINKS_START + 4 * (
                2 * last_tree_ind + direction)
            self._data_file.seek(last_dir_link_position)
            last_dir_link = struct.unpack('>l', self._data_file.read(4))[0]
            if last_dir_link == 0:
                self._data_file.seek(res_link_position)
                self._data_file.write(struct.pack('>l', 0)[0:4])
                return -1
            cur_tree_ind = last_tree_ind
            return 1

        self._is_it_valid_data_file()
        old_key = key
        key = self._get_type_and_correct_value(key).correct_value
        is_in_storage = self._find_position_of_link_of_key(old_key)
        if not is_in_storage[0]:
            raise NoSuchKeyError(self._data_file_name, key)
        position_of_link = is_in_storage[1]
        cur_tree_ind = (position_of_link - 4) // 4

        while True:
            if cur_tree_ind > self.MAX_TREE_IND:
                break
            cur_tree_height = self._calc_tree_ind_height(cur_tree_ind)
            if self._is_checksum_changed(cur_tree_height):
                raise NotDataFileError(self._data_file_name)
            cur_link_position = self.LINKS_START + 4 * cur_tree_ind
            self._data_file.seek(cur_link_position)
            self._data_file.write(struct.pack('>l', 0))
            cur_right_link_position = self.LINKS_START + 4 * (
                2 * cur_tree_ind + 2)
            self._data_file.seek(cur_right_link_position)
            right_link = struct.unpack('>l', self._data_file.read(4))[0]
            if right_link != 0:
                res = find_next_tree_ind(2)
                if res == -1:
                    break
                continue
            cur_left_link_position = self.LINKS_START + 4 * (
                2 * cur_tree_ind + 1)
            self._data_file.seek(cur_left_link_position)
            left_link = struct.unpack('>l', self._data_file.read(4))[0]
            if left_link != 0:
                res = find_next_tree_ind(1)
                if res == -1:
                    break
                continue
            break
        self._update_checksums_in_file()

    def clear(self):
        self._is_it_valid_data_file()
        self._data_file.seek(0)
        self._data_file.write(
            struct.pack('>l', self.CHECKSUMS_AND_DATA_BOUNDARY))
        for i in range((self.FULL_CAPACITY - 4) // 4):
            self._data_file.write(struct.pack('>l', 0))
        for i in range((self.FULL_CAPACITY - 4) % 4):
            self._data_file.write(b'0')

    def change(self, key, value_type, value):
        self._is_it_valid_data_file()
        old_key, old_value = key, value
        key_type, key = self._get_type_and_correct_value(key)
        if value_type == self.TYPE_DATA:
            value_type, value = self._get_type_and_correct_value(value)
        link_position = self._find_position_of_link_of_key(old_key)
        if not link_position[0]:
            raise NoSuchKeyError(self._data_file_name, key)
        if value_type == self.TYPE_FILE:
            current_cell = self._create_cell_of_file(key_type, key,
                                                     'file', value)
        else:
            current_cell = self._create_cell_of_data(key_type, key,
                                                     value_type, value)
        link_position = link_position[1]
        self._data_file.seek(link_position)
        link = struct.unpack('>l', self._data_file.read(4))[0]
        self._data_file.seek(link)
        prev_cell_size = struct.unpack('>l', self._data_file.read(4))[0]
        current_cell_size = len(current_cell)
        if current_cell_size <= prev_cell_size:
            self._data_file.seek(link)
            self._data_file.write(current_cell[0:current_cell_size])
            self._update_checksums_in_file()
        else:
            self.erase(old_key)
            self.add(old_key, old_value)

    def check_validity_of_file(self):
        try:
            self._data_file.seek(0)
            last_link = struct.unpack('>l', self._data_file.read(4))[0]
            if last_link < self.CHECKSUMS_AND_DATA_BOUNDARY:
                return False
            for i in range((self.LINKS_AND_CHECKSUMS_BOUNDARY - 4) // 4):
                link = struct.unpack('>l', self._data_file.read(4))[0]
                if (link != 0 and
                    link != self.ERASED_ELEMENT_NUMBER and
                    (link < self.CHECKSUMS_AND_DATA_BOUNDARY or
                     link >= self.FULL_CAPACITY)):
                    return False
            for i in range(self.MAX_TREE_HEIGHT):
                if self._is_checksum_changed(i):
                    return False
        except Exception:
            return False
        return True

    def add_package(self, error_handling_func=None, csv_file=None):

        def handle_row(cur_row):
            try:
                if cur_row[0] == 'data':
                    self.add(cur_row[1], cur_row[2])
                else:
                    self.add_file(cur_row[1], cur_row[2])
            except Exception:
                if callable(error_handling_func):
                    error_handling_func(row_ind, cur_row)

        self._is_it_valid_data_file()
        row_ind = 0
        if csv_file is None:
            for line in sys.stdin:
                row = line.split(',')
                if len(row) != 3:
                    continue
                if row[0] != 'data' and row[0] != 'file':
                    continue
                handle_row(row)
                row_ind += 1
            return
        if not os.path.isfile(csv_file):
            raise FileFailureError(csv_file)
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) != 3:
                    raise InvalidCsvFileError(csv_file)
                if row[0] != 'data' and row[0] != 'file':
                    raise InvalidCsvFileError(csv_file)
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                handle_row(row)
                row_ind += 1

    def get_all_keys(self):
        self._is_it_valid_data_file()
        keys = []
        stack = []
        used_set = set()
        stack.append(0)
        while len(stack) != 0:
            cur_tree_ind = stack[-1]
            used_set.add(cur_tree_ind)
            cur_link_position = (self.LINKS_START + 4 * cur_tree_ind)
            self._data_file.seek(cur_link_position)
            cur_link = struct.unpack('>l', self._data_file.read(4))[0]
            if cur_link == 0:
                stack.pop()
                continue
            left_tree_ind = cur_tree_ind * 2 + 1
            if (left_tree_ind < self.MAX_TREE_IND and
                    left_tree_ind not in used_set):
                left_link_position = (
                    self.LINKS_START + 4 * left_tree_ind)
                self._data_file.seek(left_link_position)
                left_link = struct.unpack('>l', self._data_file.read(4))[0]
                if left_link != 0:
                    stack.append(left_tree_ind)
            right_tree_ind = cur_tree_ind * 2 + 2
            if (right_tree_ind < self.MAX_TREE_IND and
                    right_tree_ind not in used_set):
                right_link_position = (
                    self.LINKS_START + 4 * right_tree_ind)
                self._data_file.seek(right_link_position)
                right_link = struct.unpack('>l', self._data_file.read(4))[0]
                if right_link != 0:
                    stack.append(right_tree_ind)
            if cur_tree_ind == stack[-1]:
                stack.pop()
                self._data_file.seek(cur_link)
                cell_size = struct.unpack('>l', self._data_file.read(4))[0]
                self._data_file.seek(cur_link)
                keys.append(
                    self._parse_cell(self._data_file.read(cell_size)).key)
        return keys

    def _update_checksums_in_file(self):
        for i in range(self.MAX_TREE_HEIGHT):
            cur_checksum = self._calc_tree_height_checksum(i)
            self._data_file.seek(self.LINKS_AND_CHECKSUMS_BOUNDARY + 4 * i)
            self._data_file.write(struct.pack('>l', cur_checksum)[0:4])

    def _calc_tree_ind_height(self, tree_ind):
        height = 0
        while tree_ind != 0:
            tree_ind = (tree_ind - 1) // 2
            height += 1
        return height

    def _convert_bytes_to_sum_of_integers(self, bytes_str):
        bytes_str = bytes(bytes_str)
        length = len(bytes_str)
        result_sum = 0
        for i in range(length // 4):
            result_sum += struct.unpack('>l', bytes_str[4 * i: 4 * i + 4])[0]
        remainder = bytes_str[4 * (i + 1):length]
        for j in range(4 - length % 4):
            remainder += b'0'
        result_sum += struct.unpack('>l', remainder[0:4])[0]
        return result_sum

    def _calc_tree_height_checksum(self, tree_height):
        st = int(2 ** tree_height - 1)
        fn = int(2 ** (tree_height + 1) - 2)
        checksum = 0
        for tree_ind in range(st, fn + 1):
            link_position = self.LINKS_START + 4 * tree_ind
            self._data_file.seek(link_position)
            link = struct.unpack('>l', self._data_file.read(4))[0]
            if link == 0:
                continue
            self._data_file.seek(link)
            cell_size = struct.unpack('>l', self._data_file.read(4))[0]
            self._data_file.seek(link)
            checksum = checksum ^ self._convert_bytes_to_sum_of_integers(
                self._data_file.read(cell_size))
        return checksum % 1000000007

    def _is_checksum_changed(self, tree_height):
        self._data_file.seek(self.LINKS_AND_CHECKSUMS_BOUNDARY +
                             tree_height * 4)
        prev_checksum = struct.unpack('>l', self._data_file.read(4))[0]
        cur_checksum = self._calc_tree_height_checksum(tree_height)
        return prev_checksum != cur_checksum

    def _is_file_existing(self, file):
        return os.path.isfile(file)

    def _compare_keys(self, a, b):
        if type(a) is type(b) and a == b:
            return 0
        if (type(a) is int and type(b) is str or
                type(a) is type(b) and a > b):
            return -1
        return 1

    def _find_position_of_link_of_key(self, key):
        self._is_it_valid_data_file()
        key_type, key = self._get_type_and_correct_value(key)
        cur_tree_ind = 0
        cur_tree_height = 0
        while True:
            if cur_tree_ind > self.MAX_TREE_IND:
                return False, -1
            if self._is_checksum_changed(cur_tree_height):
                raise NotDataFileError(self._data_file_name)
            cur_link_position = self.LINKS_START + cur_tree_ind * 4
            self._data_file.seek(cur_link_position, 0)
            cur_link = struct.unpack('>l', self._data_file.read(4))[0]
            if cur_link == 0:
                return False, -1
            self._data_file.seek(cur_link, 0)
            cur_cell_size = struct.unpack('>l', self._data_file.read(4))[0]
            self._data_file.seek(cur_link, 0)
            cur_cell = self._data_file.read(cur_cell_size)
            cur_key = self._parse_cell(cur_cell).key
            compare_result = self._compare_keys(key, cur_key)
            if compare_result == 0:
                return True, cur_link_position
            if compare_result == 1:
                cur_tree_ind = cur_tree_ind * 2 + 1
            else:
                cur_tree_ind = cur_tree_ind * 2 + 2
            cur_tree_height += 1

    def _get_position_of_link_and_link_in_inp(self, cell):
        key = self._parse_cell(cell).key
        cur_tree_ind = 0
        cur_tree_height = 0
        while True:
            if cur_tree_ind > self.MAX_TREE_IND:
                raise FullDataFileError(self._data_file_name)
            if self._is_checksum_changed(cur_tree_height):
                raise NotDataFileError(self._data_file_name)
            cur_link_position = self.LINKS_START + 4 * cur_tree_ind
            self._data_file.seek(cur_link_position)
            cur_link = struct.unpack('>l', self._data_file.read(4))[0]
            if cur_link == 0:
                link_position = cur_link_position
                break
            self._data_file.seek(cur_link)
            cur_cell_size = struct.unpack('>l', self._data_file.read(4))[0]
            self._data_file.seek(cur_link)
            cur_key = self._parse_cell(self._data_file.read(
                cur_cell_size)).key
            if self._compare_keys(key, cur_key) == 1:
                cur_tree_ind = cur_tree_ind * 2 + 1
            else:
                cur_tree_ind = cur_tree_ind * 2 + 2
            cur_tree_height += 1
        self._data_file.seek(0)
        free_place = struct.unpack('>l', self._data_file.read(4))[0]
        return link_position, free_place

    def _create_cell_of_data(self, type_of_key, key, type_of_value, value):
        if type_of_key == 'string':
            key = key.encode()
            key_to_pack = (
                struct.pack(f'>l{len(key)}s', len(key), key))
        else:
            key_to_pack = (
                struct.pack('>ll', 4, key))
        if type_of_value == 'string':
            value = value.encode()
            value_to_pack = (
                struct.pack(f'>l{len(value)}s', len(value), value))
        else:
            value_to_pack = (
                struct.pack('>ll', 4, value))
        type_of_value = type_of_value.encode()
        type_of_key = type_of_key.encode()
        len_of_cell = (12 +
                       len(type_of_key) + len(key_to_pack) +
                       len(type_of_value) + len(value_to_pack))
        byte_cell = (
            struct.pack('>l', len_of_cell) +
            struct.pack(f'>l{len(type_of_key)}s',
                        len(type_of_key), type_of_key) +
            key_to_pack +
            struct.pack(f'>l{len(type_of_value)}s',
                        len(type_of_value), type_of_value) +
            value_to_pack)
        return byte_cell

    def _create_cell_of_file(self, type_of_key, key, type_of_value, value):
        if type_of_key == 'string':
            key = key.encode()
            key_to_pack = (
                struct.pack(f'>l{len(key)}s', len(key), key))
        else:
            key_to_pack = (
                struct.pack('>ll', 4, key))
        type_of_key = type_of_key.encode()
        type_of_value = type_of_value.encode()
        value_to_pack = struct.pack(
            f'>l{len(value)}s', len(value), value)
        len_of_cell = (12 + len(type_of_key) + len(key_to_pack) +
                       len('file') + len(value_to_pack))
        byte_cell = (
            struct.pack('>l', len_of_cell) +
            struct.pack(f'>l{len(type_of_key)}s',
                        len(type_of_key), type_of_key) +
            key_to_pack +
            struct.pack(f'>l{len(type_of_value)}s',
                        len(type_of_value), type_of_value) +
            value_to_pack)
        return byte_cell

    def _add_data(self, cell):
        cell_len = len(cell)
        if (cell_len >
                self.FULL_CAPACITY - self.CHECKSUMS_AND_DATA_BOUNDARY):
            raise BigDataError()
        link_position, link = self._get_position_of_link_and_link_in_inp(cell)
        if link + cell_len >= self.FULL_CAPACITY:
            raise LackOfMemoryError(self._data_file_name)
        link_in_bytes = struct.pack('>l', link)
        self._data_file.seek(link_position)
        self._data_file.write(link_in_bytes[0:4])
        self._data_file.seek(link)
        self._data_file.write(cell[0:cell_len])
        self._data_file.seek(0)
        self._data_file.write(struct.pack('>l', link + cell_len)[0:4])
        tree_ind = (link_position - self.LINKS_START) // 4
        tree_height = self._calc_tree_ind_height(tree_ind)
        checksum = self._calc_tree_height_checksum(tree_height)
        self._data_file.seek(self.LINKS_AND_CHECKSUMS_BOUNDARY +
                             4 * tree_height)
        self._data_file.write(struct.pack('>l', checksum)[0:4])

    def _parse_cell(self, cell):
        cur_ind = 0
        cell_len = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        cur_ind += 4
        key_type_len = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        cur_ind += 4
        key_type = struct.unpack(
            f'>{key_type_len}s',
            cell[cur_ind:cur_ind + key_type_len])[0].decode()
        cur_ind += key_type_len
        key_len = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        cur_ind += 4
        if key_type == 'int':
            key = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        else:
            key = struct.unpack(
                f'>{key_len}s', cell[cur_ind:cur_ind + key_len])[0].decode()
        cur_ind += key_len

        value_type_len = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        cur_ind += 4
        value_type = struct.unpack(
            f'>{value_type_len}s',
            cell[cur_ind:cur_ind + value_type_len])[0].decode()
        cur_ind += value_type_len
        value_len = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        cur_ind += 4
        if value_type == 'int':
            value = struct.unpack('>l', cell[cur_ind:cur_ind + 4])[0]
        else:
            value = struct.unpack(
                f'>{value_len}s', cell[cur_ind:cur_ind +
                                       value_len])[0].decode()
        cur_ind += value_len
        parsed_cell = Cell(cell_len, key_type_len, key_type,
                           key_len, key,
                           value_type_len, value_type,
                           value_len, value)
        return parsed_cell

    def _is_it_valid_data_file(self):
        if not self._is_file_existing(self._data_file_name):
            raise FileFailureError(self._data_file_name)
        if not self.check_validity_of_file():
            raise NotDataFileError(self._data_file_name)

    def _read_file(self, file):
        with open(file, "rb") as f:
            return f.read()

    def _get_type_and_correct_value(self, string):
        result_tuple = namedtuple('result', ['type', 'correct_value'])
        try:
            return result_tuple('int', int(string))
        except ValueError:
            pass
        reg = re.compile(r'(\'+)\d+\1')
        match = reg.fullmatch(string)
        if match is not None:
            return result_tuple('string', string[1:-1])
        reg = re.compile(r'(\"+)\d+\1')
        match = reg.fullmatch(string)
        if match is not None:
            return result_tuple('string', string[1:-1])
        return result_tuple('string', string)
