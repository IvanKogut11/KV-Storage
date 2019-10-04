#!/usr/bin/env python3

import sys
from kv_storage_commands import (KVStorage, NotDataFileError,
                                 DataFileExistenceError, FileFailureError,
                                 UsedKeyError, FullDataFileError,
                                 LackOfMemoryError, BigDataError,
                                 NoSuchKeyError, InvalidCsvFileError)

import argparse


class Interface:

    EXIT_CODES = {
        NotDataFileError: 1,
        DataFileExistenceError: 2,
        FileFailureError: 3,
        UsedKeyError: 4,
        FullDataFileError: 5,
        LackOfMemoryError: 6,
        BigDataError: 7,
        NoSuchKeyError: 8,
        InvalidCsvFileError: 9
    }
    EXECUTOR = {}
    MESSAGE_TO_USER = {}
    PARSER = None

    def _init_command(self, name, func_in_KV, print_message_func):
        self.EXECUTOR[name] = func_in_KV
        self.MESSAGE_TO_USER[name] = print_message_func

    def _init_all_commands(self, kv):
        self._init_command('add', kv.add, lambda args:
                           print('Item was successfully added to KV-Storage'))
        self._init_command('add_file', kv.add_file, lambda args:
                           print('Content of file '
                                 'was successfully added to KV-Storage'))
        self._init_command('get', kv.get, lambda args:
                           print(args.result))
        self._init_command('get_file', kv.get_file, lambda args:
                           print(f'Value of item with key {args.key}'
                                 f' was successfully'
                                 f' stored in output file '
                                 f'{args.path_to_output_file}'))
        self._init_command('contains', kv.contains, lambda args:
                           print('Data file contains item with such key')
                           if args.result else
                           print('Data file doesn\'t contains item'
                                 ' with such key'))
        self._init_command('erase', kv.erase, lambda args:
                           print('Item was successfully erased'
                                 ' from KV-Storage'))
        self._init_command('init', kv.init, lambda args:
                           print(f'Data file was successfully initialized'))
        self._init_command('clear', kv.clear, lambda args:
                           print(f'Data file was successfully cleared'))
        self._init_command('change', kv.change, lambda args:
                           print(f'Value of item with the key \'{args.key}\''
                                 f' was successfully changed'))
        self._init_command('cvf', kv.check_validity_of_file,
                           lambda args:
                           print(f'It is data file') if args.result else
                           print(f'It is not data file'))
        self._init_command('add_package', kv.add_package, lambda args:
                           print(f'All correct queries were executed'))
        self._init_command('get_all_keys', kv.get_all_keys,
                           lambda args:
                           print("\n".join([str(x) for x in args.result])))

    def _get_parser(self):
        parser = argparse.ArgumentParser(
            description='To use KV-Storage '
                        'write one of the '
                        'positional arguments')
        subparsers = parser.add_subparsers()

        parser_add = subparsers.add_parser(
            'add',
            help='Command to add element(not file) in KV-Storage',
            description='Command to add element(not file) in KV-Storage')
        parser_add.set_defaults(command_name='add', result=None)
        parser_add.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_add.add_argument(
            'key', help='key of the element you want to add')
        parser_add.add_argument(
            'value', type=str, help='value of the element you want to add')

        parser_add_file = subparsers.add_parser(
            'add_file', help='Command to add file in KV-Storage',
            description='Command to add file in KV-Storage')
        parser_add_file.set_defaults(command_name='add_file', result=None)
        parser_add_file.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_add_file.add_argument(
            'key', type=str, help='key of the element you want to add')
        parser_add_file.add_argument(
            'path_to_file', type=str,
            help='path to file which you want to add')

        parser_get = subparsers.add_parser(
            'get', help='Command to get value(not file) by key',
            description='Command to get value(not file) by key')
        parser_get.set_defaults(command_name='get', result=None)
        parser_get.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_get.add_argument(
            'key', type=str, help='key of the value you want to get')

        parser_get_file = subparsers.add_parser(
            'get_file',
            help='Command to get the content of file in KV-Storage. '
                 'This content goes to your specified file.'
                 ' If file doesn\'t exist, programme will create it',
            description='Command to get the content of file in KV-Storage. '
                        'This content goes to your specified file.'
                        ' If file doesn\'t exist, programme will create it')
        parser_get_file.set_defaults(command_name='get_file', result=None)
        parser_get_file.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_get_file.add_argument(
            'key', type=str, help='key of the element you want to get')
        parser_get_file.add_argument(
            'path_to_output_file', type=str,
            help='path to file in which content will store')

        parser_contains = subparsers.add_parser(
            'contains',
            help='Command to find out if element with'
                 ' such key is in KV-Storage or not',
            description='Command to find out if element with such key'
                        ' is in KV-Storage or not')
        parser_contains.set_defaults(command_name='contains', result=None)
        parser_contains.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_contains.add_argument(
            'key', type=str, help='key which existence interests you')

        parser_erase = subparsers.add_parser(
            'erase',
            help='Command to erase element with such key from KV-Storage',
            description='Command to erase element '
                        'with such key from KV-Storage')
        parser_erase.set_defaults(command_name='erase', result=None)
        parser_erase.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_erase.add_argument(
            'key', type=str, help='key of the element you want to erase')

        parser_init = subparsers.add_parser(
            'init',
            help='Command to create new KV-Storage file',
            description='Command to create new KV-Storage file')
        parser_init.set_defaults(command_name='init', result=None)
        parser_init.add_argument(
            'data_file', type=str, help='path to file you want to create')

        parser_clear = subparsers.add_parser(
            'clear',
            help='Command to clear the content of data file',
            description='Command to clear the content of data file')
        parser_clear.set_defaults(command_name='clear', result=None)
        parser_clear.add_argument(
            'data_file', type=str, help='data file which you want to clear')

        parser_change = subparsers.add_parser(
            'change',
            help='Command to change value of the element with such key',
            description='Command to change value '
                        'of the element with such key')
        parser_change.set_defaults(command_name='change', result=None)
        parser_change.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_change.add_argument(
            'key', type=str, help='key of the element you want to change')
        parser_change.add_argument(
            'value_type', choices=['file', 'data'], type=str,
            help='type of new value(file or data)')
        parser_change.add_argument('value', type=str, help='new value')

        parser_cvf = subparsers.add_parser(
            'check_validity_of_file', aliases=['cvf'],
            help='Command to check if specified file is a KV-Storage '
                 'file(data file)',
            description='Command to check if specified file is a KV-Storage '
                        'file(data file)')
        parser_cvf.set_defaults(command_name='cvf', result=None)
        parser_cvf.add_argument(
            'data_file', type=str, help='file which you want to inspect')

        parser_add_package = subparsers.add_parser(
            'add_package',
            help='Command to add package of items to KV-Storage',
            description='Command to add package of items to KV-Storage')
        parser_add_package.set_defaults(command_name='add_package',
                                        result=None)
        parser_add_package.add_argument(
            'data_file', type=str, help='data file you want to work with')
        parser_add_package.add_argument(
            '-f', type=str, help='if you want to read queries from csv file',
            nargs='?', default=-1, dest='csv_file')

        parser_get_all_keys = subparsers.add_parser(
            'get_all_keys',
            help='Command to get list of all keys in KV-Storage',
            description='Command to get list of all keys in KV-Storage')
        parser_get_all_keys.set_defaults(command_name='get_all_keys',
                                         result=None)
        parser_get_all_keys.add_argument(
            'data_file', type=str, help='data file you want to work with')
        return parser

    def __init__(self):
        self.PARSER = self._get_parser()

    def handle_command(self):
        args = self.PARSER.parse_args()
        dict_of_args = vars(args)
        if not hasattr(args, 'command_name'):
            self.PARSER.print_help()
            return
        command = args.command_name
        data_file_name = args.data_file
        dict_of_args.pop('command_name', None)
        dict_of_args.pop('result', None)
        dict_of_args.pop('data_file', None)
        list_of_args = list(dict_of_args.values())
        kv = KVStorage(data_file_name)
        self._init_all_commands(kv)
        try:
            if command == 'add_package' and args.csv_file == -1:
                args.result = self.EXECUTOR[command]()
            else:
                args.result = self.EXECUTOR[command](*list_of_args)
            self.MESSAGE_TO_USER[command](args)
            kv.close()
            return 0
        except Exception as e:
            kv.close()
            print(e, file=sys.stderr)
            t, v, tb = sys.exc_info()
            if t in self.EXIT_CODES.keys():
                return self.EXIT_CODES[t]
            return 100


if __name__ == '__main__':
    interface = Interface()
    interface.handle_command()
