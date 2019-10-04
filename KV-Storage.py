#!/usr/bin/env python3

from kv_storage_interface import Interface
import sys


def main():
    interface = Interface()
    error_level = interface.handle_command()
    sys.exit(error_level)


if __name__ == "__main__":
    main()
