#!/usr/bin/env python3
"""Load files into METdb."""

def main():
    import sys
    import pymysql

    print("--- Start METdbLoad ---")

    print(sys.argv[1])
    print(sys.path)

    print("--- End METdbLoad ---")

if __name__ == '__main__':
    main()