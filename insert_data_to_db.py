#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Parse data from BoardGameGeek XMLs and insert them to MySQL database"""

from rs_db_connection import RSDBConnection


if __name__ == "__main__":
    rsdbc = RSDBConnection()
    rsdbc.create_tables()
    rsdbc.process_games("dane")
    # rsdbc.show_games()
    rsdbc.process_users("users")
    rsdbc.finalize()
    print "Done."
