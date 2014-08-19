rdbtools
========

This is a parser for Redis RDB snapshot files. It is loosely based on [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools).

It is compatible with Redis RDB files generated since Redis 1.0 and up to the last version.
It comes with an extensive test suite.

Disclaimer
----------

Some things are not implemented:

  * checksum checking
  * keys with expire times

Also, the command line tool is very basic and doesn't do a lot. If you have a feature request, please shoot me a e-mail or open a issue.

Utilities
---------

In addition to the parser, there is a command line tool which can be used to:

  * generate a memory report

Documentation
-------------

The reference is available [here](http://godoc.org/github.com/vrischmann/rdbtools]).

License
-------

This project is licensed under the MIT license. See the LICENSE file for details.
