rdbtools
========

[![Build Status](https://travis-ci.org/vrischmann/rdbtools.svg?branch=master)](https://travis-ci.org/vrischmann/rdbtools)

This is a parser for Redis RDB snapshot files. It is loosely based on [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools).

It is compatible with Redis RDB files generated since Redis 1.0 and up to the last version.
It comes with an extensive test suite.

Disclaimer
----------

Some things are not implemented:

  * checksum checking
  * keys with expire times

Also, the command line tool is very basic and doesn't do a lot. If you have a feature request, please shoot me a e-mail or open a issue.

Known issues
------------

There are known bugs:

  * [This](https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/tests/dumps/zipmap_with_big_values.rdb) file fails to be parsed, the parser ends with a unexpected EOF error.

Utilities
---------

In addition to the parser, there is a command line tool which can be used to:

  * generate a memory report

Documentation
-------------

The reference is available [here](http://godoc.org/github.com/vrischmann/rdbtools]). Right now it's not really useful, but it will be improved.

License
-------

This project is licensed under the MIT license. See the LICENSE file for details.
