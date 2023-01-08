#####
Reata
#####

Reata is a simple wrapper for the official MySQL DBAPI for Python
(mysql.connector). The main purpose of the library is to simplify common MySQL
operations and make interfacing with the DBAPI a more python-centric affair.
Reata attempts to minimize the need for writing SQL statements into Python
scripts. Unlike similar libraries, however, Reata is not an ORM and it makes
no attempts to be an ORM. Instead, Reata aims to limit the amount of code
which must be written to interact with the DBAPI while improving
readability. Rather than force the user to write MySQL statements and execute
them via a cursor object, as is typically the case with standard DBAPIs in
Python, Reata abstracts away the underlying connection object and allows
users to execute common MySQL operations via class methods.

Reata also makes efforts to help simplify data processing pipelines by
incorporating methods that leverage the Pandas API. Users can load tables
directly into dataframes and use dataframes to update tables.

.. note::

   This distribution has been developed and tested on a standalone Python build
   and could--under rare circumstances--exibit unexpected behavior on standard
   CPython. Although no differences between the execution of code in the
   standalone build and normal CPython have been found during the process of
   development, I nevertheless wish to provide fair warning.

Installation
------------

.. code:: python

	python -m pip install reata

Documentation
-------------
Up-to-date documentation can be found here: `<https://jammin93.github.io/reata/>`_

Usage
-----

.. code:: python

   import mysql.connector

   from reata import MySQLClient, TableSchema

   cnx = mysql.connector.connect(...)
   client = MySQLClient(cnx)

License
-------
GNU General Public License
