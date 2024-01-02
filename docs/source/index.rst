.. tciopy documentation master file, created by
   sphinx-quickstart on Wed Dec 27 19:01:30 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.



tciopy
==================================
Tropical Cyclone IO Python Package

This package is intended to be a simple reader for the variety of Tropical Cyclone track data formats.
The aim is to provide a lightweight minimal dependency package that reads, ATCF ascii, CXML, ECMWF BUFR files into a consistent structure. 

Installation
==================
The package can be installed using pip:

.. code-block:: console
  
  python -m pip install tciopy

Usage
==================
Currently the package has 4 primary functions:
   ```read_adeck```
   ```read_cxml```
   ```read_fdeck```
   ```read_bdeck```

   e.g.
   

.. ipython:: python
   :suppress:

   from pathlib import Path
   datadir = Path().resolve().parent.parent / 'data'
   print(datadir)

.. ipython:: python

   from tciopy import read_adeck
   adeck = read_adeck(datadir / 'aal032023.dat')
   print(adeck.head())
   

.. ipython:: python

   from tciopy import read_fdeck
   fdeck = read_fdeck(datadir / 'fal132023.dat')
   print(fdeck.head())
   

.. ipython:: python

   from tciopy import read_cxml
   cxdeck = read_cxml(datadir / 'complete_cxml.xml')
   print(cxdeck.head())


.. note:: 
      This documentation is still under construction. Please check back later for more information.




Indices and tables
==================
.. toctree::
   api
   :maxdepth: 2
   :caption: Contents:


* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
