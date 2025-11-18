# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'tciopy'
copyright = '2023, Alan Brammer'
author = 'Alan Brammer'
release = 'v0.0.1'

import sys
from pathlib import Path
sys.path.insert(0, str((Path(__file__).parent.parent.parent / 'src' ).resolve()))

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    # 'sphinx.ext.duration',
    # 'sphinx.ext.doctest',
    'sphinx_rtd_theme',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx', 
    'sphinx.ext.napoleon',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive',
    "sphinx.ext.inheritance_diagram",
    ]

autodoc_default_options = {
    'autosummary': True,
    'members':         True,
    'member-order':    'bysource',
    'special-members': '',
    'exclude-members': '__weakref__',
    'imported-members': False,
}
autoclass_content = 'both'
autosummary_generate = True
autosummary_imported_members = False
autodoc_inherit_docstrings = False
numpydoc_class_members_toctree = True
numpydoc_show_class_members = False

# Suppress duplicate object warnings and other common warnings
suppress_warnings = [
    'autosummary',
    'ref.python',
    'app.add_node',  # Suppress warnings about duplicate nodes
    'app.add_directive',  # Suppress warnings about duplicate directives  
    'toc.not_included',  # Suppress warnings about documents not in toctree (for auto-generated aliases)
]
# Don't warn about duplicate descriptions - these are expected for re-exported functions
nitpicky = False

templates_path = ['_templates']
exclude_patterns = []

html_theme = "sphinx_rtd_theme"