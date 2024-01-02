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
}
autoclass_content = 'both'
autosummary_generate = True
numpydoc_class_members_toctree = True
numpydoc_show_class_members = False

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

import sphinx_rtd_theme
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
html_static_path = ['_static']
