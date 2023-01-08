"""
Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html

If extensions (or modules to document with autodoc) are in another directory,
add these directories to sys.path here. If the directory is relative to the
documentation root, use os.path.abspath to make it absolute, like shown here.
"""
import os.path
import sys

from pathlib import Path

sys.path.insert(0, os.path.abspath("../"))
sys.path.insert(0, os.path.abspath("../reata"))

# -- Project information -----------------------------------------------------
project = "Reata"
copyright = '2022, Ben Ohling'
author = 'Ben Ohling'

# -- General configuration ---------------------------------------------------
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    # The PyData theme seems to accomplish what the napoleon extension
    # attempts to accomplish but does a better job at it.
    # "sphinx.ext.napoleon",
]

templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "pygment_light_style": "one-dark",
    "pygment_dark_style": "trac",
    # Issue with Sphinx >= 6.0.0: Logos for PyData theme are not loading.
    # Use the following workaround. See https://bit.ly/3vPjTPT for details.
    "logo": {
        "image_light": "logo-light.png",
        "image_dark": "logo-dark.png",
    }
}

html_context = {
    "default_mode": "dark"
}

# -- Autodoc Configurations --------------------------------------------------
autodoc_class_signature = "mixed"
autodoc_typehints = "none"

autodoc_default_options = {
   "member-order": "groupwise",
   "exclude-members": (
       "read_yaml,"
       "Columns,"
       "PrimaryKey"
   )
}
