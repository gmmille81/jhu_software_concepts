project = 'Modern Software Concepts in Python Module 4'
copyright = '2026, Greg Miller'
author = 'Greg Miller'
release = '1.0'

import os
import sys
from pathlib import Path

DOCS_SOURCE_DIR = Path(__file__).resolve().parent
MODULE4_DIR = DOCS_SOURCE_DIR.parents[1]
SRC_DIR = MODULE4_DIR / "src"

sys.path.insert(0, str(MODULE4_DIR))
sys.path.insert(0, str(SRC_DIR))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]

templates_path = ['_templates']
exclude_patterns = []

autodoc_member_order = 'bysource'

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
