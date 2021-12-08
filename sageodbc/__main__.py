# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

"""
Entrypoint module, in case you use `python -m mpetools`.
Why does this file exist, and why __main__? For more info, read:
- https://www.python.org/dev/peps/pep-0338/
- https://docs.python.org/3/using/cmdline.html#cmdoption-m
"""

from sageodbc.cli import main

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
