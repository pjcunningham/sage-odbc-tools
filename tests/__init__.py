# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import os
import site

script_dir = os.path.dirname(os.path.realpath(__file__))
site_packages = os.path.join(script_dir, "..", "sageodbc")
site.addsitedir(site_packages)

