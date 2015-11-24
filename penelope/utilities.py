#!/usr/bin/env python
# coding=utf-8

"""
This file contains a collection of miscellaneous utility functions.
"""

from __future__ import absolute_import
from __future__ import print_function
import imp
import os
import shutil
import tempfile
import uuid

__author__ = "Alberto Pettarin"
__copyright__ = "Copyright 2012-2015, Alberto Pettarin (www.albertopettarin.it)"
__license__ = "MIT"
__version__ = "3.0.1"
__email__ = "alberto@albertopettarin.it"
__status__ = "Production"

def print_debug(msg, do_print=True):
    if do_print:
        print(u"[DEBU] %s" % msg)

def print_error(msg):
    print(u"[ERRO] %s" % msg)

def print_info(msg):
    print(u"[INFO] %s" % msg)

def get_uuid():
    return str(uuid.uuid4()).replace("-", "")

def load_input_parser(parser_file_path):
    parser = None
    if os.path.exists(parser_file_path):
        try:
            # load source file
            parser = imp.load_source("", parser_file_path)
            try:
                # try calling parse function
                parser.parse(None, None)
            except:
                print_error("Error trying to call the parse() function. Does file '%s' contain a parse() function?" % parser_file_path)
        except:
            print_error("Error trying to load parser from file '%s'" % parser_file_path)
    else:
        print_error("File '%s' does not exist" % parser_file_path)
    return parser

def create_temp_file():
    tmp_handler, tmp_path = tempfile.mkstemp()
    return (tmp_handler, tmp_path)

def create_temp_directory():
    return tempfile.mkdtemp()

def copy_file(origin, destination):
    try:
        shutil.copyfile(origin, destination)
    except:
        pass

def rename_file(origin, destination):
    try:
        os.rename(origin, destination)
    except:
        pass

def delete_file(handler, path):
    """
    Safely delete file.

    :param handler: the file handler (as returned by tempfile)
    :type  handler: obj
    :param path: the file path
    :type  path: string (path)
    """
    if handler is not None:
        try:
            os.close(handler)
        except:
            pass
    if path is not None:
        try:
            os.remove(path)
        except:
            pass

def delete_directory(path):
    """
    Safely delete a directory.

    :param path: the file path
    :type  path: string (path)
    """
    if path is not None:
        try:
            shutil.rmtree(path)
        except:
            pass


