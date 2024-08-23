#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceIusta

def run():
    source = SourceIusta()
    launch(source, sys.argv[1:])
