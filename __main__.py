# ##################################################################################################
#  Copyright (c) 2022.    Caber Systems, Inc.                                                      #
#  All rights reserved.                                                                            #
#                                                                                                  #
#  CABER SYSTEMS CONFIDENTIAL SOURCE CODE                                                          #
#  No license is granted to use, copy, or share this software outside of Caber Systems, Inc.       #
#                                                                                                  #
#  Filename:  __main__.py                                                                          #
#  Authors:  Rob Quiros <rob@caber.com>  rlq                                                       #
# ##################################################################################################

import os
import sys
import time
import json
import logging
from json import JSONDecodeError
from fnmatch import fnmatch


# Make sure CSI_MODULE is set properly to ensure Common.init is initializing this package
mod = os.getenv("CSI_MODULE", None)
if __package__ and not mod:
    os.environ["CSI_MODULE"] = __package__.split('.')[-1]
elif __package__ and mod:
    if mod.split('.')[-1] != __package__.split('.')[-1]:
        print(f"[WARNING] CSI_MODULE != Package Name -> {mod.split('.')[-1]} != {__package__.split('.')[-1]}")
else:
    raise RuntimeError(f"__package__ is not set")

# from ..Common.init import CFG, hue
from .main import start_and_loop
#
# DEBUG = CFG.logLevel in ["verbose", "debug"]
# mfile = None
#
# ############################################################################################### #
# #                             REMOTE DEBUGGING VIA DASH (FLASK)                               # #
# #                                                                                             # #
# #  PyCharm remote debugging used for the other modules conflicts with the Flask webserver     # #
# #  that runs in this module.  Because Flask offers its own debugging capabilities, instead    # #
# #  of enabling PyCharm remote debug, we'll enable Flask debug using the same CFG key and      # #
# #  environment variable.                                                                      # #
# #                                                                                             # #
#
# off = ['<not set>', 'f*', '0', 'n*', 'off']
# env_debug = os.getenv('CSI_PYMOD_DEBUG', '<not set>')
# # Turn on debug if the environment variable is set and isn't anything
# # that indicates false, no, 0, off.
# if len(env_debug) and not any([fnmatch(env_debug.lower(), e) for e in off]):
#     env_debug = True
#     print(f"{hue.fg.orn}Flask in-browser debugging is {hue.fg.red}ENABLED{hue.rst}")
# else:
#     env_debug = False
#     print(f"Flask in-browser debugging is DISABLED CSI_PYMOD_DEBUG")
#
# #                                                                                             # #
# ############################################################################################### #
#
#
# if len(sys.argv) > 1:
#     # Take input from a file for debugging
#     mfile = sys.argv[1]
#     try:
#         with open(mfile, 'r') as f:            # ALWAYS LOCAL - USING BUILT-IN OPEN
#             mstr = f.read()
#         mdict = json.loads(mstr)
#     except OSError:
#         print(f"[ERROR] {CFG.name}: Cannot read debug input file {mfile}")
#         exit(1)
#     except JSONDecodeError:
#         print(f"[ERROR] {CFG.name}: Debug input file {mfile} is not valid JSON")
#         exit(1)
#     else:
#         print(f"{hue.fg.grn}Initialization Complete: {hue.fg.orn}Using debug input file {mfile}{hue.rst}")
#         start_and_loop(0)
# else:

start_and_loop(True)     # Set Flask debugging to on
