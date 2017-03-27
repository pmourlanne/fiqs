# -*- coding: utf-8 -*-

import gettext
import os
import sys


if sys.version_info[0] >= 3:
    _ = gettext.gettext
else:
    localedir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'locale')
    translate = gettext.translation('fiqs', localedir, fallback=True)
    _ = translate.ugettext
