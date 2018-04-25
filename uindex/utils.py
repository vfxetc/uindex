from __future__ import print_function

import functools
import re


class cached_property(object):
    
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
    
    def __get__(self, instance, owner_type=None):
        if instance is None:
            return self
        try:
            return instance.__dict__[self.__name__]
        except KeyError:
            value = self.func(instance)
            instance.__dict__[self.__name__] = value
            return value








def prompt_bool(prompt, default=True):
    while True:
        res = raw_input(prompt + ' [{}{}]: '.format('yY'[default], 'Nn'[default])).strip()
        if not res:
            return default
        if res in ('y', 'Y', 'yes'):
            return True
        if res in ('n', 'N', 'no'):
            return False



def parse_size(x):
    m = re.match(r'^(\d+)([BkMG])$', x)
    if not m:
        raise ValueError("Could not parse size.", x)

    num, unit = m.groups()
    return int(num) * (1024 ** dict(B=0, k=1, M=2, G=3)[unit])

def format_bytes(x):
    unit = 0
    while x > 1000:
        unit += 1
        x /= 1024.0
    num = '{:.3f}'.format(x).rstrip('0').rstrip('.')
    return '{}{}B'.format(num, ('', 'k', 'M', 'G', 'T', 'P')[unit])
