#!/usr/bin/python 
# -*- coding: utf-8 -*-
import sys

cur_key = None
cur_val = 0
key = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, val = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        val = float(val)
    except ValueError:
        continue

    if cur_key == key:
        cur_val += val
    else:
        if cur_key:
            # write result to STDOUT
            print '%s\t%s' % (cur_key, cur_val)
        cur_key = key
        cur_val = val

# process the last line
if cur_key == key:
    print '%s\t%s' % (cur_key, cur_val)

