#!/usr/bin/python 
# -*- coding: utf-8 -*-

import sys

cur_word = None
cur_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = float(count)
    except ValueError:
        continue

    if cur_word == word:
        cur_count += count
    else:
        if cur_word:
            # write result to STDOUT
            print '%s\t%s' % (cur_word, cur_count)
        cur_count = count
        cur_word = word

# process the last line
if cur_word == word:
    print '%s\t%s' % (cur_word, cur_count)
