#!/usr/bin/python 
# -*- coding: utf-8 -*-

##while mapreduce is not the best choice for decile computation problem(as we will see, spark is much faster), 
##we will still do this for the completeness of comparison. We generate a frequency table for the values then 
##finding deciles would become much easier.
 
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(',')
    # increase counters
    try:
        word = float(words[-1]) - float(words[-2]) ##compute total amount less tolls amount
    except ValueError:
        continue  ##skip the header and other wrong formatted rows
    print '%s\t%s' % (word, 1)
