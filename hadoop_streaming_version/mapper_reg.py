#!/usr/bin/python 
# -*- coding: utf-8 -*-

##To calculate coefficients in a regression problem, we need to track four values:
##XY, Y, X, X^2, N, then we can sum them up and use formula to calculate coefficients
##Here, notice x and y are in different files thus should be processed separately

import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split(',')
    try:
        Y = float(words[-1]) - float(words[-2])
        X = float(words[-3])
    except ValueError:
        continue
    
    print '%s\t%s' % ('XY', X * Y)
    print '%s\t%s' % ('X', X)
    print '%s\t%s' % ('Y', Y)
    print '%s\t%s' % ('X2', X ** 2)
    print '%s\t%s' % ('N', 1)
