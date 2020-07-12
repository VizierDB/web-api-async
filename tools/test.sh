#!/bin/bash

if [ "$*" ] ; then
  TESTS="$*"
else 
  TESTS=$(find tests -name '*.py')
fi

python3 -m unittest $TESTS