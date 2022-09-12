#!/usr/bin/bash

# Copy from end-to-end test

mkdir -p grch37-chr22 input/grch37-chr22

ln -sr ../../../../tests/hg19-chr22/* grch37-chr22
rm grch37-chr22/Case_1_*

ln -sr ../../../../tests/hg19-chr22/Case_1_* input/grch37-chr22
