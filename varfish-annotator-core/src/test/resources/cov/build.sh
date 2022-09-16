#!/usr/bin/bash

rm -f coverage.vcf.*

bgzip -c coverage.vcf >coverage.vcf.gz
tabix -f coverage.vcf.gz
