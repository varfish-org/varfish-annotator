#!/usr/bin/bash

CHROM=1
sample=SAMPLE

cat >example.SAMPLE.cov.vcf <<EOF
##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##fileDate=20200828
##contig=<ID=1,length=249250621>
##contig=<ID=22,length=51304566>
##ALT=<ID=WINDOW,Description="Record describes a window for read or coverage counting">
##INFO=<ID=END,Number=1,Type=Integer,Description="Window end">
##INFO=<ID=MAPQ,Number=1,Type=Float,Description="Mean MAPQ value across samples for approximating mapability">
##INFO=<ID=GC,Number=1,Type=Float,Description="Reference GC fraction, if reference FASTA file was given">
##INFO=<ID=GAP,Number=0,Type=Flag,Description="Window overlaps with N in reference (gap)">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=MQ,Number=1,Type=Float,Description="Mean read MAPQ from region">
##FORMAT=<ID=RCV,Number=1,Type=Float,Description="Raw coverage value">
##FORMAT=<ID=RCVSD,Number=1,Type=Float,Description="Raw coverage standard deviation">
##FORMAT=<ID=CV,Number=1,Type=Float,Description="Normalized coverage value">
##FORMAT=<ID=CVSD,Number=1,Type=Float,Description="Normalized coverage standard deviation">
##median-coverage=<ID=$sample,autosomes=10,_one=0,_two=0>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	$sample
EOF

pos=1
while [[ $pos -le 20000000 ]]; do
  end=$((pos + 1000 - 1))
  echo -e "$CHROM\t$pos\t$CHROM:$pos-$end\tN\t<WINDOW>\t0\t.\tEND=$end;MAPQ=40\tGT:RCV:RCVSD:MQ:CV:CVSD\t./.:30:1:40:1.0:0.1" \
  >> example.$sample.cov.vcf
  let "pos=$pos+1000"
done

bgzip -c example.$sample.cov.vcf >example.$sample.cov.vcf.gz
tabix -f example.$sample.cov.vcf.gz
