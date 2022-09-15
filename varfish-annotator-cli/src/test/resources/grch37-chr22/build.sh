#!/usr/bin/bash

set -euo pipefail
set -x

if [[ ! -e chr22.fa ]]; then
  wget -O chr22.fa.gz.tmp https://hgdownload.cse.ucsc.edu/goldenpath/hg19/chromosomes/chr22.fa.gz
  zcat chr22.fa.gz.tmp >chr22.fa.tmp
  mv chr22.fa.tmp chr22.fa
fi

if [[ ! -e chr22_part.fa.fai ]]; then
  samtools faidx chr22.fa chr22:1-22,000,000 > chr22_part.fa.tmp
  perl -p -i -e 's/^>chr.*/>22/g' chr22_part.fa.tmp
  gzip -c chr22_part.fa.tmp >chr22_part.fa.gz
  mv chr22_part.fa.tmp chr22_part.fa
  samtools faidx chr22_part.fa
fi

# ADA2(hg19): 22:17,660,192-17,680,545
# GAB4(hg19): 22:17,442,827-17,489,112

if [[ ! -e hg19_refseq.ser ]]; then
  jannovar -Xmx4096m download -d hg19/ensembl --gene-ids ENSG00000093072 ENSG00000215568  # ADA2 GAB4
  cp data/hg19_refseq.ser hg19_refseq.ser.tmp
  mv hg19_refseq.ser.tmp hg19_refseq.ser
fi

if [[ ! -e hg19_ensembl.ser ]]; then
  jannovar -Xmx4096m download -d hg19/ensembl --gene-ids 51816 128954  # ADA2 GAB4
  cp data/hg19_ensembl.ser hg19_ensembl.ser.tmp
  mv hg19_ensembl.ser.tmp hg19_ensembl.ser
fi

REGIONS="22:17,660,192-17,680,545 22:17,442,827-17,489,112"
BASEDIR=/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader

( \
  tabix --only-header $BASEDIR/GRCh37/ExAC/r1/download/ExAC.r1.sites.vep.vcf.gz $REGIONS; \
  tabix $BASEDIR/GRCh37/ExAC/r1/download/ExAC.r1.sites.vep.vcf.gz $REGIONS \
  | sort -k1,1V -k2,2n \
  | uniq; \
) \
| bgzip -c \
> ExAC.r1.sites.vep.vcf.gz
tabix -f ExAC.r1.sites.vep.vcf.gz

( \
  tabix --only-header $BASEDIR/GRCh37/gnomAD_exomes/r2.1.1/download/gnomad.exomes.r2.1.1.sites.chr22.vcf.bgz $REGIONS; \
  tabix $BASEDIR/GRCh37/gnomAD_exomes/r2.1.1/download/gnomad.exomes.r2.1.1.sites.chr22.vcf.bgz $REGIONS \
  | sort -k1,1V -k2,2n \
  | uniq; \
) \
| bgzip -c \
> gnomad.exomes.r2.1.1.sites.chr22.vcf.bgz
tabix -f gnomad.exomes.r2.1.1.sites.chr22.vcf.bgz

( \
  tabix --only-header $BASEDIR/GRCh37/gnomAD_genomes/r2.1.1/download/gnomad.genomes.r2.1.1.sites.chr22.vcf.bgz $REGIONS; \
  tabix $BASEDIR/GRCh37/gnomAD_genomes/r2.1.1/download/gnomad.genomes.r2.1.1.sites.chr22.vcf.bgz $REGIONS \
  | sort -k1,1V -k2,2n \
  | uniq; \
) \
| bgzip -c \
> gnomad.genomes.r2.1.1.sites.chr22.vcf.bgz
tabix -f gnomad.genomes.r2.1.1.sites.chr22.vcf.bgz

( \
  tabix --only-header $BASEDIR/GRCh37/thousand_genomes/phase3/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz; \
  tabix $BASEDIR/GRCh37/thousand_genomes/phase3/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz $REGIONS \
  | sort -k1,1V -k2,2n \
  | uniq; \
) \
| bgzip -c \
> ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
tabix -f ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz

(head -n 1 /tmp/Clinvar.tsv; tail -n +2 /tmp/Clinvar.tsv | sort -k2,2V -k3,3n -k4,4n) \
| bgzip -c >/tmp/Clinvar.tsv.gz
tabix -S 1 -b 3 -e 4 -s 2 -f /tmp/Clinvar.tsv.gz
head -n 1 /tmp/Clinvar.tsv >Clinvar.tsv.tmp
tabix /tmp/Clinvar.tsv.gz $REGIONS \
>>Clinvar.tsv.tmp
gzip Clinvar.tsv.tmp
mv Clinvar.tsv.tmp.gz Clinvar.tsv.gz

(head -n 1 /tmp/HgmdPublicLocus.tsv; tail -n +2 /tmp/HgmdPublicLocus.tsv | sort -k2,2V -k3,3n -k4,4n) \
| bgzip -c >/tmp/HgmdPublicLocus.tsv.gz
tabix -S 1 -b 3 -e 4 -s 2 -f /tmp/HgmdPublicLocus.tsv.gz
head -n 1 /tmp/HgmdPublicLocus.tsv >HgmdPublicLocus.tsv.tmp
tabix /tmp/HgmdPublicLocus.tsv.gz $REGIONS \
>>HgmdPublicLocus.tsv.tmp
gzip HgmdPublicLocus.tsv.tmp
mv HgmdPublicLocus.tsv.tmp.gz HgmdPublicLocus.tsv.gz

BASEDIR=/fast/groups/cubi/work/projects/2022-07-06_VarFish_Course_Data/snappy-processing/
VCF=$BASEDIR/variant_calling/output/bwa.gatk_hc.Case_1_index-N1-DNA1-WGS1/out/bwa.gatk_hc.Case_1_index-N1-DNA1-WGS1.vcf.gz

( \
  tabix --only-header $VCF $REGIONS; \
  tabix $VCF $REGIONS \
  | sort -k1,1V -k2,2n \
  | uniq; \
) \
| bgzip -c \
> Case_1_index.gatk_hc.vcf.gz
tabix -f Case_1_index.gatk_hc.vcf.gz

VCF=$BASEDIR/wgs_sv_calling/output/bwa.delly2.Case_1_index-N1-DNA1-WGS1/out/bwa.delly2.Case_1_index-N1-DNA1-WGS1.vcf.gz

( \
  tabix --only-header $VCF $REGIONS; \
  ( \
    cat Case_1_index.delly2.extra.vcf; \
    tabix $VCF $REGIONS; \
  ) \
  | sort -k1,1V -k2,2n \
  | uniq; \b
) \
| bgzip -c \
> Case_1_index.delly2.vcf.gz
tabix -f Case_1_index.delly2.vcf.gz
