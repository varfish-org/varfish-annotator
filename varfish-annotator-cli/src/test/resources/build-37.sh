#!/usr/bin/bash

export LC_ALL=C

PATHS_VCF_37="
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/ExAC/r1/ExAC.r1.sites.vep.vcf.gz
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/gnomAD_exomes/r2.1.1/download/gnomad.exomes.r2.1.1.sites.chr1.stripped.vcf.bgz
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/gnomAD_genomes/r2.1.1/download/gnomad.genomes.r2.1.1.sites.chr1.stripped.vcf.bgz
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/thousand_genomes/phase3/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.sites.vcf.gz
"

PATHS_TSV_37="
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/hgmd_public/ensembl_r104/HgmdPublicLocus.tsv
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh37/clinvar/20210728/Clinvar.tsv
"

set -x

for f in $PATHS_TSV_37; do
    head -n 1 $f \
    > $(basename $f)

    grep '^GRCh37\s1\s' $f \
    | sort -k2,2 -k3,3n \
    | head -n 1000 \
    >>$(basename $f)
done

for f in $PATHS_VCF_37; do
    g=${f%.gz}
    tabix --only-header $f \
    > grch37/$(basename $g)
    tabix $f 1 \
    | head -n 1000 \
    >> grch37/$(basename $g)

    bgzip grch37/$(basename $g)
    tabix -f grch37/$(basename $g).gz
done

java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/refseq_curated
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/refseq
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/ensembl

cp ./data/hg19_ensembl.ser ./data/hg19_refseq_curated.ser ./data/hg19_refseq.ser /home/holtgrem_c/Development/varfish/varfish-annotator/varfish-annotator-cli/src/test/resources/grch37