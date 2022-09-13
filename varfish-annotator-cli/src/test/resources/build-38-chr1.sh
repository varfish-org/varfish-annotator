#!/usr/bin/bash

export LC_ALL=C

PATHS_VCF_38="
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh38/gnomAD_exomes/r2.1.1/download/gnomad.exomes.r2.1.1.sites.chr1.stripped.vcf.bgz
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh38/gnomAD_genomes/r3.1.1/download/gnomad.genomes.r3.1.1.sites.chr1.stripped.vcf.bgz
"

PATHS_TSV_38="
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh38/hgmd_public/ensembl_r104/HgmdPublicLocus.tsv
/fast/groups/cubi/work/projects/2021-07-20_varfish-db-downloader-holtgrewe/varfish-db-downloader/GRCh38/clinvar/20210728/Clinvar.tsv
"

set -x

for f in $PATHS_TSV_38; do
    head -n 1 $f \
    > $(basename $f)

    egrep '^GRCh38\s(chr)?1\s' $f \
    | sort -k2,2 -k3,3n \
    | head -n 1000 \
    >>$(basename $f)
done

for f in $PATHS_VCF_38; do
    g=${f%.gz}
    tabix --only-header $f \
    > $(basename $g)
    tabix $f chr1 \
    | head -n 1000 \
    >> $(basename $g)

    bgzip $(basename $g)
    tabix -f $(basename $g).gz
done

java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/refseq_curated
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/refseq
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg19/ensembl

cp ./data/hg19_ensembl.ser ./data/hg19_refseq_curated.ser ./data/hg19_refseq.ser /home/holtgrem_c/Development/varfish/varfish-annotator/varfish-annotator-cli/src/test/resources/grch37

java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg38/refseq_curated
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg38/refseq
java -jar jannovar-cli/target/jannovar-cli-0.40-SNAPSHOT.jar download --gene-ids SAMD11 --gene-ids ENSG00000187634 --gene-ids 148398 -d hg38/ensembl

cp ./data/hg38_ensembl.ser ./data/hg38_refseq_curated.ser ./data/hg38_refseq.ser /home/holtgrem_c/Development/varfish/varfish-annotator/varfish-annotator-cli/src/test/resources/grch38