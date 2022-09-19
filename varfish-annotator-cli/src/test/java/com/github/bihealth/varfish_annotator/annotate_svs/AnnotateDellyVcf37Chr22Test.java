package com.github.bihealth.varfish_annotator.annotate_svs;

import com.ginsberg.junit.exit.FailOnSystemExit;
import com.github.bihealth.varfish_annotator.ResourceUtils;
import com.github.bihealth.varfish_annotator.VarfishAnnotatorCli;
import com.github.bihealth.varfish_annotator.utils.GzipUtil;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Test annotation of VCF files generated with Delly 2 for GRCh37 */
public class AnnotateDellyVcf37Chr22Test {

  @TempDir public File tmpFolder;
  File h2DbFile;
  File ensemblSerFile;
  File refseqSerFile;

  @BeforeEach
  void initEach() {
    h2DbFile = new File(tmpFolder + "/small-grch37.h2.db");
    ensemblSerFile = new File(tmpFolder + "/hg19_ensembl.ser");
    refseqSerFile = new File(tmpFolder + "/hg19_refseq_curated.ser");

    ResourceUtils.copyResourceToFile("/grch37-chr22/small-grch37-chr22.h2.db", h2DbFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/hg19_ensembl.ser", ensemblSerFile);
    ResourceUtils.copyResourceToFile("/grch37-chr22/hg19_refseq.ser", refseqSerFile);
  }

  void runTest(
      String inputFileName,
      String inputPedFileName,
      String inputPath,
      String expectedDbInfos,
      String expectedGts,
      String expectedFeatureEffects,
      boolean gzipOutput,
      boolean selfTestChr22Only,
      boolean optOutChrom2Columns,
      boolean optOutDbcountsColumns)
      throws IOException {
    final String gzSuffix = gzipOutput ? ".gz" : "";
    final File vcfPath = new File(tmpFolder + "/" + inputFileName);
    final File tbiPath = new File(vcfPath + ".tbi");
    final File pedPath = new File(tmpFolder + "/" + inputPedFileName);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + vcfPath.getName(), vcfPath);
    ResourceUtils.copyResourceToFile("/" + inputPath + "/" + tbiPath.getName(), tbiPath);

    final File outputDbInfoPath =
        new File(tmpFolder + "/Case_1_index.gatk_hc.db-info.tsv-expected" + gzSuffix);
    final File outputFeatureEffects =
        new File(tmpFolder + "/output.feature-effects.tsv" + gzSuffix);
    final File outputGtsPath = new File(tmpFolder + "/output.gts.tsv" + gzSuffix);

    final ArrayList<String> args =
        Lists.newArrayList(
            "annotate-svs",
            "--release",
            "GRCh37",
            "--sequential-uuids",
            "--db-path",
            h2DbFile.toString(),
            "--input-vcf",
            vcfPath.toString(),
            "--refseq-ser-path",
            refseqSerFile.toString(),
            "--ensembl-ser-path",
            ensemblSerFile.toString(),
            "--output-db-info",
            outputDbInfoPath.toString(),
            "--output-gts",
            outputGtsPath.toString(),
            "--output-feature-effects",
            outputFeatureEffects.toString());
    if (selfTestChr22Only) {
      args.add("--self-test-chr22-only");
    }
    if (inputPedFileName != null) {
      args.add("--input-ped");
      args.add(pedPath.toString());
      ResourceUtils.copyResourceToFile("/" + inputPath + "/" + pedPath.getName(), pedPath);
    }
    String optOuts = "";
    if (optOutChrom2Columns) {
      optOuts = optOuts += "chrom2-columns";
    }
    if (optOutDbcountsColumns) {
      if (!optOuts.isEmpty()) {
        optOuts += ",";
      }
      optOuts += "dbcounts-columns";
    }
    if (!optOuts.isEmpty()) {
      args.add("--opt-out");
      args.add(optOuts);
    }
    final String[] argsArr = new String[args.size()];
    args.toArray(argsArr);
    VarfishAnnotatorCli.main(argsArr);

    if (gzipOutput) {
      final File paths[] = {outputDbInfoPath, outputGtsPath, outputFeatureEffects};
      for (File path : paths) {
        try (FileInputStream fin = new FileInputStream(path)) {
          Assertions.assertTrue(GzipUtil.isGZipped(fin));
        }
      }
    } else {
      Assertions.assertEquals(
          expectedDbInfos,
          FileUtils.readFileToString(outputDbInfoPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          expectedGts,
          FileUtils.readFileToString(outputGtsPath, "utf-8").replaceAll("\\r\\n?", "\n"));
      Assertions.assertEquals(
          expectedFeatureEffects,
          FileUtils.readFileToString(outputFeatureEffects, "utf-8").replaceAll("\\r\\n?", "\n"));
    }
  }

  @FailOnSystemExit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithTrio(boolean gzipOutput) throws IOException {
    final String expectedDbInfo =
        "genomebuild\tdb_name\trelease\n"
            + "GRCh37\tclinvar\tfor-testing\n"
            + "GRCh37\texac\tr1.0\n"
            + "GRCh37\tgnomad_exomes\tr2.1.1\n"
            + "GRCh37\tgnomad_genomes\tr2.1.1\n"
            + "GRCh37\thgmd_public\tfor-testing\n"
            + "GRCh37\tthousand_genomes\tv3.20101123\n"
            + "GRCh37\tvarfish-annotator\tnull\n"
            + "GRCh37\tvarfish-annotator-db\tfor-testing\n";
    final String expectedGts =
        "release\tchromosome\tchromosome_no\tbin\tchromosome2\tchromosome_no2\tbin2\tpe_orientation\tstart\tend\tstart_ci_left\tstart_ci_right\tend_ci_left\tend_ci_right\tcase_id\tset_id\tsv_uuid\tcaller\tsv_type\tsv_sub_type\tinfo\tnum_hom_alt\tnum_hom_ref\tnum_het\tnum_hemi_alt\tnum_hemi_ref\tgenotype\n"
            + "GRCh37\t22\t22\t89\t22\t22\t89\t3to5\t17400000\t17700000\t-29\t29\t-29\t29\t.\t.\t00000000-0000-0000-0000-000000000000\tEMBL.DELLYv1.1.3\tDEL\tDEL\t{\"\"\"affectedCarriers\"\"\":0,\"\"\"backgroundCarriers\"\"\":0,\"\"\"unaffectedCarriers\"\"\":0}\t0\t2\t1\t0\t0\t{\"\"\"Case_1_father-N1-DNA1-WGS1\"\"\":{\"\"\"cn\"\"\":2,\"\"\"ft\"\"\":[\"\"\"LowQual\"\"\"],\"\"\"gq\"\"\":14,\"\"\"gt\"\"\":\"\"\"0/1\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":34,\"\"\"srv\"\"\":4},\"\"\"Case_1_index-N1-DNA1-WGS1\"\"\":{\"\"\"cn\"\"\":2,\"\"\"gq\"\"\":35,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":29,\"\"\"srv\"\"\":2},\"\"\"Case_1_mother-N1-DNA1-WGS1\"\"\":{\"\"\"cn\"\"\":2,\"\"\"gq\"\"\":67,\"\"\"gt\"\"\":\"\"\"0/0\"\"\",\"\"\"pec\"\"\":0,\"\"\"pev\"\"\":0,\"\"\"src\"\"\":32,\"\"\"srv\"\"\":1}}\n";
    final String expectedFeatureEffects =
        "case_id\tset_id\tsv_uuid\trefseq_gene_id\trefseq_transcript_id\trefseq_transcript_coding\trefseq_effect\tensembl_gene_id\tensembl_transcript_id\tensembl_transcript_coding\tensembl_effect\n"
            + ".\t.\t00000000-0000-0000-0000-000000000000\t51816\tNM_017424.2\tTRUE\t{\"transcript_ablation\",\"coding_transcript_variant\"}\tENSG00000093072\tENST00000262607.3\tTRUE\t{\"transcript_ablation\",\"coding_transcript_variant\"}\n"
            + ".\t.\t00000000-0000-0000-0000-000000000000\t128954\tNM_001037814.1\tTRUE\t{\"transcript_ablation\",\"coding_transcript_variant\"}\tENSG00000215568\tENST00000400588.1\tTRUE\t{\"transcript_ablation\",\"coding_transcript_variant\"}\n";
    runTest(
        "Case_1_index.delly2.vcf.gz",
        null,
        "input/grch37-chr22",
        expectedDbInfo,
        expectedGts,
        expectedFeatureEffects,
        gzipOutput,
        true,
        false,
        false);
  }
}
