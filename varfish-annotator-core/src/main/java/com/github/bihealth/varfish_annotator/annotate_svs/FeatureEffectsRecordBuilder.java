package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import de.charite.compbio.jannovar.annotation.SVAnnotation;
import de.charite.compbio.jannovar.annotation.VariantEffect;
import java.util.*;
import java.util.stream.Collectors;

/** Helper class for building records for the {@code .feature-effects.tsv} file. */
public class FeatureEffectsRecordBuilder {

  public class Result {
    private TreeSet<String> geneIds;
    private HashMap<String, SVAnnotation> ensemblAnnoByGene;
    private HashMap<String, SVAnnotation> refseqAnnoByGene;

    public Result(
        TreeSet<String> geneIds,
        HashMap<String, SVAnnotation> ensemblAnnoByGene,
        HashMap<String, SVAnnotation> refseqAnnoByGene) {
      this.geneIds = geneIds;
      this.ensemblAnnoByGene = ensemblAnnoByGene;
      this.refseqAnnoByGene = refseqAnnoByGene;
    }

    public TreeSet<String> getGeneIds() {
      return geneIds;
    }

    public HashMap<String, SVAnnotation> getEnsemblAnnoByGene() {
      return ensemblAnnoByGene;
    }

    public HashMap<String, SVAnnotation> getRefseqAnnoByGene() {
      return refseqAnnoByGene;
    }
  }

  String caseId;
  String setId;

  public FeatureEffectsRecordBuilder(String caseId, String setId) {
    this.caseId = caseId;
    this.setId = setId;
  }

  public Result buildAnnosByDb(
      List<SVAnnotation> sortedEnsemblAnnos, List<SVAnnotation> sortedRefseqAnnos) {
    // Collecting RefSeq and ENSEMBL annotations per gene.  We collect the variants by gene for
    // RefSeq in the simplest way possible (mapping to ENSEMBL gene by using HGNC annotation from
    // Jannovar). Further, if either only yields one gene we force it to be the same as the
    // (lexicographically first) gene of the other.
    //
    // Intergenic variants are skipped.

    // Start out with the RefSeq annotations
    final HashMap<String, SVAnnotation> refseqAnnoByGene = new HashMap<>();
    for (SVAnnotation annotation : sortedRefseqAnnos) {
      if (annotation.getTranscript() == null) {
        continue; // skip, no transcript
      }
      String geneId = annotation.getTranscript().getAltGeneIDs().get("ENSEMBL_GENE_ID");
      if (geneId == null) {
        geneId = annotation.getTranscript().getGeneID();
      }
      if (!annotation.getEffects().contains(VariantEffect.INTERGENIC_VARIANT)
          && !refseqAnnoByGene.containsKey(geneId)) {
        refseqAnnoByGene.put(geneId, annotation);
      }
    }

    // Now, for the ENSEMBL annotations
    final HashMap<String, SVAnnotation> ensemblAnnoByGene = new HashMap<>();
    for (SVAnnotation annotation : sortedEnsemblAnnos) {
      if (annotation.getTranscript() == null) {
        continue; // skip, no transcript
      }
      final String geneId = annotation.getTranscript().getGeneID();
      if (!annotation.getEffects().contains(VariantEffect.INTERGENIC_VARIANT)
          && !ensemblAnnoByGene.containsKey(geneId)) {
        ensemblAnnoByGene.put(geneId, annotation);
      }
    }

    // Match RefSeq and ENSEMBL annotations
    final TreeSet<String> geneIds = new TreeSet<>();
    if (refseqAnnoByGene.size() == 1 && ensemblAnnoByGene.size() > 0) {
      geneIds.addAll(ensemblAnnoByGene.keySet());
      final String keyEnsembl = ensemblAnnoByGene.keySet().iterator().next();
      final String keyRefseq = refseqAnnoByGene.keySet().iterator().next();
      refseqAnnoByGene.put(keyEnsembl, refseqAnnoByGene.get(keyRefseq));
    } else if (refseqAnnoByGene.size() > 0 && ensemblAnnoByGene.size() == 1) {
      geneIds.addAll(refseqAnnoByGene.keySet());
      final String keyEnsembl = ensemblAnnoByGene.keySet().iterator().next();
      final String keyRefseq = refseqAnnoByGene.keySet().iterator().next();
      ensemblAnnoByGene.put(keyRefseq, ensemblAnnoByGene.get(keyEnsembl));
    } else {
      geneIds.addAll(ensemblAnnoByGene.keySet());
      geneIds.addAll(refseqAnnoByGene.keySet());
    }

    return new Result(geneIds, ensemblAnnoByGene, refseqAnnoByGene);
  }

  public List<Object> buildRecord(
      UUID variantId, SVAnnotation refSeqAnno, SVAnnotation ensemblAnno) {
    List<Object> result = Lists.newArrayList(caseId, setId, variantId.toString());
    if (refSeqAnno == null) {
      result.addAll(Arrays.asList(null, null, "FALSE", "{}"));
    } else {
      result.addAll(
          Arrays.asList(
              refSeqAnno.getTranscript().getGeneID(),
              refSeqAnno.getTranscript().getAccession(),
              refSeqAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
              (refSeqAnno.getEffects() == null)
                  ? "{}"
                  : buildEffectsValue(refSeqAnno.getEffects())));
    }
    if (ensemblAnno == null) {
      result.addAll(Arrays.asList(null, null, "FALSE", "{}"));
    } else {
      result.addAll(
          Arrays.asList(
              ensemblAnno.getTranscript().getGeneID(),
              ensemblAnno.getTranscript().getAccession(),
              ensemblAnno.getTranscript().isCoding() ? "TRUE" : "FALSE",
              (ensemblAnno.getEffects() == null)
                  ? "{}"
                  : buildEffectsValue(ensemblAnno.getEffects())));
    }
    return result;
  }

  /**
   * Build Postgres array expression for TSV file with variant effects.
   *
   * @param effects The effects to create expression for.
   * @return String with the variant effects.
   */
  private String buildEffectsValue(ImmutableSet<VariantEffect> effects) {
    final List<String> effectStrings =
        effects
            .stream()
            .map(e -> "\"" + e.getSequenceOntologyTerm() + "\"")
            .collect(Collectors.toList());
    return Joiner.on("").join("{", Joiner.on(',').join(effectStrings), "}");
  }
}
