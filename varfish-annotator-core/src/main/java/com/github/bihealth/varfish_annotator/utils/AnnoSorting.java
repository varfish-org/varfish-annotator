package com.github.bihealth.varfish_annotator.utils;

import com.google.common.collect.ImmutableList;
import de.charite.compbio.jannovar.annotation.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class AnnoSorting {

  public static List<Annotation> sortAnnotations(
      ImmutableList<VariantAnnotations> refseqAnnotationsList, int i) {
    if (refseqAnnotationsList == null) {
      return new ArrayList<>();
    } else {
      return refseqAnnotationsList
          .get(i - 1)
          .getAnnotations()
          .stream()
          .sorted(
              Comparator.<Annotation, VariantEffect>comparing(
                  Annotation::getMostPathogenicVarType,
                  (t1, t2) -> {
                    if (t1 == null && t2 == null) {
                      return 0;
                    } else if (t2 == null) {
                      return -1;
                    } else if (t1 == null) {
                      return 1;
                    } else {
                      return t1.compareTo(t2);
                    }
                  }))
          .collect(Collectors.toList());
    }
  }

  public static List<SVAnnotation> sortSvAnnos(
      ImmutableList<SVAnnotations> refseqAnnotationsList, int i) {
    if (refseqAnnotationsList == null) {
      return new ArrayList<>();
    } else {
      return refseqAnnotationsList
          .get(i - 1)
          .getAnnotations()
          .stream()
          .sorted(
              Comparator.comparing(
                  SVAnnotation::getMostPathogenicVariantEffect,
                  (t1, t2) -> {
                    if (t1 == null && t2 == null) {
                      return 0;
                    } else if (t2 == null) {
                      return -1;
                    } else if (t1 == null) {
                      return 1;
                    } else {
                      return t1.compareTo(t2);
                    }
                  }))
          .collect(Collectors.toList());
    }
  }
}
