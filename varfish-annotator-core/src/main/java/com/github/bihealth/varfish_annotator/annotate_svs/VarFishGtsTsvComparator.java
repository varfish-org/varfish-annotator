package com.github.bihealth.varfish_annotator.annotate_svs;

import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import org.apache.commons.csv.CSVRecord;

public class VarFishGtsTsvComparator implements Comparator<CSVRecord> {

  private boolean hasChrom2Columns;

  public VarFishGtsTsvComparator(boolean hasChrom2Columns) {
    this.hasChrom2Columns = hasChrom2Columns;
  }

  @Override
  public int compare(CSVRecord lhs, CSVRecord rhs) {
    int offset = hasChrom2Columns ? 4 : 0;
    return ComparisonChain.start()
        .compare(lhs.get(0), rhs.get(0))
        .compare(Integer.parseInt(lhs.get(2)), Integer.parseInt(rhs.get(2)))
        .compare(Integer.parseInt(lhs.get(4 + offset)), Integer.parseInt(rhs.get(4 + offset)))
        .compare(Integer.parseInt(lhs.get(5 + offset)), Integer.parseInt(rhs.get(5 + offset)))
        .result();
  }
}
