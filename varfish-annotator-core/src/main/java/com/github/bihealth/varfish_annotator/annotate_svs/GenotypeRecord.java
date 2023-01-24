package com.github.bihealth.varfish_annotator.annotate_svs;

import static com.github.bihealth.varfish_annotator.utils.StringUtils.tripleQuote;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class GenotypeRecord {
  /** Header fields for the SV and genotype file (part 1). */
  private static final ImmutableList<String> HEADERS_GT_PART_1 =
      ImmutableList.of("release", "chromosome", "chromosome_no", "bin");
  /** Header fields for the SV and genotype file (part 2, optional). */
  private static final ImmutableList<String> HEADERS_GT_PART_2 =
      ImmutableList.of("chromosome2", "chromosome_no2", "bin2", "pe_orientation");
  /** Header fields for the SV and genotype file (part 3). */
  private static final ImmutableList<String> HEADERS_GT_PART_3_1 =
      ImmutableList.of(
          "start",
          "end",
          "start_ci_left",
          "start_ci_right",
          "end_ci_left",
          "end_ci_right",
          "case_id",
          "set_id",
          "sv_uuid");

  private static final ImmutableList<String> HEADERS_GT_PART_3_2 =
      ImmutableList.of("sv_type", "sv_sub_type", "info");
  /** Header fields for the SV and genotype file (part 4, optional). */
  private static final ImmutableList<String> HEADERS_GT_PART_4 =
      ImmutableList.of("num_hom_alt", "num_hom_ref", "num_het", "num_hemi_alt", "num_hemi_ref");
  /** Header fields for the SV and genotype file (part 5). */
  private static final ImmutableList<String> HEADERS_GT_PART_5 = ImmutableList.of("genotype");

  private final String release;
  private final String chromosome;
  private final int chromosomeNo;
  private final int bin;
  private final String chromosome2;
  private final int chromosomeNo2;
  private final int bin2;
  private final String peOrientation;
  private final int start;
  private final int end;
  private final int startCiLeft;
  private final int startCiRight;
  private final int endCiLeft;
  private final int endCiRight;
  private final String caseId;
  private final String setId;
  private final String svUuid;
  private final String caller;
  private final ImmutableList<String> callers;
  private final String svType;
  private final String svSubType;
  private final ImmutableMap<String, Object> info;

  private final int numHomAlt;
  private final int numHomRef;
  private final int numHet;
  private final int numHemiAlt;
  private final int numHemiRef;
  private final ImmutableMap<String, Object> genotype;

  public GenotypeRecord(
      String release,
      String chromosome,
      int chromosomeNo,
      int bin,
      String chromosome2,
      int chromosomeNo2,
      int bin2,
      String peOrientation,
      int start,
      int end,
      int startCiLeft,
      int startCiRight,
      int endCiLeft,
      int endCiRight,
      String caseId,
      String setId,
      String svUuid,
      String caller,
      Collection<String> callers,
      String svType,
      String svSubType,
      Map<String, Object> info,
      int numHomAlt,
      int numHomRef,
      int numHet,
      int numHemiAlt,
      int numHemiRef,
      Map<String, Object> genotype) {
    this.release = release;
    this.chromosome = chromosome;
    this.chromosomeNo = chromosomeNo;
    this.bin = bin;
    this.chromosome2 = chromosome2;
    this.chromosomeNo2 = chromosomeNo2;
    this.bin2 = bin2;
    this.peOrientation = peOrientation;
    this.start = start;
    this.end = end;
    this.startCiLeft = startCiLeft;
    this.startCiRight = startCiRight;
    this.endCiLeft = endCiLeft;
    this.endCiRight = endCiRight;
    this.caseId = caseId;
    this.setId = setId;
    this.svUuid = svUuid;
    this.caller = caller;
    this.callers = ImmutableList.copyOf(callers);
    this.svType = svType;
    this.svSubType = svSubType;
    this.info = ImmutableMap.copyOf(info);
    this.numHomAlt = numHomAlt;
    this.numHomRef = numHomRef;
    this.numHet = numHet;
    this.numHemiAlt = numHemiAlt;
    this.numHemiRef = numHemiRef;
    this.genotype = ImmutableMap.copyOf(genotype);
  }

  public static String tsvHeader(
      boolean showChrom2Columns, boolean showDbCountColumns, boolean showCallersArrayColumn) {
    // (Conditionally) write genotype header.
    final List<java.lang.String> headers = new ArrayList<>();
    headers.addAll(HEADERS_GT_PART_1);
    if (showChrom2Columns) {
      headers.addAll(HEADERS_GT_PART_2);
    }
    headers.addAll(HEADERS_GT_PART_3_1);
    if (showCallersArrayColumn) {
      headers.add("callers");
    } else {
      headers.add("caller");
    }
    headers.addAll(HEADERS_GT_PART_3_2);
    if (showDbCountColumns) {
      headers.addAll(HEADERS_GT_PART_4);
    }
    headers.addAll(HEADERS_GT_PART_5);
    return Joiner.on("\t").join(headers);
  }

  public static GenotypeRecord fromTsv(List<String> values, List<String> headers) {
    final GenotypeRecordBuilder builder = new GenotypeRecordBuilder();
    for (int i = 0; i < values.size(); i++) {
      final String value = values.get(i);
      final String header = headers.get(i);
      switch (header) {
        case "release":
          builder.setRelease(value);
          break;
        case "chromosome":
          builder.setChromosome(value);
          break;
        case "chromosome_no":
          builder.setChromosomeNo(Integer.parseInt(value));
          break;
        case "bin":
          builder.setBin(Integer.parseInt(value));
          break;
        case "chromosome2":
          builder.setChromosome2(value);
          break;
        case "chromosome_no2":
          builder.setChromosomeNo2(Integer.parseInt(value));
          break;
        case "bin2":
          builder.setBin2(Integer.parseInt(value));
          break;
        case "pe_orientation":
          builder.setPeOrientation(value);
          break;
        case "start":
          builder.setStart(Integer.parseInt(value));
          break;
        case "end":
          builder.setEnd(Integer.parseInt(value));
          break;
        case "start_ci_left":
          builder.setStartCiLeft(Integer.parseInt(value));
          break;
        case "start_ci_right":
          builder.setStartCiRight(Integer.parseInt(value));
          break;
        case "end_ci_left":
          builder.setEndCiLeft(Integer.parseInt(value));
          break;
        case "end_ci_right":
          builder.setEndCiRight(Integer.parseInt(value));
          break;
        case "case_id":
          builder.setCaseId(value);
          break;
        case "set_id":
          builder.setSetId(value);
          break;
        case "sv_uuid":
          builder.setSvUuid(value);
          break;
        case "caller":
          builder.setCaller(value);
          break;
        case "callers":
          builder.setCallers(parseArray(value));
          break;
        case "sv_type":
          builder.setSvType(value);
          break;
        case "sv_sub_type":
          builder.setSvSubType(value);
          break;
        case "info":
          builder.setInfo(parseJsonObjectValue(value));
          break;
        case "num_hom_alt":
          builder.setNumHomAlt(Integer.parseInt(value));
          break;
        case "num_hom_ref":
          builder.setNumHomRef(Integer.parseInt(value));
          break;
        case "num_het":
          builder.setNumHet(Integer.parseInt(value));
          break;
        case "num_hemi_alt":
          builder.setNumHemiAlt(Integer.parseInt(value));
          break;
        case "num_hemi_ref":
          builder.setNumHemiRef(Integer.parseInt(value));
          break;
        case "genotype":
          builder.setGenotype(parseJsonObjectValue(value));
          break;
        default:
          throw new RuntimeException("Unknown header: " + header);
      }
    }

    // Handle the case that either case/cases is empty and the other is not.
    if ((builder.getCaller() == null || ".".equals(builder.getCaller()))
        && !builder.getCallers().isEmpty()) {
      builder.setCaller(Joiner.on(";").join(builder.getCallers()));
    } else if ((builder.getCaller() != null && !".".equals(builder.getCaller()))
        && builder.getCallers().isEmpty()) {
      builder.setCallers(Arrays.asList(builder.getCaller().split(";")));
    }

    return builder.build();
  }

  private static List<String> parseArray(String value) {
    if (!value.startsWith("{") || !value.endsWith("}")) {
      throw new RuntimeException("Could not parse postgres array from: " + value);
    } else {
      final List<String> result = new ArrayList<>();
      final String[] split = value.substring(1, value.length() - 1).split(",");
      for (int i = 0; i < split.length; i++) {
        final String s = split[i];
        if (s.startsWith("\"")) {
          result.add(s.substring(1, s.length() - 1));
        } else {
          result.add(s);
        }
      }
      return result;
    }
  }

  private static Map<String, Object> parseJsonObjectValue(String value) {
    if (!value.startsWith("{") || !value.endsWith("}")) {
      throw new RuntimeException("Not a valid JSON object: " + value);
    }
    final String jsonValue = value.replace("\"\"\"", "\"");
    final JSONObject infoObj = new JSONObject(jsonValue);
    return jsonObjectToMap(infoObj);
  }

  private static Map<String, Object> jsonObjectToMap(JSONObject jsonObject) {
    final Map<String, Object> result = new TreeMap<>();
    for (String key : jsonObject.keySet()) {
      final Object value = jsonObject.get(key);
      if (value instanceof Integer
          || value instanceof Double
          || value instanceof Float
          || value instanceof String) {
        result.put(key, value);
      } else if (value instanceof BigDecimal) {
        final BigDecimal bdValue = (BigDecimal) value;
        result.put(key, bdValue.doubleValue());
      } else if (value instanceof JSONObject) {
        result.put(key, jsonObjectToMap((JSONObject) value));
      } else if (value instanceof JSONArray) {
        result.put(key, ((JSONArray) value).toList());
      } else {
        throw new RuntimeException(
            "Cannot handle JSON object: " + value + " of type " + value.getClass());
      }
    }
    return result;
  }

  public String toTsv(
      boolean showChrom2Columns, boolean showDbCountColumns, boolean showCallersArrayColumn) {
    final ImmutableList.Builder builder = ImmutableList.builder();
    builder.add(release, chromosome, chromosomeNo, bin);
    if (showChrom2Columns) {
      builder.add(chromosome2, chromosomeNo2, bin2, peOrientation);
    }
    builder.add(
        start, end, startCiLeft, startCiRight, endCiLeft, endCiRight, caseId, setId, svUuid);
    if (showCallersArrayColumn) {
      final ImmutableList.Builder<String> innerBuilder = ImmutableList.builder();
      innerBuilder.add("{");
      boolean first = true;
      for (String caller : callers) {
        if (!first) {
          innerBuilder.add(",");
        }
        innerBuilder.add("\"");
        innerBuilder.add(caller);
        innerBuilder.add("\"");
        first = false;
      }
      innerBuilder.add("}");
      builder.add(Joiner.on("").join(innerBuilder.build()));
    } else {
      builder.add(caller);
    }
    builder.add(svType, svSubType);
    builder.add(convert(info));
    if (showDbCountColumns) {
      builder.add(numHomAlt, numHomRef, numHet, numHemiAlt, numHemiRef);
    }
    builder.add(convert(genotype));
    return Joiner.on("\t").useForNull(".").join(builder.build());
  }

  private String convert(Object value) {
    final ImmutableList.Builder innerBuilder = ImmutableList.builder();
    pushValue(value, innerBuilder);
    return Joiner.on("").useForNull(".").join(innerBuilder.build());
  }

  private void pushValue(Object value, ImmutableList.Builder builder) {
    if (value instanceof Integer || value instanceof Double || value instanceof Float) {
      builder.add(value.toString());
    } else if (value instanceof String) {
      builder.add(tripleQuote(value.toString()));
    } else if (value instanceof List) {
      final List<Object> valueAsList = (List) value;
      builder.add("[");
      boolean first = true;
      for (Object element : valueAsList) {
        if (!first) {
          builder.add(",");
        }
        pushValue(element, builder);
        first = false;
      }
      builder.add("]");
    } else if (value instanceof Map) {
      final Map<String, Object> valueAsMap = (Map) value;
      builder.add("{");
      boolean first = true;
      for (String key : valueAsMap.keySet()) {
        if (!first) {
          builder.add(",");
        }
        pushValue(key, builder);
        builder.add(":");
        pushValue(valueAsMap.get(key), builder);
        first = false;
      }
      builder.add("}");
    } else {
      throw new RuntimeException("Could not encode " + value);
    }
  }

  public String getRelease() {
    return release;
  }

  public String getChromosome() {
    return chromosome;
  }

  public int getChromosomeNo() {
    return chromosomeNo;
  }

  public int getBin() {
    return bin;
  }

  public String getChromosome2() {
    return chromosome2;
  }

  public int getChromosomeNo2() {
    return chromosomeNo2;
  }

  public int getBin2() {
    return bin2;
  }

  public String getPeOrientation() {
    return peOrientation;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public int getStartCiLeft() {
    return startCiLeft;
  }

  public int getStartCiRight() {
    return startCiRight;
  }

  public int getEndCiLeft() {
    return endCiLeft;
  }

  public int getEndCiRight() {
    return endCiRight;
  }

  public String getCaseId() {
    return caseId;
  }

  public String getSetId() {
    return setId;
  }

  public String getSvUuid() {
    return svUuid;
  }

  public String getCaller() {
    return caller;
  }

  public ImmutableList<String> getCallers() {
    return callers;
  }

  public String getSvType() {
    return svType;
  }

  public String getSvSubType() {
    return svSubType;
  }

  public ImmutableMap<String, Object> getInfo() {
    return info;
  }

  public int getNumHomAlt() {
    return numHomAlt;
  }

  public int getNumHomRef() {
    return numHomRef;
  }

  public int getNumHet() {
    return numHet;
  }

  public int getNumHemiAlt() {
    return numHemiAlt;
  }

  public int getNumHemiRef() {
    return numHemiRef;
  }

  public ImmutableMap<String, Object> getGenotype() {
    return genotype;
  }

  @Override
  public String toString() {
    return "GenotypeRecord{"
        + "release='"
        + release
        + '\''
        + ", chromosome='"
        + chromosome
        + '\''
        + ", chromosomeNo="
        + chromosomeNo
        + ", bin="
        + bin
        + ", chromosome2='"
        + chromosome2
        + '\''
        + ", chromosomeNo2="
        + chromosomeNo2
        + ", bin2="
        + bin2
        + ", peOrientation='"
        + peOrientation
        + '\''
        + ", start="
        + start
        + ", end="
        + end
        + ", startCiLeft="
        + startCiLeft
        + ", startCiRight="
        + startCiRight
        + ", endCiLeft="
        + endCiLeft
        + ", endCiRight="
        + endCiRight
        + ", caseId='"
        + caseId
        + '\''
        + ", setId='"
        + setId
        + '\''
        + ", svUuid='"
        + svUuid
        + '\''
        + ", caller='"
        + caller
        + '\''
        + ", callers='"
        + callers
        + '\''
        + ", svType='"
        + svType
        + '\''
        + ", svSubType='"
        + svSubType
        + '\''
        + ", info="
        + info
        + ", numHomAlt="
        + numHomAlt
        + ", numHomRef="
        + numHomRef
        + ", numHet="
        + numHet
        + ", numHemiAlt="
        + numHemiAlt
        + ", numHemiRef="
        + numHemiRef
        + ", genotype="
        + genotype
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GenotypeRecord that = (GenotypeRecord) o;
    return getChromosomeNo() == that.getChromosomeNo()
        && getBin() == that.getBin()
        && getChromosomeNo2() == that.getChromosomeNo2()
        && getBin2() == that.getBin2()
        && getStart() == that.getStart()
        && getEnd() == that.getEnd()
        && getStartCiLeft() == that.getStartCiLeft()
        && getStartCiRight() == that.getStartCiRight()
        && getEndCiLeft() == that.getEndCiLeft()
        && getEndCiRight() == that.getEndCiRight()
        && numHomAlt == that.numHomAlt
        && numHomRef == that.numHomRef
        && numHet == that.numHet
        && numHemiAlt == that.numHemiAlt
        && numHemiRef == that.numHemiRef
        && Objects.equal(getRelease(), that.getRelease())
        && Objects.equal(getChromosome(), that.getChromosome())
        && Objects.equal(getChromosome2(), that.getChromosome2())
        && Objects.equal(getPeOrientation(), that.getPeOrientation())
        && Objects.equal(getCaseId(), that.getCaseId())
        && Objects.equal(getSetId(), that.getSetId())
        && Objects.equal(getSvUuid(), that.getSvUuid())
        && Objects.equal(getCaller(), that.getCaller())
        && Objects.equal(getCallers(), that.getCallers())
        && Objects.equal(getSvType(), that.getSvType())
        && Objects.equal(getSvSubType(), that.getSvSubType())
        && Objects.equal(getInfo(), that.getInfo())
        && Objects.equal(genotype, that.genotype);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getRelease(),
        getChromosome(),
        getChromosomeNo(),
        getBin(),
        getChromosome2(),
        getChromosomeNo2(),
        getBin2(),
        getPeOrientation(),
        getStart(),
        getEnd(),
        getStartCiLeft(),
        getStartCiRight(),
        getEndCiLeft(),
        getEndCiRight(),
        getCaseId(),
        getSetId(),
        getSvUuid(),
        getCaller(),
        callers,
        getSvType(),
        getSvSubType(),
        getInfo(),
        numHomAlt,
        numHomRef,
        numHet,
        numHemiAlt,
        numHemiRef,
        genotype);
  }

  public static class Compare implements Comparator<GenotypeRecord> {

    @Override
    public int compare(GenotypeRecord lhs, GenotypeRecord rhs) {
      return ComparisonChain.start()
          .compare(lhs.getRelease(), rhs.getRelease())
          .compare(lhs.getChromosomeNo(), rhs.getChromosomeNo())
          .compare(lhs.getStart(), rhs.getStart())
          .compare(lhs.getEnd(), rhs.getEnd())
          .result();
    }
  }
}
