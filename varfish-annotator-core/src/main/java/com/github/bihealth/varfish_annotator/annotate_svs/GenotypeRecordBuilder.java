package com.github.bihealth.varfish_annotator.annotate_svs;

import java.util.Map;
import java.util.TreeMap;

/** Helper for building {@code GenotypeRecord} objects. */
public class GenotypeRecordBuilder {
  private String release;
  private String chromosome;
  private int chromosomeNo;
  private int bin;
  private String chromosome2;
  private int chromosomeNo2;
  private int bin2;
  private String peOrientation;
  private int start;
  private int end;
  private int startCiLeft;
  private int startCiRight;
  private int endCiLeft;
  private int endCiRight;
  private String caseId;
  private String setId;
  private String svUuid;
  private String caller;
  private String svType;
  private String svSubType;
  private Map<String, Object> info = new TreeMap();

  private int numHomAlt;
  private int numHomRef;
  private int numHet;
  private int numHemiAlt;
  private int numHemiRef;
  private Map<String, Object> genotype = new TreeMap();

  public void init(GenotypeRecord record) {
    release = record.getRelease();
    chromosome = record.getChromosome();
    chromosomeNo = record.getChromosomeNo();
    bin = record.getBin();
    chromosome2 = record.getChromosome2();
    bin2 = record.getBin2();
    peOrientation = record.getPeOrientation();
    start = record.getStart();
    end = record.getEnd();
    startCiLeft = record.getStartCiLeft();
    startCiRight = record.getStartCiRight();
    endCiLeft = record.getEndCiLeft();
    endCiRight = record.getEndCiRight();
    caseId = record.getCaseId();
    setId = record.getSetId();
    svUuid = record.getSvUuid();
    caller = record.getCaller();
    svType = record.getSvType();
    svSubType = record.getSvSubType();
    info = new TreeMap();
    info.putAll(record.getInfo());
    numHomAlt = record.getNumHomAlt();
    numHomRef = record.getNumHomRef();
    numHet = record.getNumHet();
    numHemiAlt = record.getNumHemiAlt();
    numHemiRef = record.getNumHemiRef();
    genotype = new TreeMap<>();
    genotype.putAll(record.getGenotype());
  }

  public GenotypeRecord build() {
    return new GenotypeRecord(
        release,
        chromosome,
        chromosomeNo,
        bin,
        chromosome2,
        chromosomeNo2,
        bin2,
        peOrientation,
        start,
        end,
        startCiLeft,
        startCiRight,
        endCiLeft,
        endCiRight,
        caseId,
        setId,
        svUuid,
        caller,
        svType,
        svSubType,
        info,
        numHomAlt,
        numHomRef,
        numHet,
        numHemiAlt,
        numHemiRef,
        genotype);
  }

  public String getRelease() {
    return release;
  }

  public void setRelease(String release) {
    this.release = release;
  }

  public String getChromosome() {
    return chromosome;
  }

  public void setChromosome(String chromosome) {
    this.chromosome = chromosome;
  }

  public int getChromosomeNo() {
    return chromosomeNo;
  }

  public void setChromosomeNo(int chromosomeNo) {
    this.chromosomeNo = chromosomeNo;
  }

  public int getBin() {
    return bin;
  }

  public void setBin(int bin) {
    this.bin = bin;
  }

  public String getChromosome2() {
    return chromosome2;
  }

  public void setChromosome2(String chromosome2) {
    this.chromosome2 = chromosome2;
  }

  public int getChromosomeNo2() {
    return chromosomeNo2;
  }

  public void setChromosomeNo2(int chromosomeNo2) {
    this.chromosomeNo2 = chromosomeNo2;
  }

  public int getBin2() {
    return bin2;
  }

  public void setBin2(int bin2) {
    this.bin2 = bin2;
  }

  public String getPeOrientation() {
    return peOrientation;
  }

  public void setPeOrientation(String peOrientation) {
    this.peOrientation = peOrientation;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  public int getStartCiLeft() {
    return startCiLeft;
  }

  public void setStartCiLeft(int startCiLeft) {
    this.startCiLeft = startCiLeft;
  }

  public int getStartCiRight() {
    return startCiRight;
  }

  public void setStartCiRight(int startCiRight) {
    this.startCiRight = startCiRight;
  }

  public int getEndCiLeft() {
    return endCiLeft;
  }

  public void setEndCiLeft(int endCiLeft) {
    this.endCiLeft = endCiLeft;
  }

  public int getEndCiRight() {
    return endCiRight;
  }

  public void setEndCiRight(int endCiRight) {
    this.endCiRight = endCiRight;
  }

  public String getCaseId() {
    return caseId;
  }

  public void setCaseId(String caseId) {
    this.caseId = caseId;
  }

  public String getSetId() {
    return setId;
  }

  public void setSetId(String setId) {
    this.setId = setId;
  }

  public String getSvUuid() {
    return svUuid;
  }

  public void setSvUuid(String svUuid) {
    this.svUuid = svUuid;
  }

  public String getCaller() {
    return caller;
  }

  public void setCaller(String caller) {
    this.caller = caller;
  }

  public String getSvType() {
    return svType;
  }

  public void setSvType(String svType) {
    this.svType = svType;
  }

  public String getSvSubType() {
    return svSubType;
  }

  public void setSvSubType(String svSubType) {
    this.svSubType = svSubType;
  }

  public Map<String, Object> getInfo() {
    return info;
  }

  public void setInfo(Map<String, Object> info) {
    this.info = new TreeMap();
    this.info.putAll(info);
  }

  public int getNumHomAlt() {
    return numHomAlt;
  }

  public void setNumHomAlt(int numHomAlt) {
    this.numHomAlt = numHomAlt;
  }

  public int getNumHomRef() {
    return numHomRef;
  }

  public void setNumHomRef(int numHomRef) {
    this.numHomRef = numHomRef;
  }

  public int getNumHet() {
    return numHet;
  }

  public void setNumHet(int numHet) {
    this.numHet = numHet;
  }

  public int getNumHemiAlt() {
    return numHemiAlt;
  }

  public void setNumHemiAlt(int numHemiAlt) {
    this.numHemiAlt = numHemiAlt;
  }

  public int getNumHemiRef() {
    return numHemiRef;
  }

  public void setNumHemiRef(int numHemiRef) {
    this.numHemiRef = numHemiRef;
  }

  public Map<String, Object> getGenotype() {
    return genotype;
  }

  public void setGenotype(Map<String, Object> genotype) {
    this.genotype = new TreeMap();
    this.genotype.putAll(genotype);
  }

  @Override
  public String toString() {
    return "GenotypeRecordBuilder{"
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
}
