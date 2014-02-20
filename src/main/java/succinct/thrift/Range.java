/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package succinct.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Range implements org.apache.thrift.TBase<Range, Range._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Range");

  private static final org.apache.thrift.protocol.TField START_INDEX_FIELD_DESC = new org.apache.thrift.protocol.TField("startIndex", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField END_INDEX_FIELD_DESC = new org.apache.thrift.protocol.TField("endIndex", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RangeStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RangeTupleSchemeFactory());
  }

  public long startIndex; // required
  public long endIndex; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    START_INDEX((short)1, "startIndex"),
    END_INDEX((short)2, "endIndex");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // START_INDEX
          return START_INDEX;
        case 2: // END_INDEX
          return END_INDEX;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __STARTINDEX_ISSET_ID = 0;
  private static final int __ENDINDEX_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_INDEX, new org.apache.thrift.meta_data.FieldMetaData("startIndex", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.END_INDEX, new org.apache.thrift.meta_data.FieldMetaData("endIndex", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Range.class, metaDataMap);
  }

  public Range() {
  }

  public Range(
    long startIndex,
    long endIndex)
  {
    this();
    this.startIndex = startIndex;
    setStartIndexIsSet(true);
    this.endIndex = endIndex;
    setEndIndexIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Range(Range other) {
    __isset_bitfield = other.__isset_bitfield;
    this.startIndex = other.startIndex;
    this.endIndex = other.endIndex;
  }

  public Range deepCopy() {
    return new Range(this);
  }

  @Override
  public void clear() {
    setStartIndexIsSet(false);
    this.startIndex = 0;
    setEndIndexIsSet(false);
    this.endIndex = 0;
  }

  public long getStartIndex() {
    return this.startIndex;
  }

  public Range setStartIndex(long startIndex) {
    this.startIndex = startIndex;
    setStartIndexIsSet(true);
    return this;
  }

  public void unsetStartIndex() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTINDEX_ISSET_ID);
  }

  /** Returns true if field startIndex is set (has been assigned a value) and false otherwise */
  public boolean isSetStartIndex() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTINDEX_ISSET_ID);
  }

  public void setStartIndexIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTINDEX_ISSET_ID, value);
  }

  public long getEndIndex() {
    return this.endIndex;
  }

  public Range setEndIndex(long endIndex) {
    this.endIndex = endIndex;
    setEndIndexIsSet(true);
    return this;
  }

  public void unsetEndIndex() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ENDINDEX_ISSET_ID);
  }

  /** Returns true if field endIndex is set (has been assigned a value) and false otherwise */
  public boolean isSetEndIndex() {
    return EncodingUtils.testBit(__isset_bitfield, __ENDINDEX_ISSET_ID);
  }

  public void setEndIndexIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ENDINDEX_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_INDEX:
      if (value == null) {
        unsetStartIndex();
      } else {
        setStartIndex((Long)value);
      }
      break;

    case END_INDEX:
      if (value == null) {
        unsetEndIndex();
      } else {
        setEndIndex((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_INDEX:
      return Long.valueOf(getStartIndex());

    case END_INDEX:
      return Long.valueOf(getEndIndex());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_INDEX:
      return isSetStartIndex();
    case END_INDEX:
      return isSetEndIndex();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Range)
      return this.equals((Range)that);
    return false;
  }

  public boolean equals(Range that) {
    if (that == null)
      return false;

    boolean this_present_startIndex = true;
    boolean that_present_startIndex = true;
    if (this_present_startIndex || that_present_startIndex) {
      if (!(this_present_startIndex && that_present_startIndex))
        return false;
      if (this.startIndex != that.startIndex)
        return false;
    }

    boolean this_present_endIndex = true;
    boolean that_present_endIndex = true;
    if (this_present_endIndex || that_present_endIndex) {
      if (!(this_present_endIndex && that_present_endIndex))
        return false;
      if (this.endIndex != that.endIndex)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Range other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Range typedOther = (Range)other;

    lastComparison = Boolean.valueOf(isSetStartIndex()).compareTo(typedOther.isSetStartIndex());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartIndex()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startIndex, typedOther.startIndex);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEndIndex()).compareTo(typedOther.isSetEndIndex());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEndIndex()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.endIndex, typedOther.endIndex);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Range(");
    boolean first = true;

    sb.append("startIndex:");
    sb.append(this.startIndex);
    first = false;
    if (!first) sb.append(", ");
    sb.append("endIndex:");
    sb.append(this.endIndex);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'startIndex' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'endIndex' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RangeStandardSchemeFactory implements SchemeFactory {
    public RangeStandardScheme getScheme() {
      return new RangeStandardScheme();
    }
  }

  private static class RangeStandardScheme extends StandardScheme<Range> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Range struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // START_INDEX
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.startIndex = iprot.readI64();
              struct.setStartIndexIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // END_INDEX
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.endIndex = iprot.readI64();
              struct.setEndIndexIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetStartIndex()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'startIndex' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetEndIndex()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'endIndex' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Range struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(START_INDEX_FIELD_DESC);
      oprot.writeI64(struct.startIndex);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(END_INDEX_FIELD_DESC);
      oprot.writeI64(struct.endIndex);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RangeTupleSchemeFactory implements SchemeFactory {
    public RangeTupleScheme getScheme() {
      return new RangeTupleScheme();
    }
  }

  private static class RangeTupleScheme extends TupleScheme<Range> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Range struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.startIndex);
      oprot.writeI64(struct.endIndex);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Range struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.startIndex = iprot.readI64();
      struct.setStartIndexIsSet(true);
      struct.endIndex = iprot.readI64();
      struct.setEndIndexIsSet(true);
    }
  }

}
