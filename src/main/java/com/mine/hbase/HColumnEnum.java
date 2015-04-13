package com.mine.hbase;



/**
 * HBase table columns for the 'srv' column family
 */
public enum HColumnEnum {
  SRV_COL_RECORDTYPE ("recordtype".getBytes()),
  SRV_COL_CUSTOMERID ("customerid".getBytes()),
  SRV_COL_NAME ("name".getBytes()),
  SRV_COL_ADDRESS ("address".getBytes()),
  SRV_COL_PHONE ("phone".getBytes()),
  SRV_COL_ORDERNUMBER ("ordernumber".getBytes()),
  SRV_COL_SALESDATE ("salesdate".getBytes()),
  SRV_COL_LOCATIONNUMBER ("locationnumber".getBytes()),
  SRV_COL_SHIPTOLINE1 ("shiptoline1".getBytes()),
  SRV_COL_SHIPTOLINE2 ("shiptoline2".getBytes()),
  SRV_COL_SHIPTOCITY ("shiptocity".getBytes()),
  SRV_COL_SHIPTOSTATE ("shiptostate".getBytes()),
  SRV_COL_SHIPTOZIP ("shiptozip".getBytes()),
  SRV_COL_LINEITEMNUMBER("lineitemnumber".getBytes()),
  SRV_COL_ITEMNUMBER ("itemnumber".getBytes()),
  SRV_COL_QUANTITY ("quantity".getBytes()),
  SRV_COL_PRICE ("price".getBytes());
 
  private final byte[] columnName;
  
  HColumnEnum (byte[] column) {
    this.columnName = column;
  }

  public byte[] getColumnName() {
    return this.columnName;
  }
}
