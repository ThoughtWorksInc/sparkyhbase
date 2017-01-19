package com.thoughtworks.sparkyhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Functions {
    static public Put makePut(Row row, String keyColumn, String[] columns, String family) {
        byte[] key = Bytes.toBytes(row.getLong(row.fieldIndex(keyColumn)));
        Put put = new Put(key);
        for (String col : columns) {
            if (col.equals(keyColumn)) continue;
            int index = row.fieldIndex(col);
            DataType t = row.schema().fields()[index].dataType();
            byte[] value;
            if (t instanceof StringType) {
                value = Bytes.toBytes(row.getString(index));
            } else if (t instanceof DoubleType) {
                value = Bytes.toBytes(row.getDouble(index));
            } else if (t instanceof LongType) {
                value = Bytes.toBytes(row.getLong(index));
            } else {
                throw new IllegalArgumentException("not implemented checking this type, soz" + t.toString());
            }
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), value);
        }
        return put;
    }

    static public Iterator<Cell> makeCells(Put put) throws IOException {
        CellScanner scanner = put.cellScanner();
        List<Cell> cells = new ArrayList<>();
        while (scanner.advance()) {
            cells.add(scanner.current());
        }
        return cells.iterator();
    }
}
