package org.itmo;

public interface ValidationData {

    DataSet readCsv(String filePath);

    DataSet getValidDataSet(DataSet dataSet);
}
