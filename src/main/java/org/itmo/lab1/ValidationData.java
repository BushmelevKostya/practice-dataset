package org.itmo.lab1;

public interface ValidationData {

    DataSet readCsv(String filePath);

    DataSet getValidDataSet(DataSet dataSet);
}
