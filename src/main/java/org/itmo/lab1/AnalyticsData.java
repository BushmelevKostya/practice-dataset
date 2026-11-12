package org.itmo.lab1;

public interface AnalyticsData {

    DataSet getTopFiveOrder(DataSet dataSet);

    DataSet getClientOrder(DataSet dataSet);

    DataSet getTotalCost(DataSet dataSet);

    DataSet getMeanCost(DataSet dataSet);
}
