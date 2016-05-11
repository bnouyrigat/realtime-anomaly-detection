package bnouyrigat.h2o.processor;

import java.util.Arrays;

public class ECGData {

    private double[] dataArray;

    public ECGData(double[] doubleArray) {
        dataArray = doubleArray;
    }

    public ECGData() {
    }

    public double[] getDataArray() {
        return dataArray;
    }

    public void setDataArray(double[] dataArray) {
        this.dataArray = dataArray;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ECGData ecgData = (ECGData) o;
        return Arrays.equals(dataArray, ecgData.dataArray);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(dataArray);
    }
}
