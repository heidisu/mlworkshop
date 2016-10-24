package no.kantega.mlworkshop.plotters;

import java.util.List;

public class Series {
    String name;
    List<?> xValues;
    List<? extends Number> yValues;

    public Series(String name, List<?> xValues, List<? extends Number> yValues) {
        this.name = name;
        this.xValues = xValues;
        this.yValues = yValues;
    }
}
