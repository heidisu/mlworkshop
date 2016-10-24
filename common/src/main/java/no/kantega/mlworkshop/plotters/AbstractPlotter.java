package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.internal.chartpart.Chart;

public abstract class AbstractPlotter<T extends Chart>{
    public void plot(){
        new SwingWrapper<>(getChart()).displayChart();
    }

    protected abstract T getChart();
}
