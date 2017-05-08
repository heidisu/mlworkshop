package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import java.awt.*;
import java.util.List;

public class LinePlotter extends AbstractPlotter<XYChart>{
    private List<?> xValues;
    private List<List<Double>> ySeries;
    private List<Color> colors;

    public LinePlotter(List<?> xValues, List<List<Double>> ySeries, List<Color> colors){
        this.xValues = xValues;
        this.ySeries = ySeries;
        this.colors = colors;
    }

    @Override
    protected XYChart getChart() {
        XYChart chart = new XYChartBuilder().width(800).height(600).build();

        //chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);
        chart.getStyler().setChartTitleVisible(false);
        if(colors != null) {
            chart.getStyler().setSeriesColors((Color[]) colors.toArray());
        }
        int i = 1;
        for(List<Double> yValues : ySeries){
            chart.addSeries("week " + i, xValues, yValues);
            i++;
        }

        return chart;
    }
}
