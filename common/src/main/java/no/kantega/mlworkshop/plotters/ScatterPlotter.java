package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;

import java.awt.*;
import java.util.List;

public class ScatterPlotter {

    public void plot(List<Series> series, Color[] colors){
        new SwingWrapper<>(getChart(series, colors)).displayChart();
    }

    private XYChart getChart(List<Series> series, Color[] colors){
        // Create Chart
        XYChart chart = new XYChartBuilder().width(800).height(600).build();

        // Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideSW);
        chart.getStyler().setMarkerSize(10);
        if(colors != null) {
            chart.getStyler().setSeriesColors(colors);
        }
        for(Series s : series){
            chart.addSeries(s.name, s.xValues, s.yValues);
        }

        return chart;
    }


    public static class Series{
        String name;
        List<Double> xValues;
        List<Double> yValues;

        public Series(String name, List<Double> xValues, List<Double> yValues) {
            this.name = name;
            this.xValues = xValues;
            this.yValues = yValues;
        }
    }
}

