package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;

import java.awt.*;
import java.util.List;

public class ScatterPlotter extends AbstractPlotter<XYChart>{
    private List<Series> series;
    private List<Color> colors;

    public ScatterPlotter(List<Series> series, List<Color> colors){
        this.series = series;
        this.colors = colors;
    }

    protected XYChart getChart(){
        // Create Chart
        XYChart chart = new XYChartBuilder().width(800).height(600).build();

        // Customize Chart
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        chart.getStyler().setChartTitleVisible(false);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setMarkerSize(10);
        if(colors != null) {
            chart.getStyler().setSeriesColors((Color[]) colors.toArray());
        }
        for(Series s : series){
            chart.addSeries(s.name, s.xValues, s.yValues);
        }

        return chart;
    }
}

