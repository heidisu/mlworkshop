package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HistogramPlotter extends AbstractPlotter<CategoryChart>{
    private Map<String, Map<Double, Double>> groupedCounts;
    private String xTitle, yTitle;


    public HistogramPlotter(String xTitle, String yTitle, Map<String, Map<Double, Double>> groupedCounts){
        this.xTitle = xTitle;
        this.yTitle = yTitle;
        this.groupedCounts = groupedCounts;
    }


    @Override
    protected CategoryChart getChart() {
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).xAxisTitle(xTitle).build();
        for(Map.Entry<String, Map<Double, Double>> entry : groupedCounts.entrySet()){
            List<Double> xValues = new ArrayList<>();
            List<Double> yValues = new ArrayList<>();
            for(Map.Entry<Double, Double> countEntry : entry.getValue().entrySet()){
                xValues.add(countEntry.getKey());
                yValues.add(countEntry.getValue());
            }
            chart.addSeries(yTitle + " " + entry.getKey(), xValues, yValues);
        }
        return chart;
    }
}
