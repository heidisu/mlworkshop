package no.kantega.mlworkshop.plotters;

import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import java.awt.*;
import java.util.List;
import java.util.Objects;

/**
 * Klasse for å tegne en kurve som består av rette linjer mellom datapunktene
 * xValues er en liste av verdiene langs x-aksen
 * ySeries er en liste av dataserier, det tegnes en kurve per dataserie og lengde av en dataserie må være lik lengden av xValues
 * colors er en liste av farger for hver dataserie, valgfritt å angi
 * seriesNames er navn/label man vil gi hver dataserie
 */
public class LinePlotter extends AbstractPlotter<XYChart>{
    private List<?> xValues;
    private List<List<Double>> ySeries;
    private List<Color> colors;
    private List<String> seriesNames;

    public LinePlotter(List<?> xValues, List<List<Double>> ySeries, List<Color> colors, List<String> seriesNames){
        this.xValues = xValues;
        this.ySeries = ySeries;
        this.colors = colors;
        this.seriesNames = seriesNames;
    }

    @Override
    protected XYChart getChart() {
        XYChart chart = new XYChartBuilder().width(800).height(600).build();

        chart.getStyler().setChartTitleVisible(false);
        if(colors != null) {
            Color[] array = new Color[colors.size()];
            chart.getStyler().setSeriesColors(colors.toArray(array));
        }
        int i = 0;
        for(List<Double> yValues : ySeries){
            chart.addSeries(seriesNames.get(i), xValues, yValues);
            i++;
        }

        return chart;
    }
}
