package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.CsvReader;
import no.kantega.mlworkshop.plotters.ScatterPlotter;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws FileNotFoundException, IllegalAccessException, InstantiationException {

        String dataPath = App.class.getClassLoader().getResource("winequality-white.csv").getPath();
        List<Wine> wines = new CsvReader<>(Wine.class).objectsFromFile(dataPath, ';');

        plotFeatures(wines);
    }

    private static void plotFeatures(List<Wine> wines){
        List<Double> xValues = new ArrayList<>();
        List<Double> yValues = new ArrayList<>();

        for(Wine wine : wines){
            xValues.add(wine.getAlcohol());
            yValues.add((double)wine.getQuality());
        }

        ScatterPlotter.Series series = new ScatterPlotter.Series("Quality/alcohol", xValues, yValues);
        new ScatterPlotter().plot(Collections.singletonList(series), null);
    }
}
