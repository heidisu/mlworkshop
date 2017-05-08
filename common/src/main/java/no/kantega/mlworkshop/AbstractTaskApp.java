package no.kantega.mlworkshop;

import no.kantega.mlworkshop.plotters.HistogramPlotter;
import no.kantega.mlworkshop.plotters.ScatterPlotter;
import no.kantega.mlworkshop.plotters.Series;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractTaskApp {
    private TaskSubmitter taskSubmitter;

    public AbstractTaskApp(){
        this.taskSubmitter = new TaskSubmitter();
    }

    protected void plotFeatures(Dataset<Row> rows, String xColName, String yColName) {
        List<Double> xValues = new ArrayList<>();
        List<Double> yValues = new ArrayList<>();

        rows.collectAsList().forEach(r -> {
            xValues.add(r.getAs(xColName));
            yValues.add(r.getAs(yColName));
        });

        Series series = new Series(xColName + "/" + yColName, xValues, yValues);
        new ScatterPlotter(Collections.singletonList(series), null).plot();
    }

    protected void plotHistogram(Dataset<Row> rows, String xColName, String yColName){
        Dataset<Row> groupedDataSet = rows.groupBy(xColName, yColName).count().sort(xColName, yColName);
        List<Double> possibleValues = groupedDataSet.collectAsList().stream().map(r -> r.<Number>getAs(xColName).doubleValue()).collect(Collectors.toList());

        Map<String, Map<Double, Double>> countMap = new TreeMap<>();
        List<Row> sortedRows = groupedDataSet.collectAsList();
        for(Row r : sortedRows){
            String yName = r.<Number>getAs(yColName).toString();
            double xVal = r.<Number>getAs(xColName).doubleValue();
            double count = r.<Long>getAs("count");
            Map<Double, Double> countMapAllValues = countMap.computeIfAbsent(yName, i ->
            {
                Map<Double, Double> initialized = new TreeMap<>();
                possibleValues.forEach( v -> initialized.put(v, 0.0));
                return initialized;
            });
            countMapAllValues.put(xVal, count);
        }

        new HistogramPlotter(xColName, yColName, countMap).plot();
    }

    protected void submit(List<Prediction> predictions){
        taskSubmitter.submit(predictions);
    }

    protected void createTempFile(String path) {
        try {
            InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(path);
            File file = new File(path);
            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
