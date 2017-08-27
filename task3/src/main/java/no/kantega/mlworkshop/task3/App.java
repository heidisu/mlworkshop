package no.kantega.mlworkshop.task3;

import no.kantega.mlworkshop.AbstractTaskApp;
import no.kantega.mlworkshop.plotters.LinePlotter;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.PolynomialExpansion;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hello world!
 *
 */
public class App extends AbstractTaskApp
{
    private App(int taskId) {
        super(taskId);
    }

    public static void main(String[] args )
    {
        new App(3).run();
    }



    private void run(){
        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("Bike").config(conf).getOrCreate();

        // TODO 1.1 Last ned csv-filer om bysykkelturer fra https://developer.oslobysykkel.no/data og legg de i resources/

        // TODO 1.2 Les inn filene til ett datasett og undersøk hvordan datasettet ser ut. Bruk metoden readCsvFile for å lese en fil
        // For å sette sammen to datasett til ett kan du bruke dataset.union, loop eller bruk streams og map for å samle alle datasettene.
        List<String> filenames = Arrays.asList(
                "trips-2016.8.1-2016.8.31.csv",
                "trips-2016.9.1-2016.9.30.csv",
                "trips-2016.10.1-2016.10.31.csv",
                "trips-2016.11.1-2016.11.30.csv");

        Dataset<Row> trips = filenames.stream().map(name -> readCsvFile(spark, name)).reduce(Dataset::union).get();
        trips.show();
        System.out.println(trips.count());

        // TODO 1.3 Legg til nye kolonner i datasetet for år, måned, dag, time og ukedag for starttidspunktet
        // Bruk functions.unix_timestamp(trips.col(<kolonnenavn>), "yyyy-MM-dd HH:mm:ss").cast("timestamp") først for å få en timestamp fra starttidspunktet
        // Finne ukedag er litt tricky, functions.date_format(trips.col(<timestampkolonne>), "u").cast("int")
        // For å finne år, måned, dag og time finnes det nyttige funksjoner functions.year(), functions.month() etc
        // For å legge til kolonner i datasettet kan dataset.withColumn() brukes
        trips = trips.withColumn("start_time", functions.unix_timestamp(trips.col("Start time"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"));
        trips = trips.withColumn("year", functions.year(trips.col("start_time")))
                .withColumn("month", functions.month(trips.col("start_time")))
                .withColumn("day", functions.dayofmonth(trips.col("start_time")))
                .withColumn("hour", functions.hour(trips.col("start_time")))
                .withColumn("day_of_week", functions.date_format(trips.col("start_time"), "u").cast("int"));
        trips.show();


        // TODO 1.4 Gjør en groupBy-operasjon sånn at du får antall turer pr time
        // Bruk dataset.groupBy().count() med kolonnene for år, måned, dag, ukedag og time
        Dataset<Row> tripsPrHour = trips.groupBy("year", "month", "day", "day_of_week", "hour").count();
        tripsPrHour.show();

        // TODO Hvilke kolonner av de du har nå tror du har betydning for antall sykkelturer i timen eller rushtid?
        // Undersøk datasettet litt og prøv å plotte med plotTrips()
        // Hvordan er forskjellen mellom ulike ukedager og ulike måneder?
        plotTrips(tripsPrHour, "year", "month", "day", "hour", "day_of_week", "count");
        Dataset<Row> weekends = tripsPrHour.where(tripsPrHour.col("day_of_week").isin(6,7));
        Dataset<Row> weekDays = tripsPrHour.where(tripsPrHour.col("day_of_week").isin(1,2,3,4,5));
        plotTrips(weekends, "year", "month", "day", "hour", "day_of_week", "count");
        plotTrips(weekDays, "year", "month", "day", "hour", "day_of_week", "count");

        // TODO Om du velger å se på når det er rushtid for syklene må du lage en kolonne som har verdien 1 om antallet er større eller lik 1000, 0 ellers.
        // Dette kan gjøres med withColumn, functions.when(, 1).otherwise(0), og column.geq()
        // Etter at du har fått denne kolonnen kan du også bruke plotHistogram med feks time eller ukedag, og den nye kolonnen.
        Dataset<Row> categoryTrips = tripsPrHour.withColumn("over1000", functions.when(tripsPrHour.col("count").geq(1000), 1).otherwise(0));

        // TODO Tren modell som passer med problemet du valgte
        // Lage pipeline som i de tidligere oppgavene med RFormula og valgt maskinlæringsmodell
        // Se hvor bra modellen gjør det med testdata og se om det er noe med features eller valg modell som kan gjøre prediksjonene bedre


        // ********* Rushtid *************
        Dataset<Row>[] splits = categoryTrips.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        RFormula formula = new RFormula().setFormula("over1000 ~ month + day_of_week + hour");
        RandomForestClassifier rf = new RandomForestClassifier();

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{formula, rf});
        PipelineModel model = pipeline.fit(training);
        Dataset<Row> predictions = model.transform(test);

        Evaluator evaluator = new BinaryClassificationEvaluator();
        System.out.println("Random forest classifier accuracy: " + evaluator.evaluate(predictions));

        // ******* Antall turer pr time ************
        RFormula regressionFormula = new RFormula().setFormula("count ~ month + day_of_week + hour ");
        Evaluator regressionEvaluator = new RegressionEvaluator();

        // Random forest
        RandomForestRegressor rfr = new RandomForestRegressor();
        Pipeline rfrPipeline = new Pipeline().setStages(new PipelineStage[]{regressionFormula, rfr});
        PipelineModel rfrModel = rfrPipeline.fit(training);
        Dataset<Row> rfrPredictions = rfrModel.transform(test);
        System.out.println("Random forest regressor accuracy: " + regressionEvaluator.evaluate(rfrPredictions));

        // Linear regression
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(true);

        Normalizer normalizer = new Normalizer()
                .setInputCol("scaledFeatures")
                .setOutputCol("normFeatures");

        PolynomialExpansion polyExpansion = new PolynomialExpansion()
                .setInputCol("normFeatures")
                .setOutputCol("polyFeatures")
                .setDegree(15);

        LinearRegression linearRegression = new LinearRegression();
        linearRegression.setFitIntercept(true);
        linearRegression.setFeaturesCol("polyFeatures");


        Pipeline lrPipeline = new Pipeline().setStages(
                new PipelineStage[]{regressionFormula, scaler, normalizer, polyExpansion, linearRegression});
        PipelineModel lrModel = lrPipeline.fit(training);
        Dataset<Row> lrPredictions = lrModel.transform(test);
        System.out.println("Linear regression accuracy: " + regressionEvaluator.evaluate(lrPredictions));


        // TODO Er det andre data som du tror kan forbedre modellen?
    }

    private Dataset<Row> readCsvFile(SparkSession spark, String path) {
        createTempFile(path);
        return spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ",")
                .load(path);
    }

    /**
     * Metode som plotter antall turer pr time i datasett med sykkeltur,  med en kurve for hver dag
     *
     * @param trips datasettet med sykkelturer
     * @param yearCol navn på kolonne som inneholder år
     * @param monthCol navn på kolonne som inneholder måned
     * @param dayCol navn på kolonne som inneholder dag
     * @param hourCol navn kolonne som inneholder time
     * @param dayOfWeekCol navn på kolonne som inneholder ukedag
     * @param countCol navn på kolonne som inneholder antall
     */
    private void plotTrips(
            Dataset<Row> trips,
            String yearCol,
            String monthCol,
            String dayCol,
            String hourCol,
            String dayOfWeekCol,
            String countCol) {
        List<List<Double>> datasets = new ArrayList<>();
        List<Color> colors = new ArrayList<>();
        List<String> seriesNames = new ArrayList<>();
        List<Double> zeroes = Collections.nCopies(24, 0.0);
        List<Double> dataset = new ArrayList<>(zeroes);
        List<Row> rows = trips.orderBy(yearCol, monthCol, dayCol, hourCol).collectAsList();
        int prevDay = -1;
        boolean firstIteration = true;
        for (Row row : rows){
            int day = row.getInt(row.fieldIndex(dayCol));
            if(day != prevDay){
                if(!firstIteration) {
                    datasets.add(dataset);
                    dataset = new ArrayList<>(zeroes);
                }
                firstIteration = false;
                prevDay = day;
                int dayOfWeek = row.getInt(row.fieldIndex(dayOfWeekCol));
                colors.add(getColor(dayOfWeek));
                seriesNames.add(row.getInt(row.fieldIndex(yearCol)) + "-" + row.get(row.fieldIndex(monthCol)) + "-" + day);
            }
            int hour = row.getInt(row.fieldIndex(hourCol));
            int count = (int) row.getLong(row.fieldIndex(countCol));
            dataset.set(hour, (double) count);
        }
        datasets.add(dataset);
        List<Integer> labels = IntStream.rangeClosed(0, 23).boxed().collect(Collectors.toList());
        new LinePlotter(labels, datasets, colors, seriesNames).plot();
    }

    private Color getColor(int dayOfWeek){
        switch (dayOfWeek){
            case 1: return Color.yellow;
            case 2: return Color.green;
            case 3: return Color.blue;
            case 4: return Color.cyan;
            case 5: return Color.pink;
            case 6: return Color.orange;
            case 7: return Color.red;
            default: return Color.white;
        }
    }
}
