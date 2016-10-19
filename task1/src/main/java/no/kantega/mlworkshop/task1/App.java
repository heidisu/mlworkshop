package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.ModelSubmitter;
import no.kantega.mlworkshop.Prediction;
import no.kantega.mlworkshop.plotters.ScatterPlotter;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {
    private ModelSubmitter modelSubmitter;

    public static void main(String[] args) throws FileNotFoundException, IllegalAccessException, InstantiationException {
        App app = new App();
        app.run();
    }

    public App(){
        modelSubmitter = new ModelSubmitter();
    }

    private void run() {
        String dataPath = this.getClass().getClassLoader().getResource("student.csv").getPath();

        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("ML student").config(conf).getOrCreate();

        Dataset<Row> students = readFile(spark, dataPath);
        students.show();
        Dataset<Row> transformed = transformToFeatures(students);
        transformed.show();

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(6)
                .fit(transformed);

        Dataset<Row>[] split = transformed.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingSet = split[0];
        Dataset<Row> testSet = split[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures");

        // Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{featureIndexer, rf});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingSet);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testSet);
        predictions.show();

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Area under ROC: " + accuracy);
        System.out.println("Test Error : " + (1 - accuracy));

        submitTask(spark, model);
    }

    private void submitTask(SparkSession spark, Model model){
        String mathStudentPath = App.class.getClassLoader().getResource("student-test.csv").getPath();
        Dataset<Row> mathStudents = readFile(spark, mathStudentPath);
        Dataset<Row> mathTransformed = transformToFeatures(mathStudents);
        Dataset<Row> predictionSet = model.transform(mathTransformed);
        predictionSet.select("id", "prediction").show();
        List<Prediction> predicitons = predictionSet.collectAsList().stream()
                .map(row -> new Prediction(row.<Integer>getAs("id").toString(), row.<Double>getAs("prediction")))
                .collect(Collectors.toList());
        modelSubmitter.submit(predicitons);

    }

    private Dataset<Row> transformToFeatures(Dataset<Row> data) {
        RFormula formula = new RFormula().setFormula("pass ~ . - id");
        return formula.fit(data).transform(data);
    }

    private Dataset<Row> readFile(SparkSession spark, String path) {
        return spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(path);
    }

    private static void plotFeatures(Dataset<Row> rows) {
        List<Double> xValues = new ArrayList<>();
        List<Double> yValues = new ArrayList<>();

        rows.collectAsList().forEach(r -> {
            xValues.add(r.getAs(0));
            yValues.add(r.getAs(1));
        });

        ScatterPlotter.Series series = new ScatterPlotter.Series(rows.columns()[0] + "/" + rows.columns()[1], xValues, yValues);
        new ScatterPlotter().plot(Collections.singletonList(series), null);
    }
}
