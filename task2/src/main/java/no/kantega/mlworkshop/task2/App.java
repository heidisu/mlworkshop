package no.kantega.mlworkshop.task2;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.SnapshotParameters;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.TextField;
import javafx.scene.image.PixelReader;
import javafx.scene.image.PixelWriter;
import javafx.scene.image.WritableImage;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.transform.Transform;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class App extends Application
{
    private static SparkApp sparkApp;
    private static final int IMAGE_FACTOR = 8;
    private static final int SIDE_LENGTH= SparkApp.IMAGE_SIDE * IMAGE_FACTOR;
    private static double SCALE_FACTOR = 1.0/IMAGE_FACTOR;

    public static void main(String[] args) throws IOException {
        sparkApp = new SparkApp(2);
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        Canvas canvas = new Canvas(SIDE_LENGTH, SIDE_LENGTH);
        final GraphicsContext graphicsContext = canvas.getGraphicsContext2D();
        initDraw(graphicsContext);
        canvas.addEventHandler(MouseEvent.MOUSE_PRESSED,
                event -> {
                    graphicsContext.beginPath();
                    graphicsContext.moveTo(event.getX(), event.getY());
                    graphicsContext.stroke();
                });

        canvas.addEventHandler(MouseEvent.MOUSE_DRAGGED,
                event -> {
                    graphicsContext.lineTo(event.getX(), event.getY());
                    graphicsContext.stroke();
                });

        Label predictionLabel = new Label();
        predictionLabel.setId("predictionLabel");
        Button predictionButton = new Button("Gjett!");
        predictionButton.setOnAction((ActionEvent event) -> {
            predictionLabel.setText("");
            WritableImage img = new WritableImage(SparkApp.IMAGE_SIDE, SparkApp.IMAGE_SIDE);
            GraphicsContext context = canvas.getGraphicsContext2D();
            SnapshotParameters parameters = new SnapshotParameters();
            parameters.setTransform(Transform.scale(SCALE_FACTOR, SCALE_FACTOR));
            canvas.snapshot(parameters, img);
            PixelWriter pixelWriter = context.getPixelWriter();
            pixelWriter.getPixelFormat();
            PixelReader pixelReader = img.getPixelReader();
            List<Double> numbers = new ArrayList<>();
            for(int y = 0; y < SparkApp.IMAGE_SIDE; y++){
                for(int x = 0; x < SparkApp.IMAGE_SIDE; x++){
                    Color color = pixelReader.getColor(x, y);
                    if (color.getBlue() == 1f && color.getRed() == 0f && color.getGreen() == 0f){
                        System.out.print("1 ");
                        numbers.add(1.0);
                    }
                    else{
                        System.out.print("0 ");
                        numbers.add(0.0);
                    }
                    if(x == SparkApp.IMAGE_SIDE - 1){
                        System.out.println();
                    }
                }
            }

            int number =  sparkApp.predict(Collections.singletonList(numbers)).get(0).intValue();
            predictionLabel.setText("Tallet er: " + number);
        });

        TextField numberField = new TextField();

        Button labelButton = new Button("Label");
        labelButton.setOnAction( e -> {
            int number = Integer.parseInt(numberField.getCharacters().toString());
            addTrainingSample(canvas, number);
            clear(canvas);
        });

        Button clearButton = new Button("Tøm");
        clearButton.setOnAction(
                event -> {
                    clear(canvas);
                    predictionLabel.setText("");
                }
        );

        Label trainingLabel = new Label();
        Button retrainButton = new Button("Retrain");
        retrainButton.setOnAction(
                event -> {
                    trainingLabel.setText("");
                    String trainingResult = sparkApp.trainModel();
                    trainingLabel.setText(trainingResult);
                }
        );

        Button submitButton = new Button("Submit");
        submitButton.setOnAction( event -> sparkApp.evaluateModel());

        VBox vbox = new VBox();
        vbox.setSpacing(5);
        vbox.getChildren().add(canvas);
        HBox predictionBox = new HBox();
        predictionBox.setSpacing(5);
        predictionBox.getChildren().add(predictionButton);
        predictionBox.getChildren().add(predictionLabel);
        vbox.getChildren().add(predictionBox);
        vbox.getChildren().add(clearButton);
        Separator separator = new Separator();
        separator.setStyle("-fx-padding: 20 0 10 0;");
        vbox.getChildren().add(separator);
        HBox hbox = new HBox();
        hbox.setSpacing(5);
        hbox.getChildren().add(numberField);
        hbox.getChildren().add(labelButton);
        vbox.getChildren().add(hbox);
        HBox trainingBox = new HBox();
        trainingBox.setSpacing(5);
        trainingBox.getChildren().add(retrainButton);
        trainingBox.getChildren().add(trainingLabel);
        vbox.getChildren().add(trainingBox);
        vbox.getChildren().add(submitButton);
        Scene scene = new Scene(vbox, SIDE_LENGTH, 400);
        primaryStage.setTitle("Tall");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    private void initDraw(GraphicsContext gc) {
        double canvasWidth = gc.getCanvas().getWidth();
        double canvasHeight = gc.getCanvas().getHeight();

        gc.setFill(Color.WHITE);
        gc.fillRect(0,0,canvasWidth, canvasHeight);
        gc.strokeRect(
                0,              //x of the upper left corner
                0,              //y of the upper left corner
                canvasWidth,    //width of the rectangle
                canvasHeight);  //height of the rectangle

        gc.setStroke(Color.BLUE);
        gc.setLineWidth(10);

    }

    private void clear(Canvas canvas){
        GraphicsContext gc = canvas.getGraphicsContext2D();
        double canvasWidth = gc.getCanvas().getWidth();
        double canvasHeight = gc.getCanvas().getHeight();
        gc.setFill(Color.WHITE);
        gc.fillRect(0,0,canvasWidth, canvasHeight);
    }

    private void addTrainingSample(Canvas canvas, int number){
        WritableImage img = new WritableImage(SparkApp.IMAGE_SIDE, SparkApp.IMAGE_SIDE);
        GraphicsContext context = canvas.getGraphicsContext2D();
        SnapshotParameters parameters = new SnapshotParameters();
        parameters.setTransform(Transform.scale(SCALE_FACTOR, SCALE_FACTOR));
        canvas.snapshot(parameters, img);
        PixelWriter pixelWriter = context.getPixelWriter();
        pixelWriter.getPixelFormat();
        PixelReader pixelReader = img.getPixelReader();
        StringBuilder stringBuffer = new StringBuilder();
        for(int y = 0; y < SparkApp.IMAGE_SIDE; y++){
            for(int x = 0; x < SparkApp.IMAGE_SIDE; x++){
                Color color = pixelReader.getColor(x, y);
                if (color.getBlue() == 1f && color.getRed() == 0f && color.getGreen() == 0f){
                    stringBuffer.append("1; ");
                }
                else{
                    stringBuffer.append("0; ");
                }
            }
        }
        stringBuffer.append(number);
        stringBuffer.append("\n");
        try {
            sparkApp.addTrainingSample(stringBuffer.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
