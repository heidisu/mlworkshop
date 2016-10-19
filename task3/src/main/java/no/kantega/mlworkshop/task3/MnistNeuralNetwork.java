package no.kantega.mlworkshop.task3;

import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.Updater;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MnistNeuralNetwork {
    private static Logger log = LoggerFactory.getLogger(MnistNeuralNetwork.class);

    public static void main(String[] args) throws Exception {
        final int numRows = 28;
        final int numColumns = 28;
        int outputNum = 10;
        int batchSize = 64;
        int rngSeed = 123;
        int numEpochs = 15;
        double rate = 0.0015;

        //Get the DataSetIterators:
        DataSetIterator mnistTrain = new MnistDataSetIterator(batchSize, true, rngSeed);
        DataSetIterator mnistTest = new MnistDataSetIterator(batchSize, false, rngSeed);


        log.info("Build model....");
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(rngSeed)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .iterations(1)
                .activation("relu")
                .weightInit(WeightInit.XAVIER)
                .learningRate(rate)
                .updater(Updater.NESTEROVS).momentum(0.98)
                .regularization(true).l2(rate * 0.005)
                .list()
                .layer(0, new DenseLayer.Builder()
                        .nIn(numRows * numColumns)
                        .nOut(500)
                        .build())
                .layer(1,  new DenseLayer.Builder()
                        .nIn(500)
                        .nOut(100)
                        .build())
                .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                        .activation("softmax")
                        .nIn(100)
                        .nOut(outputNum)
                        .build())
                .pretrain(false).backprop(true)
                .build();

        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(5));

        log.info("Train model....");
        for( int i=0; i<numEpochs; i++ ){
            log.info("Epoch " + i);
            model.fit(mnistTrain);
        }


        log.info("Evaluate model....");
        Evaluation eval = new Evaluation(outputNum);
        while(mnistTest.hasNext()){
            DataSet next = mnistTest.next();
            INDArray output = model.output(next.getFeatureMatrix());
            eval.eval(next.getLabels(), output);
        }

        log.info(eval.stats());
        log.info("****************Example finished********************");

    }
}
