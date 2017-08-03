package no.kantega.mlworkshop.submission;

import java.util.List;

public class TaskSubmission {
    private String name;
    private List<Prediction> predictions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Prediction> getPredictions() {
        return predictions;
    }

    public void setPredictions(List<Prediction> predictions) {
        this.predictions = predictions;
    }
}
