package no.kantega.mlworkshop.submission;

import java.util.List;

public class PredictionData {
    private String id;
    private List<Double> data;

    public PredictionData(String id, List<Double> data) {
        this.id = id;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public List<Double> getData() {
        return data;
    }
}
