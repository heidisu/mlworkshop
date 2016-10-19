package no.kantega.mlworkshop.task2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Trip {
    private String fromStation;
    private String toStation;
    private LocalDateTime fromTime;
    private LocalDateTime toTime;
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");

    public Trip() {
    }

    public String getFromStation() {
        return fromStation;
    }

    public void setFromStation(String fromStation) {
        this.fromStation = fromStation;
    }

    public String getToStation() {
        return toStation;
    }

    public void setToStation(String toStation) {
        this.toStation = toStation;
    }

    public LocalDateTime getFromTime() {
        return fromTime;
    }

    public void setFromTime(LocalDateTime fromTime) {
        this.fromTime = fromTime;
    }

    public LocalDateTime getToTime() {
        return toTime;
    }

    public void setToTime(LocalDateTime toTime) {
        this.toTime = toTime;
    }

    public void setFromTimeString(String fromTimeString) {
        setFromTime(LocalDateTime.parse(fromTimeString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")));
    }

    public void setToTimeString(String toTimeString) {
        setToTime(LocalDateTime.parse(toTimeString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")));
    }

    public String[] columns() {
        return new String[]{"fromStation","fromTimeString","toStation","toTimeString"};
    }
}