package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.DataObject;

public class Wine implements DataObject{
    private double fixedAcidity;
    private double volatileAcidity;
    private double citricAcid;
    private double residualSugar;
    private double chlorides;
    private double freeSulfurDioxide;
    private double totalSulfurDioxide;
    private double density;
    private double ph;
    private double sulphates;
    private double alcohol;
    private int quality; // between 0 and 10

    public Wine(){

    }

    public double getFixedAcidity() {
        return fixedAcidity;
    }

    public double getVolatileAcidity() {
        return volatileAcidity;
    }

    public double getCitricAcid() {
        return citricAcid;
    }

    public double getResidualSugar() {
        return residualSugar;
    }

    public double getChlorides() {
        return chlorides;
    }

    public double getFreeSulfurDioxide() {
        return freeSulfurDioxide;
    }

    public double getTotalSulfurDioxide() {
        return totalSulfurDioxide;
    }

    public double getDensity() {
        return density;
    }

    public double getPh() {
        return ph;
    }

    public double getSulphates() {
        return sulphates;
    }

    public double getAlcohol() {
        return alcohol;
    }

    public int getQuality() {
        return quality;
    }

    public void setFixedAcidity(double fixedAcidity) {
        this.fixedAcidity = fixedAcidity;
    }

    public void setVolatileAcidity(double volatileAcidity) {
        this.volatileAcidity = volatileAcidity;
    }

    public void setCitricAcid(double citricAcid) {
        this.citricAcid = citricAcid;
    }

    public void setResidualSugar(double residualSugar) {
        this.residualSugar = residualSugar;
    }

    public void setChlorides(double chlorides) {
        this.chlorides = chlorides;
    }

    public void setFreeSulfurDioxide(double freeSulfurDioxide) {
        this.freeSulfurDioxide = freeSulfurDioxide;
    }

    public void setTotalSulfurDioxide(double totalSulfurDioxide) {
        this.totalSulfurDioxide = totalSulfurDioxide;
    }

    public void setDensity(double density) {
        this.density = density;
    }

    public void setPh(double ph) {
        this.ph = ph;
    }

    public void setSulphates(double sulphates) {
        this.sulphates = sulphates;
    }

    public void setAlcohol(double alcohol) {
        this.alcohol = alcohol;
    }

    public void setQuality(int quality) {
        this.quality = quality;
    }

    @Override
    public String[] columns() {
        return new String[]{
                "fixedAcidity",
                "volatileAcidity",
                "citricAcid",
                "residualSugar",
                "chlorides",
                "freeSulfurDioxide",
                "totalSulfurDioxide",
                "density",
                "ph",
                "sulphates",
                "alcohol",
                "quality"};
    }
}
