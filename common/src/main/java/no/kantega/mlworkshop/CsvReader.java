package no.kantega.mlworkshop;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.bean.ColumnPositionMappingStrategy;
import au.com.bytecode.opencsv.bean.CsvToBean;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

public class CsvReader<T extends DataObject> {
    private Class<T> typeClass;

    public CsvReader(Class<T> typeClass){
        this.typeClass = typeClass;
    }

    public List<T> objectsFromFile(String filePath, char separator) throws FileNotFoundException, IllegalAccessException, InstantiationException {
        CSVReader reader = new CSVReader(new FileReader(filePath), separator, '\"', 1);
        ColumnPositionMappingStrategy<T> strategy = new ColumnPositionMappingStrategy<>();
        strategy.setType(typeClass);
        strategy.setColumnMapping(typeClass.newInstance().columns());

        CsvToBean<T> csv = new CsvToBean<>();
        return csv.parse(strategy, reader);
    }
}
