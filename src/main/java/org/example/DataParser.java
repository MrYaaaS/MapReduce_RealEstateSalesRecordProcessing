package org.example;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.StringReader;

public class DataParser {
    private static CSVReader csvReader;
    private static StringReader strReader;
    public static final String delimiter = "\t";
    static int yearIndex = 0;
    static int stateIndex = 9;
    static int propertyTypeIndex = 11;
    static int propertyTypeIdIndex = 12;

    public Record getSingleRecord(String line) throws IOException, CsvValidationException{
        strReader = new StringReader(line);
        csvReader = new CSVReader(strReader);
        String[] values = csvReader.readNext();
        int year = -1;
        String state = "Unknown";
        String propertyType = "Unknown";
        int propertyTypeId = -100;

        if(values != null && values.length > 0){
            year = Integer.parseInt(values[0].split(delimiter)[0].substring(0,4));
        }

        if(values.length > 1 && values[1].split(delimiter).length > 2) {
            state = values[1].split(delimiter)[2].trim();
        }
//        String state = values.length > 1 ? values[1].split(delimiter)[2].trim() : "Unknown";
        if(values.length > 1 && values[1].split(delimiter).length > 4){
            propertyType = values[1].split(delimiter)[4].trim();
        }
//        propertyType = values.length > 1 ? values[1].split(delimiter)[4].trim() : "Unknown";

        if(values.length > 1 && values[1].split(delimiter).length > 5){
            propertyTypeId = Integer.parseInt(values[1].split(delimiter)[5].trim());
        }
//        int propertyTypeId = Integer.parseInt(values.length > 1 ? values[1].split(delimiter)[5].trim() : "-100");
        return new Record(state, propertyType, propertyTypeId, year);
    }

    /*
     * Get & Set methods
     */
    public CSVReader getCsvReader() {
        return csvReader;
    }
    public void setCsvReader(CSVReader csvReader) {
        DataParser.csvReader = csvReader;
    }
    public StringReader getStrReader() {
        return strReader;
    }
    public void setStrReader(StringReader strReader) {
        DataParser.strReader = strReader;
    }
}
