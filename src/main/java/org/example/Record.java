package org.example;

public class Record {
    String state;
    String propertyType;
    int propertyTypeId;
    int year;

    public Record(String state, String propertyType, int propertyTypeId, int year) {
        this.state = state;
        this.propertyType = propertyType;
        this.propertyTypeId = propertyTypeId;
        this.year = year;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPropertyType() {
        return propertyType;
    }

    public void setPropertyType(String propertyType) {
        this.propertyType = propertyType;
    }

    public int getPropertyTypeId() {
        return propertyTypeId;
    }

    public void setPropertyTypeId(int propertyTypeId) {
        this.propertyTypeId = propertyTypeId;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
}
