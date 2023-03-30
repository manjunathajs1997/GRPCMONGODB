package io.grpc.examples.helloworld;

public class CalculateCost {
    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    private String year;

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getExpense() {
        return expense;
    }

    public void setExpense(String expense) {
        this.expense = expense;
    }

    private String state;
    private String type;
    private String length;
    private String expense;
}
