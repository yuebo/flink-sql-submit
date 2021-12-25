package com.eappcat.flink.reporter;

import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class JobReport {
    private String name;
    private List<Row> rows=new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }
}
