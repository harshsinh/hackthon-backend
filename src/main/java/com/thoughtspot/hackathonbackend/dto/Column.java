package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

@Data
public class Column {
    public String name;
    public ColumnType colType;
    public DataType dataType;
}
