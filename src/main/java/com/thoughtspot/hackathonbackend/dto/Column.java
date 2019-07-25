package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class Column implements Serializable {
    String name;
    ColumnType colType;
    DataType dataType;
}
