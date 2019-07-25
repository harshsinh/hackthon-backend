package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Data
@Getter
@Setter
public class CustomDataset implements Serializable{
    List<List<String>> values;
    List<Column> columns;
}
