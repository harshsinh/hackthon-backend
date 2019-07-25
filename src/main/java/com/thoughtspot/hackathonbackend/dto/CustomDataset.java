package com.thoughtspot.hackathonbackend.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Data
@Getter
@Setter
public class CustomDataset {
    public List<String> values;
    public List<Column> columns;
}
