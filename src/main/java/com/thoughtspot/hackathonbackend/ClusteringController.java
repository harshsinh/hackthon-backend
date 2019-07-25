package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.dto.*;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

@RestController
public class ClusteringController {

    @Autowired
    ClusteringService clusteringService;

    // target dimension
    // cluster algo.
    @RequestMapping(method = RequestMethod.POST, path = "/clustering", produces = "application/json", consumes =  "application/json")
    public ClusterDefinition cluster(@RequestBody CustomDataset data){
        System.out.println(data);
        Map<String, Long> first = clusteringService.getCount(Arrays.asList("first"));
        return null;
    }

    @RequestMapping(method = RequestMethod.POST, path = "/localclustering", produces = "application/json", consumes =  "application/json")
    public String localCluster() throws IOException {
        Map<String, Long> first = clusteringService.getClustering(getDummyRequest());
        return "testData";
    }

    public CustomDataset getDummyRequest() throws IOException {
        String data = FileUtils.readFileToString(new File("/Users/harsh.sinha/workspace/thoughtspot/lineorder_csv.csv"));
        CustomDataset dataset = new CustomDataset();

        String[] rows = data.split("\n");
        dataset.values = Arrays.asList(rows).subList(3, rows.length);
        String[] headers = rows[0].split(",");
        String[] colTypes = rows[1].split(",");
        String[] datatypes = rows[2].split(",");
        dataset.columns = new ArrayList<Column>();
        for (int i = 0; i < headers.length; i++) {
            Column column = new Column();
            column.name = headers[i];
            column.colType = getColumn(colTypes[i]);
            column.dataType = getDataType(datatypes[i]);
            dataset.columns.add(column);
        }
        return dataset;
    }

    public ColumnType getColumn(String columnType) {
        switch (columnType) {
            case "ATTRIBUTE" : return ColumnType.ATTRIBUTE;
            case "MEASURE" : return ColumnType.MEASURE;
            default: return null;
        }
    }

    public DataType getDataType(String columnType) {
        switch (columnType) {
            case "VARCHAR" : return DataType.VARCHAR;
            case "INT64" : return DataType.INT64;
            default: return null;
        }
    }

}
