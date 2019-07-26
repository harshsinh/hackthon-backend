package com.thoughtspot.hackathonbackend;

import com.thoughtspot.hackathonbackend.dto.*;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
public class ClusteringController {

    @Autowired
    ClusteringService clusteringService;

    // target dimension
    // cluster algo.
    @RequestMapping(method = RequestMethod.POST, path = "/clustering", produces = "application/json", consumes =  "application/json")
    public ClusterDefinition cluster(@RequestBody ClusteringInput input) throws IOException {
        ClusterDefinition cd = clusteringService.getClustering(input);
        System.out.println("Cluster Definition " + cd.toString());
        return clusteringService.getClustering(input);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/abcd", produces = "application/json", consumes =  "application/json")
    public ClusterDefinition cluster2(@RequestBody CustomDataset data) {
        System.out.println(data);

        List<Tuple> tuples = new ArrayList<>();
        int i = 0;
        for (String str : TempDS.values) {
            String[] split = str.split(";");
            Integer cluster = (int) Math.round(Double.parseDouble(split[0]));
            String[] coordinateSplit = split[1].split(",");
            Double x = Double.parseDouble(coordinateSplit[0]) / 1000000;
            Double y = Double.parseDouble(coordinateSplit[1]) / 1000000;
            Double z = Double.parseDouble(coordinateSplit[2]) / 1000000;

            tuples.add(Tuple.builder()
                    .clusterId(cluster)
                    .coordinates(Arrays.asList(x, y, z))
                    .originalValues(Arrays.asList("x" + i, "y" + i, "z" + i))
                    .build());
        }

        ClusterDefinition cf = ClusterDefinition.builder()
                .numberOfCluster(5)
                .centroid(tuples)
                .datapoints(tuples)
                .build();
        return cf;
    }

    @RequestMapping(method = RequestMethod.POST, path = "/localclustering", produces = "application/json", consumes =  "application/json")
    public String localCluster() throws IOException {
        //Map<String, Long> first = clusteringService.getClustering(getDummyRequest());
        return "testData";
    }

    public ClusteringInput getDummyRequest() throws IOException {
        String data = FileUtils.readFileToString(new File("/Users/vikas.singh/Downloads/lineorder_csv.csv"));
        ClusteringInput clusteringInput = new ClusteringInput();
        CustomDataset dataset = new CustomDataset();
        String[] rows = data.split("\n");
        List<String> rowsString = Arrays.asList(rows).subList(3, rows.length);
        dataset.setValues(new ArrayList<>());
        rowsString.forEach(x -> dataset.getValues().add(Arrays.asList(x.split(","))));
        String[] headers = rows[0].split(",");
        String[] colTypes = rows[1].split(",");
        String[] datatypes = rows[2].split(",");
        dataset.setColumns(new ArrayList<>());
        for (int i = 0; i < headers.length; i++) {
            Column column = new Column();
            column.setName(headers[i]);
            column.setColType(getColumn(colTypes[i]));
            column.setDataType(getDataType(datatypes[i]));
            dataset.getColumns().add(column);
        }
        clusteringInput.setData(dataset);
        return clusteringInput;
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
