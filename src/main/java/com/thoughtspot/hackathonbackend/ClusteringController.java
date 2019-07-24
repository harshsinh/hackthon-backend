package com.thoughtspot.hackathonbackend;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;

@RestController
public class ClusteringController {

    @Autowired
    ClusteringService clusteringService;

    @RequestMapping(method = RequestMethod.POST, path = "/clustering")
    public void cluster(){
        clusteringService.getCount(Arrays.asList("first"));
    }

}
