package at.datasciencelabs.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

class PopularPlacesElasticSearchSink implements ElasticsearchSinkFunction<Tuple5<Float, Float, Long, Boolean, Integer>> {

    private static final String POPULAR_LOCATIONS_ES_TYPE = "popular-locations";
    private static final String POPULAR_PLACE_ES_INDEX = "nyc-places";

    @Override
    public void process(Tuple5<Float, Float, Long, Boolean, Integer> ridesRecord,
                        RuntimeContext runtimeContext,
                        RequestIndexer requestIndexer) {

        Map<String, String> json = new HashMap<>();
        json.put("time", ridesRecord.f2.toString());
        json.put("location", ridesRecord.f1 + "," + ridesRecord.f0);
        json.put("isStart", ridesRecord.f3.toString());
        json.put("cnt", ridesRecord.f4.toString());

        IndexRequest indexRequest = Requests.indexRequest()
                .index(POPULAR_PLACE_ES_INDEX)
                .type(POPULAR_LOCATIONS_ES_TYPE)
                .source(json);

        requestIndexer.add(indexRequest);
    }
}
