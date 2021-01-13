package com.geekbang.recommend.function;

import com.geekbang.recommend.util.MysqlClient;
import com.geekbang.recommend.entity.RatingEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class DataLoaderMapFunction2 implements MapFunction<RatingEntity, RatingEntity> {
    @Override
    public RatingEntity map(RatingEntity ratingEntity) throws Exception {
        System.out.println(ratingEntity);
        if(ratingEntity != null) {
            MysqlClient.putData(ratingEntity);
        }
        return ratingEntity;
    }
}