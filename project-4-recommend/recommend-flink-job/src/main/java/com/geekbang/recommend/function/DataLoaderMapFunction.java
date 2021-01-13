package com.geekbang.recommend.function;

import com.geekbang.recommend.util.MysqlClient;
import com.geekbang.recommend.entity.ProductEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class DataLoaderMapFunction implements MapFunction<ProductEntity, ProductEntity> {
    @Override
    public ProductEntity map(ProductEntity productEntity) throws Exception {
        if(productEntity != null) {
            MysqlClient.putData(productEntity);
        }
        return productEntity;
    }
}

