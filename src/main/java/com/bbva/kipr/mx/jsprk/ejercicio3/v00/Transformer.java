package com.bbva.kipr.mx.jsprk.ejercicio3.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        SparkSession spark = SparkSession.builder()
                .appName("EJERCICIO3")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data_p = spark.read().csv("local-execution/files/V_HUELLA.csv");
        Dataset<Row> data_s= spark.read().csv("local-execution/files/V_HUELLA_2.csv");
        Dataset<Row> ds_data_p = data_p.alias("Data1").join(data_s.alias("Data2"), data_p.col("input_01").equalTo(data_s.col("input_03")), "inner");
        Dataset<Row> ds_drop = ds_data_p.drop("DNI");
        Dataset<Row> dataSet3 = ds_drop.select("Data1.N_folio","Data2.code_1","Data1.input_1","Data1.input_2","Data2.input_3","Data2.input_4");

        dataSet3.show();

        //datasetsToWrite.put("targetAlias1", dataset.toDF());
        datasetsToWrite.put("targetAlias2", dataSet3.toDF());

        // dataSet3.write().option("header", "true").option("delimiter", ";").csv("local-execution/files/salida");

        return datasetsToWrite;
    }

}