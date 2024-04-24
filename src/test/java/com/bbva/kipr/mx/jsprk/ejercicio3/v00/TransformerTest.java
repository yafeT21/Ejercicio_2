package com.bbva.kipr.mx.jsprk.ejercicio3.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.kipr.mx.jsprk.ejercicio3.v00.model.RowData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("CAMPO1", DataTypes.StringType, false),
                        DataTypes.createStructField("CAMPO2", DataTypes.StringType, false),
                        DataTypes.createStructField("CAMPO3", DataTypes.StringType, false),
                });
        Row firstRow = RowFactory.create("0182", "000001", "John Doe");
        Row secondRow = RowFactory.create("0182", "000002", "Mike Doe");
        Row thirdRow = RowFactory.create("0182", "000003", "Paul Doe");

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("sourceAlias1", dataset)));

        assertNotNull(datasetMap);
    }
    @Test
    void transform_Output1() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("N_folio", DataTypes.IntegerType, false),
                        DataTypes.createStructField("code_1", DataTypes.IntegerType, false),
                        DataTypes.createStructField("input_01", DataTypes.IntegerType, false),
                        DataTypes.createStructField("input_02", DataTypes.IntegerType, false),
                        DataTypes.createStructField("input_03", DataTypes.IntegerType, false),
                        DataTypes.createStructField("input_04", DataTypes.IntegerType, false),
                });
        Row firstRow = RowFactory.create(100073, 51821, 241105, 1104, 241105, 1104);
        Row secondRow = RowFactory.create(100074, 51822, 241106, 1105, 241106, 1105);
        Row thirdRow = RowFactory.create(100075, 51823, 241107, 1106, 241107, 1106);

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("targetAlias2", dataset)));

        assertNotNull(datasetMap);
    }

}