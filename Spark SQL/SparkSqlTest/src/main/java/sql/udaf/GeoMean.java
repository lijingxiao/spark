package sql.udaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Created by lijingxiao on 2018/10/15.
 * 求几何平均数
 */
public class GeoMean extends UserDefinedAggregateFunction {
    //输入数据的类型
    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("value", DataTypes.DoubleType, true)));
    }

    //产生中间结果的数据类型
    @Override
    public StructType bufferSchema() {
        //相乘之后返回的积
        //参与运算数字的个数
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("product", DataTypes.DoubleType, true),
                DataTypes.createStructField("counts", DataTypes.LongType, true)));
    }

    //返回结果类型
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    //确保一致性 一般用true
    @Override
    public boolean deterministic() {
        return true;
    }

    //指定初始值
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        //相乘的初始值
        buffer.update(0, 1.0);
        //参与运算数字的个数的初始值
        buffer.update(1, 0L);
    }

    //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //每有一个数字参与运算就进行相乘（包含中间结果）
        buffer.update(0, buffer.getDouble(0) * input.getDouble(0));
        //参与运算数据的个数也有更新
        buffer.update(1, buffer.getLong(1) + 1L);
    }

    //全局聚合
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //每个分区计算的结果进行相乘
        buffer1.update(0, buffer1.getDouble(0) * buffer2.getDouble(0));
        //每个分区参与预算的中间结果进行相加
        buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
    }

    //计算最终的结果
    @Override
    public Object evaluate(Row buffer) {
        return Math.pow(buffer.getDouble(0), 1.0 / buffer.getLong(1));
    }
}
