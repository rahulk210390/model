package com.cmb.generic

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StringType, StructField, DoubleType, LongType, IntegerType, DateType, DataType}

class calcChangeFlg (subsectype: String, param: String) extends UserDefinedAggregateFunction {
	def inputSchema: StructType = new StructType().add("subsectype", StringType, true).add("param", StringType, true)

			def bufferSchema: StructType = new StructType().add("value", StringType, true)
			def dataType: DataType = StringType
			def deterministic: Boolean = true
			def initialize(buffer: MutableAggregationBuffer): Unit = {
					buffer(0) = " "
			}
			def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
					{
						buffer(0) = input.getString(1)
					}
			}
			def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
					buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
			}
			def evaluate(buffer: Row): Any = {
					buffer.getString(0)
			}
}//calcChangeFlg