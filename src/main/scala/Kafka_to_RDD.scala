import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.util.Properties
import org.apache.spark.sql.functions._


object Kafka_to_RDD extends App {

  val sparkConf        = new SparkConf().setAppName("MLStreaming")
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  val sparkContext     = streamingContext.sparkContext

  val model = PipelineModel.load("src/main/outputmodel")

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG                 -> "test_group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  val inputTopicSet = Set("iris_input")
  val messages = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
  )

  val lines = messages
    .map(_.value)
    .map(_.replace("\"", "").split("|"))

  val brokersout = "localhost:9092"
  val topicout = "iris_rdd_predictition"

  val propsout: Properties = new Properties()
  propsout.put("bootstrap.servers", brokersout)

  lines.foreachRDD { rdd =>

    val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import spark.implicits._

    val data = rdd
      .toDF("value")
      .withColumn("sepal_length", $"value" (0).cast(DoubleType))
      .withColumn("sepal_width", $"value" (1).cast(DoubleType))
      .withColumn("petal_length", $"value" (2).cast(DoubleType))
      .withColumn("petal_width", $"value" (3).cast(DoubleType))
      .drop("value")

    data.show()

    if (data.count > 0) {
      val prediction = model.transform(data)

      prediction.select("sepal_length", "sepal_width", "petal_length", "petal_width", "predictedLabel")

      val assembleddata = prediction.withColumn("value",
        concat(col("sepal_length"), lit("|")
          , col("sepal_width"), lit("|")
          , col("petal_length"), lit("|")
          , col("petal_width"), lit("|")
          , col("predictedLabel")
        ))

      val selecteddata = assembleddata
        .select("value")
        .selectExpr( "CAST(value AS STRING)")
        .foreachPartition { partition =>
          val producer = new KafkaProducer(propsout, new StringSerializer, new StringSerializer)
          partition.foreach { record =>
            producer.send(new ProducerRecord(topicout, record))
          }
          producer.close()
        }

    }

    streamingContext.start()
    streamingContext.awaitTermination()

    }

  //streamingContext.start()
  //streamingContext.awaitTermination()


  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  //streamingContext.start()
  //streamingContext.awaitTermination()


}
