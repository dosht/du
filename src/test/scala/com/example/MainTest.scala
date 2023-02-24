package com.example

import io.findify.s3mock.S3Mock
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Builder
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import munit.FunSuite
import org.apache.spark.sql.SparkSession

class MainTest extends FunSuite {
  private def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  def assertArrayEquals[A](actual: Seq[A], expected: Seq[A]): Unit = {
    assert(actual.forall(expected.toSet))
    assert(expected.forall(actual.toSet))
  }

  private val port = 8003
  private val s3 = FunFixture[(S3Mock, AmazonS3)] (
    setup = { test =>
      val s3 = S3Mock(port = port, dir = s"/tmp/s3/${test.name}")
      s3.start
      val endpoint = new EndpointConfiguration(s"http://localhost:$port", "us-west-2")
      val client: AmazonS3 = AmazonS3ClientBuilder
        .standard
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(endpoint)
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build

      client.createBucket("test")
      client.putObject("test", "test.csv", csvContent)
      client.putObject("test", "test.tsv", tsvContent)
      (s3, client)
    },
    teardown = { case (s3, client) =>
      client.deleteBucket("test")
      s3.shutdown
      client.shutdown()
    }
  )

  private val csvContent =
    """
      |X,Y
      |1,2
      |1,3
      |1,3
      |""".stripMargin

  private val tsvContent =
    """
      |A  B
      |2  4
      |2  4
      |2  4
      |""".stripMargin

  private val spark = FunFixture[SparkSession] (
    setup = { test =>
      setEnv("AWS_ACCESS_KEY_ID", "dummy")
      setEnv("AWS_SECRET_ACCESS_KEY", "dummy")
      SparkSession.builder.appName(test.name).master("local[*]").getOrCreate()
    }, teardown = _.stop()
  )

  FunFixture.map2(s3, spark).test("test csv files") { case ((_, s3Client), spark) =>
    val x: Int = spark.sparkContext.makeRDD(List(1, 2, 3, 4)).reduce(_ + _)
    assertEquals(x, 10)
    val expected = List("X,Y", "1,2", "1,3", "1,3")
    assertArrayEquals(spark.sparkContext.textFile(s"s3a://localhost:$port/test/*.csv").collect, expected)
  }

  FunFixture.map2(s3, spark).test("test tsv files") { case ((_, s3Client), spark) =>
    val x: Int = spark.sparkContext.makeRDD(List(1, 2, 3, 4)).reduce(_ + _)
    assertEquals(x, 10)
    val expected = List("A\tB", "2\t4", "2\t4", "2\t4")
    assertArrayEquals(spark.sparkContext.textFile(s"s3a://localhost:$port/test/*.tsv").collect, expected)
  }

  spark.test("split tabs and commas") { spark =>
    val lines = List("X,Y", "1,2", "1,3", "1,3", "A\tB", "2\t4", "2\t4", "2\t4")
    val rdd = spark.sparkContext.parallelize(lines)
    val expected = Array(("X","Y"), ("1","2"), ("1","3"), ("1","3"), ("A","B"), ("2","4"), ("2","4"), ("2","4"))
    assertArrayEquals(Main.splitLines(rdd).collect, expected)
  }

  spark.test("filter numbers only") { spark =>
    val input = Array(("X","Y"), ("1","2"), ("1","3"), ("1","3"), ("A","B"), ("2","4"), ("2","4"), ("2","4"))
    val inputRDD = spark.sparkContext.parallelize(input)
    val expected = Array(("1","2"), ("1","3"), ("1","3"), ("2","4"), ("2","4"), ("2","4"))
    assertArrayEquals(Main.filterNumbers(inputRDD).collect, expected)
  }

  spark.test("filter odd counts") { spark =>
    val input = Array(("1","2"), ("1","3"), ("1","3"), ("2","4"), ("2","4"), ("2","4"))
    val inputRDD = spark.sparkContext.parallelize(input)
    val expected = Array(("2","4"), ("1","2"))
    assertArrayEquals(Main.filterOddNumbers(inputRDD).collect, expected)
  }
}
