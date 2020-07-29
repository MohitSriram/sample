import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.graphframes._


object Task4 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // spark configuration setting master to local
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task4")
    val sc = new SparkContext(conf)
    //create or get sparksession
    val spark = SparkSession
      .builder()
      .appName("Task4")
      .config(conf = conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    // creating dataframes
    val df_edges = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("input/group-edges.csv")

    val df_groups = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("input/meta-groups.csv")

    // Printing the Schema
    df_edges.printSchema()
    df_groups.printSchema()
    df_edges.createOrReplaceTempView("ed")
    df_groups.createOrReplaceTempView("gp")
    val g1 = spark.sql("select * from gp")
    val e1 = spark.sql("select * from ed")
    //replacing column names
    val vertices = g1
      .withColumnRenamed("group_id", "id").limit(100)
      .distinct()

    val edges = e1
      .withColumnRenamed("group1", "src").limit(500).distinct()
      .withColumnRenamed("group2", "dst").limit(500).distinct()

    val graph = GraphFrame(vertices, edges)

    edges.cache()
    vertices.cache()
    graph.vertices.show()
    graph.edges.show()

    println("Total Number of vertices count is : " + graph.vertices.count)
    println("Total Number of edges count is: " + graph.edges.count)

    val stationPageRank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()
  }
}