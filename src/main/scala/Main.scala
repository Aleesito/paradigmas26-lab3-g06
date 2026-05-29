import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None         => return // scopt prints error messages
    }

    val spark = SparkSession
      .builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc    = spark.sparkContext

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    val rddSubs = sc.parallelize(subscriptions)

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = rddSubs.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      // en el kickstart se usaba el fold de forma muy interesante
      // feedOpt.fold(valor si es None)(aplico función si es Some)
      val posts   = feedOpt match {
        case None          =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        case Some(content) =>
          JsonParser.parsePosts(content, subscription.name)
      }
      (feedOpt.isDefined, posts)
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.filter(_._1).count
    val feedsFailed  = downloadResults.count - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts     = downloadResults.flatMap(_._2)
    val postsSuccess = allPosts.count
    val postsFailed  = downloadResults.filter(_._2.isEmpty).count

    // Filter empty posts
    val filteredPosts = allPosts.filter(Analyzer.isEmptyPost(_))
    val postsFiltered = allPosts.count - filteredPosts.count

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars   = if (!filteredPosts.isEmpty) (totalChars / filteredPosts.count).toInt else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess"  -> feedsSuccess.toInt,
      "feedsFailed"   -> feedsFailed.toInt,
      "postsSuccess"  -> postsSuccess.toInt,
      "postsFailed"   -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars"      -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }
    /*
    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats    = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
     */

  }
}
