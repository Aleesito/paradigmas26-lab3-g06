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
      Console.err.println("Error: No valid subscriptions found")
      return
    }

    val rddSubs = sc.parallelize(subscriptions)

    // Necesitamos crear los acumuladores antes de ejecutar cualquier operación del RDD.
    val feedsSuccessAcc    = sc.longAccumulator("Feeds Downloaded Successfully")
    val feedsFailedAcc     = sc.longAccumulator("Feeds Failed")
    val postsDownloadedAcc = sc.longAccumulator("Posts Downloaded")
    val postsFailedAcc     = sc.longAccumulator("Posts Failed (Parse Error)")
    val postsFilteredAcc   = sc.longAccumulator("Posts Filtered (empty)")

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = rddSubs.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      // en el kickstart se usaba el fold de forma muy interesante
      // feedOpt.fold(valor si es None)(aplico función si es Some)
      val posts   = feedOpt match {
        case None          =>
          Console.err.println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        case Some(content) =>
          JsonParser.parsePosts(content, subscription.name)
      }
      (feedOpt.isDefined, posts)
    }

    // Download feeds, emit posts, update feed/post accumulators
    val allPosts: RDD[Post] = rddSubs.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      feedOpt match {
        case None =>
          feedsFailedAcc.add(1)
          Console.err.println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        case Some(content) =>
          feedsSuccessAcc.add(1)
          val (posts, failed) = JsonParser.parsePosts(content, subscription.name)
          postsDownloadedAcc.add(posts.length)
          postsFailedAcc.add(failed)          // ← now tracked, not just warned
          posts
      }
    }

    // Filter empty posts — also updates the filtered accumulator as a side effect
    val filteredPosts: RDD[Post] = allPosts.filter { post =>
      if (Analyzer.isEmptyPost(post)) {
        true
      } else {
        postsFilteredAcc.add(1)   // worker increments
        false
      }
    }

    // Action: materializes the pipeline and flushes accumulator values to driver
    val t0 = System.currentTimeMillis()
    val postCount = filteredPosts.count()   // ← the action
    val t1 = System.currentTimeMillis()
    println(s"Pipeline stage 1 (download + filter): ${(t1 - t0) / 1000.0}s")

    // Compute average post length — safe to read accumulators here
    val avgChars: Int = if (postCount > 0) {
      val totalChars = filteredPosts
        .map(p => p.title.length + p.selftext.length)
        .sum()
      (totalChars / postCount).toInt
    } else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess"  -> feedsSuccessAcc.value.toInt,
      "feedsFailed"   -> feedsFailedAcc.value.toInt,
      "postsSuccess"  -> postsDownloadedAcc.value.toInt,
      "postsFailed"   -> postsFailedAcc.value.toInt,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars"      -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (postCount == 0) {
      Console.err.println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    val entitiesCounts = allEntities
      .map(entity => ((entity.entityType, entity.text), 1))
      .reduceByKey(_ + _)

    val t2 = System.currentTimeMillis()
    val sortedRDD = entitiesCounts
      .sortBy { case ((entityType, entityName), count) => (-count, entityType, entityName) }
      .take(cmdArgs.topK)
      .toMap
    val t3 = System.currentTimeMillis()
    println(s"Pipeline stage 2 (NER + reduce): ${(t3 - t2) / 1000.0}s")

    val typeMap = entitiesCounts
      .map { case ((entityType, _), count) => (entityType, count) }
      .reduceByKey(_ + _)
      .collect()
      .toMap
    val typeStats = typeMap + ("total" -> typeMap.values.sum)

    println(Formatters.formatTypeStats(typeStats))
    println(Formatters.formatEntityStats(sortedRDD, cmdArgs.topK))

    spark.stop()
  }
}
