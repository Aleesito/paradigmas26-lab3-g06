import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return list of options: Some(Subscription) for valid entries, None for malformed entries
   *         returns empty list if file not found
   */
  def readSubscriptions(filePath: String): List[Option[Subscription]] = {
    implicit val formats: Formats = DefaultFormats

    val contentOpt = try {
      val source = Source.fromFile(filePath)
      try Some(source.mkString)
      finally source.close()
    } catch {
      case _: Exception =>
        println(s"Error: Could not load $filePath - file not found")
        None
    }

    // si pudimos leer el archivo
    contentOpt match {
      case None          => List()
      case Some(content) =>
        try {
          val json          = parse(content)
          val subscriptions = json.extract[List[Map[String, String]]]
          subscriptions.map { sub =>
            if (sub.contains("name") && sub.contains("url")) {
              Some(Subscription(sub("name"), sub("url")))
            } else {
              println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
              None
            }
          }
        } catch {
          case _: Exception =>
            println(s"Error: Could not load $filePath - invalid JSON format")
            List.empty[Option[Subscription]]
        }
    }
  }

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Option containing JSON as String, None on network error or timeout
   */
  def downloadFeed(url: String): Option[String] = {
    try {
      val source  = Source.fromURL(url)
      try Some(source.mkString) finally source.close
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return Option containing list of entities, None if file missing
   */
  def readDictionaryFile(filePath: String): Option[List[String]] =
    try {
      val source = Source.fromFile(filePath)
      try {
        val lines = source
          .getLines()
          .map(_.trim)
          .filter(_.nonEmpty)
          .filterNot(_.startsWith("#"))
          .toList
        Some(lines)
      } finally source.close()
    } catch {
      case _: Exception =>
        println(s"Warning: Could not load $filePath")
        None
    }
}
