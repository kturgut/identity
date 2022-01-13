package com.conviva.id.config

import com.conviva.id.storage.StorageConfig
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

trait ConfigHelper {

  import AssignmentAggregatorConfig._
  val config: Config

  protected[config] lazy val convivaIdConfig = config.getConfig(ConvivaIdConfigPath)
  protected[config] lazy val assignmentConfig = config.getConfig(AssignmentConfigPath)
  protected[config] lazy val aggregatorConfig = config.getConfig(AggregatorConfigPath)
  lazy val storageConfig = StorageConfig(config.getConfig(Storage))


  protected def getDateTime(path:String, primary:Config, secondary:Config):DateTime =
    (primary::secondary::Nil).find (_.hasPath(path)).map(a=>ISODateTimeFormat.dateTimeParser.parseDateTime(a.getString(path)))
      .getOrElse(throw new IllegalArgumentException(s"path:$path"))

  protected def getIntSeq(path:String, primary:Config, secondary:Config):Seq[Int] = {
    import collection.JavaConverters._
    (primary::secondary::Nil).find (_.hasPath(path)).map(a=>a.getIntList(path).asScala.map(_.toInt))
      .getOrElse(throw new IllegalArgumentException(s"path:$path"))
  }

  protected def getIntOrElse(path:String, config:Config, default:Int):Int =
    if (config.hasPathOrNull(path)) config.getInt(path) else default

  protected def getStringOption(path:String, primary:Config, secondary:Config):Option[String] =
    (primary::secondary::Nil).find (_.hasPath(path)).map(a=>Some(a.getString(path))).getOrElse(None)

  protected def getStringOrElse(path:String, primary:Config, secondary:Config, default:String):String =
    getStringOption(path,primary,secondary).getOrElse(default)
}