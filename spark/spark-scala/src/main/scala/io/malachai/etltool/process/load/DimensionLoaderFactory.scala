package io.malachai.etltool.process.load

object DimensionLoaderFactory {

  def getInstance(dimension: String): AccountsDimensionLoader = dimension match {
    case "accounts" => new AccountsDimensionLoader()
    case _ => throw new IllegalArgumentException
  }
}
