package com.casey.tdc

import io.eels.component.parquet.ParquetSink
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * @author ${user.name}
  */
object App {

    def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

    def main(args: Array[String]) {
        println("Hello World!")
        println("concat arguments = " + foo(args))

        val source = args(0)
        val output = args(1)

        implicit val hadoopConfiguration = new Configuration()
        implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration)

        val parquetFilePath = new Path(source)
        println("loading parquet file " + source)

        val sink = ParquetSink(new Path(output + ".appended"))
    }

}
