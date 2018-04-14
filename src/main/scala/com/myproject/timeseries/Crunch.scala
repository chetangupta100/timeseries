package com.myproject.timeseries

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.io.InputStreamReader
import java.text.DecimalFormat

/**
 * Program to read file from input which contains timestamp and value. The program tries to group all the values within 60
 * secs and computes avg/ sum/ min/max of the given window. If the next epoch value is more than the start epoch of the
 * current window then the metrics of current window are flushed out to file and the metrics are reset.
 */
object Crunch {

  def main(args: Array[String]): Unit = {
    var inputReader: BufferedReader = null
    var reader: BufferedReader = null
    var writer: BufferedWriter = null
    try {

      inputReader = new BufferedReader(new InputStreamReader(System.in))
      val inputFile: String = inputReader.readLine()
      val outputFile: String = inputReader.readLine()
      reader = new BufferedReader(new FileReader(inputFile))
      writer = new BufferedWriter(new FileWriter(outputFile))

      var line: String = reader.readLine()
      var count: Int = 0
      val df: DecimalFormat = new DecimalFormat("#.#####")
      var runningSum: Double = 0
      var localMin: Double = java.lang.Double.MAX_VALUE
      var localMax: Double = java.lang.Double.MIN_VALUE
      var localCount: Int = 0
      var startTime: Long = 0
      var time: Long = 0

      while (line != null) {
        val split: Array[String] = line.split("\t")
        count += 1
        time = java.lang.Long.parseLong(split(0))
        val value: Double = java.lang.Double.parseDouble(split(1))

        // if current time > 60 secs : reset window
        if ((time - startTime) >= 60) {
          if (count > 1) {
            val builder: StringBuilder = new StringBuilder()
            builder.append(time + " ")
            builder.append(df.format((runningSum / localCount.toDouble)) + " ")
            builder.append(localCount + " ")
            builder.append(localMin + " ")
            builder.append(localMax + "\n")
            writer.write(builder.toString)
          }
          // reset values
          runningSum = value
          localMin = value
          localMax = value
          localCount = 1
          startTime = time

        } else {
          // process next value
          runningSum += value
          if (value < localMin) localMin = value
          if (value > localMax) localMax = value
          localCount += 1

        }
      }

      val builder: StringBuilder = new StringBuilder()
      builder.append(time + " ")
      builder.append((runningSum / localCount.toDouble) + " ")
      builder.append(localCount + " ")
      builder.append(localMin + " ")
      builder.append(localMax + "\n")
      writer.write(builder.toString)
      writer.close()

    } catch {
      case e: Exception => e.printStackTrace()

    } finally {
      if (reader != null) {
        try reader.close()
        catch {
          case e1: IOException => { // duck
          }
        }
      }
      if (writer != null) {
        try writer.close()
        catch {
          case e1: IOException => { // duck
          }
        }
      }
      if (inputReader != null) {
        try inputReader.close()
        catch {
          case e1: IOException => { // duck
          }
        }
      }
    }
  }

}
