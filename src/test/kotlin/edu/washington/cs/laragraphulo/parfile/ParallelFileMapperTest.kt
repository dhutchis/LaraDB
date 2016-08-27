package edu.washington.cs.laragraphulo.parfile

import org.junit.Assert
import org.junit.Test
import java.io.File
import java.nio.file.Files

/**
 * Spawn a bunch of threads to work on files created in a temporary directory
 */
class ParallelFileMapperTest {
  @Test(timeout = 10000)
  fun run() {
    val inputDir = Files.createTempDirectory("inputDir").toFile()
    inputDir.deleteOnExit()
    println("inputDir: $inputDir")

    /** Number of files */
    val N = 125
    val inputFiles = Array(N) {
      val f = File(inputDir, it.toString()+".txt")
      f.writeText(it.toString())
      f.deleteOnExit()
      f
    }.asList()
        //listOf("1.txt", "2.txt", "3.txt").map { File(inputDir, it) }
//    inputFiles.mapIndexed { i, file ->
//      file.writeText(i.toString())
////      file.deleteOnExit()
//    }

    val lockDir = Files.createTempDirectory("lockDir").toFile()
    lockDir.deleteOnExit()
    println("lockDir: $lockDir")


    val ACTION = object : FileAction {
      override fun run(f: File) {
        val i = f.nameWithoutExtension.toInt()
        Assert.assertEquals("Bad file $f; expected $i", i, f.readLines().first().toInt())
      }
    }

    /** Number of threads */
    val NT = 10
    val threads = Array(NT) { Thread(ParallelFileMapper(inputFiles, lockDir, ACTION), "t$it") }
    threads.forEach { it.start() }
    threads.forEach { it.join() }
    Assert.assertEquals("Different number of files in $lockDir than expected $N: ${lockDir.listFiles()}", N, lockDir.listFiles().size)
  }



}