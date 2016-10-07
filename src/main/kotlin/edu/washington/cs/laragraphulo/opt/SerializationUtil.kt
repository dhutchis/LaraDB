package edu.washington.cs.laragraphulo.opt


import com.google.common.base.Preconditions
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.Writable

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.io.Serializable

/**
 * Partially based from [org.apache.commons.lang3.SerializationUtils].

 *
 * Assists with the serialization process and performs additional functionality based
 * on serialization.
 */
class SerializationUtil private constructor() {
  companion object {

    fun serializeWritableBase64(writable: Writable): String {
      val b = serializeWritable(writable)
      return org.apache.accumulo.core.util.Base64.encodeBase64String(b)
    }

    fun deserializeWritableBase64(writable: Writable, str: String) {
      val b = Base64.decodeBase64(str)
      deserializeWritable(writable, b)
    }

    fun serializeBase64(obj: Serializable): String {
      val b = serialize(obj)
      return org.apache.accumulo.core.util.Base64.encodeBase64String(b)
    }

    fun deserializeBase64(str: String): Any {
      val b = Base64.decodeBase64(str)
      return deserialize(b)
    }


    // Interop with Hadoop Writable
    //-----------------------------------------------------------------------

    fun serializeWritable(writable: Writable): ByteArray {
      val baos = ByteArrayOutputStream(512)
      serializeWritable(writable, baos)
      return baos.toByteArray()
    }

    fun serializeWritable(obj: Writable, outputStream: OutputStream) {
      Preconditions.checkNotNull(obj)
      Preconditions.checkNotNull(outputStream)
      try {
        DataOutputStream(outputStream).use { out -> obj.write(out) }
      } catch (ex: IOException) {
        throw RuntimeException(ex)
      }

    }

    fun deserializeWritable(writable: Writable, inputStream: InputStream) {
      Preconditions.checkNotNull(writable)
      Preconditions.checkNotNull(inputStream)
      try {
        DataInputStream(inputStream).use { `in` -> writable.readFields(`in`) }
      } catch (ex: IOException) {
        throw RuntimeException(ex)
      }

    }

    fun deserializeWritable(writable: Writable, objectData: ByteArray) {
      Preconditions.checkNotNull(objectData)
      deserializeWritable(writable, ByteArrayInputStream(objectData))
    }


    // Serialize
    //-----------------------------------------------------------------------

    /**
     *
     * Serializes an `Object` to the specified stream.
     *
     *
     *
     * The stream will be closed once the object is written.
     * This avoids the need for a finally clause, and maybe also exception
     * handling, in the application code.
     *
     *
     *
     * The stream passed in is not buffered internally within this method.
     * This is the responsibility of your application if desired.

     * @param obj          the object to serialize to bytes, may be null
     * *
     * @param outputStream the stream to write to, must not be null
     * *
     * @throws IllegalArgumentException if `outputStream` is `null`
     */
    fun serialize(obj: Serializable, outputStream: OutputStream) {
      Preconditions.checkNotNull(outputStream)
      try {
        ObjectOutputStream(outputStream).use { out -> out.writeObject(obj) }
      } catch (ex: IOException) {
        throw RuntimeException(ex)
      }

    }

    /**
     *
     * Serializes an `Object` to a byte array for
     * storage/serialization.

     * @param obj the object to serialize to bytes
     * *
     * @return a byte[] with the converted Serializable
     */
    fun serialize(obj: Serializable): ByteArray {
      val baos = ByteArrayOutputStream(512)
      serialize(obj, baos)
      return baos.toByteArray()
    }

    // Deserialize
    //-----------------------------------------------------------------------

    /**
     *
     * Deserializes an `Object` from the specified stream.
     *
     *
     *
     * The stream will be closed once the object is written. This
     * avoids the need for a finally clause, and maybe also exception
     * handling, in the application code.
     *
     *
     *
     * The stream passed in is not buffered internally within this method.
     * This is the responsibility of your application if desired.

     * @param inputStream the serialized object input stream, must not be null
     * *
     * @return the deserialized object
     * *
     * @throws IllegalArgumentException if `inputStream` is `null`
     */
    fun deserialize(inputStream: InputStream): Any {
      Preconditions.checkNotNull(inputStream)
      try {
        ObjectInputStream(inputStream).use { `in` -> return `in`.readObject() }
      } catch (ex: ClassNotFoundException) {
        throw RuntimeException(ex)
      } catch (ex: IOException) {
        throw RuntimeException(ex)
      }

    }

    /**
     *
     * Deserializes a single `Object` from an array of bytes.

     * @param objectData the serialized object, must not be null
     * *
     * @return the deserialized object
     * *
     * @throws IllegalArgumentException if `objectData` is `null`
     */
    fun deserialize(objectData: ByteArray): Any {
      Preconditions.checkNotNull(objectData)
      return deserialize(ByteArrayInputStream(objectData))
    }
  }

}
