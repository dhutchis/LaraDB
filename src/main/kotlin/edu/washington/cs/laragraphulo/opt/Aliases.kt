package edu.washington.cs.laragraphulo.opt

import org.apache.accumulo.core.data.ArrayByteSequence


/**
 * An attribute name.
 */
typealias Name = String

/** An alias for the ArrayByteSequence. This is the primary data holder. */
typealias ABS = ArrayByteSequence

/**
 * `>= 0` means fixed width.
 * `-1` means variable width.
 */
typealias Width = Int

/** Attribute/column position */
typealias Position = Int

/**
 * Interpretation for missing values.
 */
typealias Default = ByteArray
