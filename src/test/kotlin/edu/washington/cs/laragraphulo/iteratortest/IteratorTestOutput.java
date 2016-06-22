/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.washington.cs.iteratortest;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;

/**
 * The expected results from invoking a {@link IteratorTestCase} on a {@link IteratorTestInput}. The output will be either a {@link SortedMap} of Keys and
 * Values or an exception but never both. If one of these members is null, the other is guaranteed to be non-null.
 */
public class IteratorTestOutput {

  /**
   * An outcome about what happened during a test case.
   */
  public enum TestOutcome {
    /**
     * The IteratorTestCase proactively passed.
     */
    PASSED, /**
     * The IteratorTestCase proactively failed.
     */
    FAILED, /**
     * The IteratorTestCase completed, but the pass/fail should be determined by the other context.
     */
    COMPLETED
  }

  private final SortedMap<Key,Value> output;
  private final Exception exception;
  private final TestOutcome outcome;

  public IteratorTestOutput(TestOutcome outcome) {
    this.outcome = outcome;
    if (outcome == TestOutcome.COMPLETED) {
      throw new IllegalArgumentException("This constructor is only for use with PASSED and FAILED");
    }
    output = null;
    exception = null;
  }

  /**
   * Create an instance of the class.
   *
   * @param output
   *          The sorted collection of Key-Value pairs generated by an Iterator.
   */
  public IteratorTestOutput(SortedMap<Key,Value> output) {
    this.output = Collections.unmodifiableSortedMap(requireNonNull(output));
    this.exception = null;
    this.outcome = TestOutcome.COMPLETED;
  }

  public IteratorTestOutput(Exception e) {
    this.output = null;
    this.exception = requireNonNull(e);
    this.outcome = TestOutcome.FAILED;
  }

  /**
   * @return The outcome of the test.
   */
  public TestOutcome getTestOutcome() {
    return outcome;
  }

  /**
   * Returns the output from the iterator.
   *
   * @return The sorted Key-Value pairs from an iterator, null if an exception was thrown.
   */
  public SortedMap<Key,Value> getOutput() {
    return output;
  }

  /**
   * @return True if there is output, false if the output is null.
   */
  public boolean hasOutput() {
    return null != output;
  }

  /**
   * Returns the exception thrown by the iterator.
   *
   * @return The exception thrown by the iterator, null if no exception was thrown.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * @return True if there is an exception, null if the iterator successfully generated Key-Value pairs.
   */
  public boolean hasException() {
    return null != exception;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((exception == null) ? 0 : exception.hashCode());
    result = prime * result + ((outcome == null) ? 0 : outcome.hashCode());
    result = prime * result + ((output == null) ? 0 : output.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IteratorTestOutput)) {
      return false;
    }

    IteratorTestOutput other = (IteratorTestOutput) o;

    if (outcome != other.outcome) {
      return false;
    }

    if (hasOutput()) {
      if (!other.hasOutput()) {
        return false;
      }
      return output.equals(other.output);
    }

    if (!other.hasException()) {
      return false;
    }
    return exception.equals(other.getException());

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append("[outcome=").append(outcome).append(", output='").append(output).append("', exception=").append(exception).append("]");
    return sb.toString();
  }
}
