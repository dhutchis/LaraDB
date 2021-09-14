package edu.washington.cs.laragraphulo.encoding;

public class VariableLengthHelper {
  private static VariableLengthHelper ourInstance = new VariableLengthHelper();

  public static VariableLengthHelper getInstance() {
    return ourInstance;
  }

  private VariableLengthHelper() {
  }
}
