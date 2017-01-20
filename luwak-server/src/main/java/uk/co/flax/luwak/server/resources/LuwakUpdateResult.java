package uk.co.flax.luwak.server.resources;

/**
 * Created by Tom.Ridd on 20/01/2017.
 */
public class LuwakUpdateResult {
  private boolean result;
  private String inputDocument;

  public LuwakUpdateResult(String inputDocument, boolean result) {
    this.result = result;
    this.inputDocument = inputDocument;
  }

  public boolean isResult() {
    return result;
  }

  public String getInputDocument() {
    return inputDocument;
  }
}
