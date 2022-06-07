package labelheap;

import chainexception.*;

public class LabelInvalidUpdateException extends ChainException {

  public LabelInvalidUpdateException() {
    super();
  }

  public LabelInvalidUpdateException(Exception ex, String name) {
    super(ex, name);
  }

}
