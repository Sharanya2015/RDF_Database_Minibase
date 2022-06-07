package labelheap;

import chainexception.*;

public class LabelSpaceNotAvailableException extends ChainException {

  public LabelSpaceNotAvailableException() {
    super();

  }

  public LabelSpaceNotAvailableException(Exception ex, String name) {
    super(ex, name);
  }

}
