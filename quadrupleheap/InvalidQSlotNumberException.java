package quadrupleheap;

import chainexception.*;

public class InvalidQSlotNumberException extends ChainException {

  public InvalidQSlotNumberException() {
    super();
  }

  public InvalidQSlotNumberException(Exception ex, String name) {
    super(ex, name);
  }

}
