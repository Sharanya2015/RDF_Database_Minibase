package quadrupleheap;

import chainexception.*;

public class InvalidQTypeException extends ChainException {

  public InvalidQTypeException() {
    super();
  }

  public InvalidQTypeException(Exception ex, String name) {
    super(ex, name);
  }

}
