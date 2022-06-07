package quadrupleheap;

import chainexception.*;

public class InvalidQUpdateException extends ChainException {

  public InvalidQUpdateException() {
    super();
  }

  public InvalidQUpdateException(Exception ex, String name) {
    super(ex, name);
  }

}
