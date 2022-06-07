package quadrupleheap;

import chainexception.*;

public class QSpaceNotAvailableException extends ChainException {

  public QSpaceNotAvailableException() {
    super();

  }

  public QSpaceNotAvailableException(Exception ex, String name) {
    super(ex, name);
  }

}
