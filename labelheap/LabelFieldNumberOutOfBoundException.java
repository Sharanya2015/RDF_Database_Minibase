package labelheap;

import chainexception.*;

public class LabelFieldNumberOutOfBoundException extends ChainException {

   public LabelFieldNumberOutOfBoundException() {
      super();
   }

   public LabelFieldNumberOutOfBoundException(Exception ex, String name) {
      super(ex, name);
   }

}
