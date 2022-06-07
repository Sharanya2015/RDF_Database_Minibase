package quadrupleheap;

import chainexception.*;

public class InvalidQuadrupleSizeException extends ChainException {

   public InvalidQuadrupleSizeException() {
      super();
   }

   public InvalidQuadrupleSizeException(Exception ex, String name) {
      super(ex, name);
   }

}
