package labelheap;

import chainexception.*;

public class LabelFileAlreadyDeletedException extends ChainException {

   public LabelFileAlreadyDeletedException() {
      super();
   }

   public LabelFileAlreadyDeletedException(Exception ex, String name) {
      super(ex, name);
   }

}
