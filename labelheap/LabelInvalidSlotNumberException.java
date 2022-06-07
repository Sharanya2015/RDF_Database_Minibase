package labelheap;
import chainexception.*;


public class LabelInvalidSlotNumberException extends ChainException {


  public LabelInvalidSlotNumberException ()
  {
     super();
  }

  public LabelInvalidSlotNumberException (Exception ex, String name)
  {
    super(ex, name);
  }



}
