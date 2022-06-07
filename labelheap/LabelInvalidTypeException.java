package labelheap;
import chainexception.*;


public class LabelInvalidTypeException extends ChainException {


  public LabelInvalidTypeException ()
  {
     super();
  }

  public LabelInvalidTypeException (Exception ex, String name)
  {
    super(ex, name);
  }



}