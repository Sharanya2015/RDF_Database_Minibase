package basicpattern;
import chainexception.*;


public class BasicPatternInvalidTypeException extends ChainException {


  public BasicPatternInvalidTypeException ()
  {
     super();
  }

  public BasicPatternInvalidTypeException (Exception ex, String name)
  {
    super(ex, name);
  }



}
