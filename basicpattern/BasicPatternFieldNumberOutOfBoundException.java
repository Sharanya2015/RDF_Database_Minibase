package basicpattern;
import chainexception.*;

public class BasicPatternFieldNumberOutOfBoundException extends ChainException{

   public BasicPatternFieldNumberOutOfBoundException()
   {
      super();
   }
   
   public BasicPatternFieldNumberOutOfBoundException (Exception ex, String name)
   {
      super(ex, name); 
   }

}

