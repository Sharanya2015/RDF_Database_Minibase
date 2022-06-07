package iterator;
import chainexception.*;

import java.lang.*;

public class QuadrupleUtilsException extends ChainException {
  public QuadrupleUtilsException(String s){super(null,s);}
  public QuadrupleUtilsException(Exception prev, String s){ super(prev,s);}
}
