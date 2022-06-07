package iterator;
import chainexception.*;

import java.lang.*;

public class LabelUtilsException extends ChainException {
  public LabelUtilsException(String s){super(null,s);}
  public LabelUtilsException(Exception prev, String s){ super(prev,s);}
}