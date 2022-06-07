package bpiterator;

import chainexception.*;

public class UnknownAttrType extends ChainException {
  public UnknownAttrType(String s) {
    super(null, s);
  }

  public UnknownAttrType(Exception prev, String s) {
    super(prev, s);
  }
}
