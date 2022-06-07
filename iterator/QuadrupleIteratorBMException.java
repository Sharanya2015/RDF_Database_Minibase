package iterator;

import chainexception.ChainException;

public class QuadrupleIteratorBMException extends ChainException {

    public QuadrupleIteratorBMException(String s){super(null,s);}
    public QuadrupleIteratorBMException(Exception prev, String s){ super(prev,s);}
}
