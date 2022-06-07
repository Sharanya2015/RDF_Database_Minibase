package labelheap;

import chainexception.ChainException;

public class InvalidLabelSizeException extends ChainException {

    public InvalidLabelSizeException()
    {
        super();
    }

    public InvalidLabelSizeException(Exception ex, String name)
    {
        super(ex, name);
    }

}
