package quadrupleheap;

import chainexception.*;

public class QFieldNumberOutOfBoundException extends ChainException {

    public QFieldNumberOutOfBoundException() {
        super();
    }

    public QFieldNumberOutOfBoundException(Exception ex, String name) {
        super(ex, name);
    }

}
