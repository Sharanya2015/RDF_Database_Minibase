package quadrupleheap;

import chainexception.*;

public class QFileAlreadyDeletedException extends ChainException {

    public QFileAlreadyDeletedException() {
        super();
    }

    public QFileAlreadyDeletedException(Exception ex, String name) {
        super(ex, name);
    }

}
