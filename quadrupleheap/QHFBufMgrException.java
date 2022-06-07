package quadrupleheap;

import chainexception.*;

public class QHFBufMgrException extends ChainException {

    public QHFBufMgrException() {
        super();
    }

    public QHFBufMgrException(Exception ex, String name) {
        super(ex, name);
    }

}
