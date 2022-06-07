/* File hferr.java  */

package quadrupleheap;

import chainexception.*;

public class QHFException extends ChainException {

    public QHFException() {
        super();

    }

    public QHFException(Exception ex, String name) {
        super(ex, name);
    }

}
