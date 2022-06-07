/* File hferr.java  */

package quadrupleheap;

import chainexception.*;

public class QHFDiskMgrException extends ChainException {

    public QHFDiskMgrException() {
        super();
    }

    public QHFDiskMgrException(Exception ex, String name) {
        super(ex, name);
    }

}
