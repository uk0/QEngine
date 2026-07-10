package com.tsdb.client;

import java.io.IOException;

/**
 * A decoded server MSG_ERROR frame: the server-side status code plus its
 * message.  {@link TsdbClient} throws it for every well-formed error payload,
 * so callers can branch on the code:
 *
 * <pre>
 *   try { client.dropTable("missing"); }
 *   catch (TsdbServerException e) { handle(e.code); }
 * </pre>
 *
 * <p>The exception message is identical to the plain IOException previously
 * thrown ({@code "server rc=&lt;code&gt;: &lt;msg&gt;"}), so existing string
 * matching keeps working.
 */
public class TsdbServerException extends IOException {

    private static final long serialVersionUID = 1L;

    /** Server status code (rc) from the MSG_ERROR payload. */
    public final int code;

    public TsdbServerException(int code, String msg) {
        super("server rc=" + code + ": " + msg);
        this.code = code;
    }
}
