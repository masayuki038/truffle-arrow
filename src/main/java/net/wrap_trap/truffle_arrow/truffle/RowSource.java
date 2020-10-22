package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

import java.util.List;

/**
 * Root expression that receives nothing and sends rows somewhere.
 *
 * Could be a literal, or a file somewhere.
 */
public abstract class RowSource extends Node {

    protected RowSink then;

    protected RowSource(RowSink then) {
        this.then = then;
        this.insert(then);
    }

    /**
     * Flush all rows. Called once for the entire execution of the query.
     */
    protected abstract List<Row> execute() throws UnexpectedResultException;
}
