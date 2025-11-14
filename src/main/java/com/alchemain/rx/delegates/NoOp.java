package com.alchemain.rx.delegates;

/**
 * A no-op class to attach to the end of a chain to add the response from the
 * last step to the audit log. Helpful during development and debugging by
 * creating a more detailed audit log.
 * 
 * @author bilal
 * 
 */

public class NoOp extends Delegate {
	@Override
	public void execute() throws Exception {
	}
}
