package com.corundumstudio.socketio.handler;

/**
 * Created by IntelliJ IDEA.
 * User: Russ
 * Date: 8/20/13
 * Time: 11:17 AM
 */
public class NullSessionAuthorizer implements SessionAuthorizer {

    private static final NullSessionAuthorizer INSTANCE = new NullSessionAuthorizer();

    @Override
    public void authorize(String sessionId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAuthorized(String sessionId) {
        return true;
    }

    @Override
    public void deauthorize(String sessionId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public static NullSessionAuthorizer getInstance() {
        return INSTANCE;
    }
}
