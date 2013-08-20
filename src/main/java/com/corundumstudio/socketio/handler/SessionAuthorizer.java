package com.corundumstudio.socketio.handler;

/**
 * Created by IntelliJ IDEA.
 * User: Russ
 * Date: 8/20/13
 * Time: 11:16 AM
 */
public interface SessionAuthorizer {

    void authorize(String sessionId);

    boolean isAuthorized(String sessionId);

    void deauthorize(String sessionId);

}
