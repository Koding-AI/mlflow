package org.mlflow.artifacts;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class UriUserInfoTest {

    @Test
    public void testOnlyUsername() {
        UriUserInfo userInfo = new UriUserInfo("username");
        assertEquals("username", userInfo.getUsername());
        assertFalse(userInfo.getPassword().isPresent());
    }

    @Test
    public void testLoginAndPassword() {
        UriUserInfo userInfo = new UriUserInfo("username:password");
        assertEquals("username", userInfo.getUsername());
        assertEquals("password", userInfo.getPassword().get());
    }
}
