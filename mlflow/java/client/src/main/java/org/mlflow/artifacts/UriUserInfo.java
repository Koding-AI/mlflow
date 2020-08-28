package org.mlflow.artifacts;

import java.util.Optional;

public class UriUserInfo {

    private final String username;
    private final String password;

    public UriUserInfo(String userInfo) {
        if (userInfo.contains(":")) {
            this.username = userInfo.substring(0,userInfo.indexOf(":"));
            this.password = userInfo.substring(userInfo.indexOf(":") + 1);
        } else {
            this.username = userInfo;
            this.password = null;
        }
    }

    public String getUsername() {
        return username;
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
