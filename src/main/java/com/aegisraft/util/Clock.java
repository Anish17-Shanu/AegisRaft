package com.aegisraft.util;

public interface Clock {
    long now();

    static Clock system() {
        return System::currentTimeMillis;
    }
}
