package org.terry.common.task;

import java.util.Properties;

public interface Task {
    void init(Properties prop);

    boolean run();
}
