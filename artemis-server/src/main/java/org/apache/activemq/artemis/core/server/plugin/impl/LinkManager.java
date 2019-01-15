package org.apache.activemq.artemis.core.server.plugin.impl;

import java.util.HashMap;
import java.util.Map;

public class LinkManager {

    public Map<String, Link> links = new HashMap<>();

    public void register(Link link) {
        links.put(link.getName(), link);
    }

    public Link get(String name) {
        return links.get(name);
    }
}
