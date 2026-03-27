package com.aegisraft;

import com.aegisraft.api.HttpApiServer;
import com.aegisraft.config.ClusterConfigLoader;
import com.aegisraft.config.NodeConfig;
import com.aegisraft.core.RaftNode;
import java.nio.file.Path;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || !"--config".equals(args[0])) {
            throw new IllegalArgumentException("Usage: java -jar aegisraft.jar --config <path>");
        }
        NodeConfig config = ClusterConfigLoader.load(Path.of(args[1]));
        RaftNode node = new RaftNode(config);
        HttpApiServer server = new HttpApiServer(config.host(), config.port(), node);
        node.start();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            node.close();
        }));
    }
}
