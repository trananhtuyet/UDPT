package com.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TimeTrackerClient implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(TimeTrackerClient.class);
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String TEAM_STATUS_PATH = "/team_status";
    public static final String CONFIG_PATH = "/config";

    private ZooKeeper zooKeeper;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private final String userId;

    // Biến để lưu trữ Watcher được thiết lập bởi API Server
    private Watcher statusWatcher;

    public TimeTrackerClient(String userId) {
        this.userId = userId;
    }
    
    // Phương thức kết nối
    public void connect(Watcher watcher) throws Exception {
        // Sử dụng watcher truyền vào (nếu là API Server) hoặc chính nó (nếu là Client)
        this.statusWatcher = watcher;
        this.zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, this);
        connectionLatch.await();
        ensurePathExists(TEAM_STATUS_PATH);
        ensurePathExists(CONFIG_PATH);
        logger.info("Client {} đã kết nối thành công. Path Status: {}", userId, TEAM_STATUS_PATH);
    }
    
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectionLatch.countDown();
        } 
        
        // Chuyển sự kiện cho Watcher của API Server xử lý (Nếu được thiết lập)
        if (statusWatcher != null) {
            statusWatcher.process(event);
        }
    }

    private void ensurePathExists(String path) throws Exception {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, "Root Node".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Đã tạo ZNode gốc: {}", path);
        }
    }

    // Thao tác Check-in (Tạo Ephemeral ZNode)
    public boolean checkIn() throws Exception {
        if (userId == null || userId.trim().isEmpty()) {
            throw new IllegalStateException("userId không được để trống");
        }
        
        String userPath = TEAM_STATUS_PATH + "/" + userId;
        if (zooKeeper.exists(userPath, false) != null) {
            logger.warn("Client {} đã Check-in trước đó.", userId);
            return false;
        }
        
        LocalDateTime now = LocalDateTime.now();
        String data = "{\"startTime\":\"" + now.toString() + "\"}";

        zooKeeper.create(userPath, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info(" Client {} Check-in thành công. Data: {}", userId, data);
        return true;
    }

    // Thao tác Check-out (Xóa ZNode)
    public boolean checkOut() throws Exception {
        if (userId == null || userId.trim().isEmpty()) {
            throw new IllegalStateException("userId không được để trống");
        }
        
        String userPath = TEAM_STATUS_PATH + "/" + userId;
        Stat stat = zooKeeper.exists(userPath, false);
        if (stat != null) {
            zooKeeper.delete(userPath, -1);
            logger.info(" Client {} Check-out thành công. (ZNode đã bị xóa)", userId);
            return true;
        } else {
            logger.warn("Client {} chưa Check-in hoặc đã Check-out trước đó.", userId);
            return false;
        }
    }
    
    //  Lấy danh sách thành viên đang hoạt động, có thể kèm Watcher
    public List<String> getActiveMembers(Watcher watcher) throws KeeperException, InterruptedException {
        // Thiết lập Watcher tại đây. Khi ZNode con của TEAM_STATUS_PATH thay đổi (tạo/xóa), Watcher sẽ được kích hoạt
        return zooKeeper.getChildren(TEAM_STATUS_PATH, watcher);
    }
    
    public byte[] getMemberData(String memberId) throws KeeperException, InterruptedException {
        String userPath = TEAM_STATUS_PATH + "/" + memberId;
        Stat stat = new Stat();
        return zooKeeper.getData(userPath, false, stat);
    }
    
    //  Cập nhật dữ liệu cấu hình
    public void setConfigData(String key, String value) throws KeeperException, InterruptedException {
        String path = CONFIG_PATH + "/" + key;
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            // Tạo mới nếu chưa có (Persistent)
            zooKeeper.create(path, value.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            // Cập nhật nếu đã có
            zooKeeper.setData(path, value.getBytes(StandardCharsets.UTF_8), -1);
        }
    }

    //  Đọc dữ liệu cấu hình
     public byte[] getConfigData(String key) throws KeeperException, InterruptedException {
        String path = CONFIG_PATH + "/" + key;
        Stat stat = new Stat();
        return zooKeeper.getData(path, false, stat);
    }

    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                logger.error("Lỗi khi đóng kết nối Zookeeper: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }
}
