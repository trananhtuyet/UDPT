package com.example;

import com.google.gson.Gson;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TimeTrackerClient implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(TimeTrackerClient.class);
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String TEAM_STATUS_PATH = "/team_status";
    
    private ZooKeeper zooKeeper;
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private final String userId;

    // Biến để lưu trữ Watcher được thiết lập bởi API Server
    private Watcher externalStatusWatcher; 

    public TimeTrackerClient(String userId) {
        this.userId = userId;
    }

    /**
     * Phương thức kết nối. Có thể truyền vào một Watcher (chỉ API Server mới làm).
     */
    public void connect(Watcher watcher) throws Exception {
        this.externalStatusWatcher = watcher; 
        logger.info("Client {} đang kết nối đến Zookeeper: {}", userId, ZK_ADDRESS);
        
        // Watcher mặc định là 'this' (TimeTrackerClient)
        this.zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, this); 
        connectionLatch.await();
        
        ensurePathExists(TEAM_STATUS_PATH);
        
        // Gắn Watcher của API Server vào ZNode gốc để nghe sự kiện Check-in/out
        if (externalStatusWatcher != null) {
            // Lần gọi đầu tiên để thiết lập Watcher
            zooKeeper.getChildren(TEAM_STATUS_PATH, externalStatusWatcher);
        }
    }

    // ⭐ Dành cho các Client Check-in/out console (không cần Watcher)
    public void connect() throws Exception {
        connect(null);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
                logger.info("Kết nối Zookeeper thành công.");
            } else if (event.getState() == Event.KeeperState.Expired) {
                logger.error("Phiên kết nối Zookeeper hết hạn!");
            }
        } 
        // Nếu Watcher được truyền vào (API Server), chuyển tiếp sự kiện
        else if (externalStatusWatcher != null) {
            try {
                externalStatusWatcher.process(event);
            } catch (Exception e) {
                logger.error("Lỗi khi xử lý Watcher ngoài: {}", e.getMessage());
            }
        }
    }

    private void ensurePathExists(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Đã tạo ZNode gốc: {}", path);
        }
    }

    public boolean checkIn() {
        if (zooKeeper == null) {
            logger.error("Kết nối Zookeeper chưa được thiết lập.");
            return false;
        }
        
        String userPath = TEAM_STATUS_PATH + "/" + userId;
        try {
            if (zooKeeper.exists(userPath, false) == null) {
                Map<String, String> data = new HashMap<>();
                data.put("startTime", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                
                String dataJson = new Gson().toJson(data);
                
                // ZNode EPHEMERAL sẽ bị xóa khi Client mất kết nối
                zooKeeper.create(userPath, dataJson.getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("✅ Client {} Check-in thành công.", userId);
                return true;
            } else {
                logger.warn("Client {} đã Check-in rồi.", userId);
                return false;
            }
        } catch (Exception e) {
            logger.error("Lỗi khi Check-in cho Client {}: {}", userId, e.getMessage());
            return false;
        }
    }

    public boolean checkOut() {
        if (zooKeeper == null) {
            logger.error("Kết nối Zookeeper chưa được thiết lập.");
            return false;
        }
        
        String userPath = TEAM_STATUS_PATH + "/" + userId;
        try {
            Stat stat = zooKeeper.exists(userPath, false);
            if (stat != null) {
                // Xóa ZNode
                zooKeeper.delete(userPath, -1);
                logger.info("❌ Client {} Check-out thành công.", userId);
                return true;
            } else {
                logger.warn("Client {} chưa Check-in hoặc đã Check-out trước đó.", userId);
                return false;
            }
        } catch (Exception e) {
            logger.error("Lỗi khi Check-out cho Client {}: {}", userId, e.getMessage());
            return false;
        }
    }
    
    // ⭐ PHƯƠNG THỨC MỚI: Lấy danh sách thành viên đang hoạt động (ZNode)
    public List<String> getActiveMembers() throws KeeperException, InterruptedException {
        if (zooKeeper == null) throw new IllegalStateException("Kết nối Zookeeper chưa được thiết lập.");
        // Gắn lại Watcher cho lần gọi này (vì Watcher chỉ kích hoạt 1 lần)
        return zooKeeper.getChildren(TEAM_STATUS_PATH, externalStatusWatcher); 
    }

    // ⭐ PHƯƠNG THỨC MỚI: Lấy dữ liệu trạng thái từ ZNode
    public byte[] getMemberData(String memberId) throws KeeperException, InterruptedException {
        if (zooKeeper == null) throw new IllegalStateException("Kết nối Zookeeper chưa được thiết lập.");
        String userPath = TEAM_STATUS_PATH + "/" + memberId;
        Stat stat = new Stat();
        // Không cần Watcher ở đây
        return zooKeeper.getData(userPath, false, stat); 
    }

    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
                logger.info("Đã đóng kết nối Zookeeper cho Client {}.", userId);
            } catch (InterruptedException e) {
                logger.error("Lỗi khi đóng kết nối Zookeeper: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }
}
