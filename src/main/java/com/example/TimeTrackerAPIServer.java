package com.example;

import com.google.gson.Gson;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;

import static spark.Spark.*;

public class TimeTrackerAPIServer {
    private static final Logger logger = LoggerFactory.getLogger(TimeTrackerAPIServer.class);
    private static final int API_PORT = 4567;
    private static final Gson gson = new Gson();

    private static final double DEFAULT_HOURLY_RATE = 50000.0; 

    private static TimeTrackerClient zkClient;
    // SET để lưu trữ các WebSocket Session đang hoạt động (Dashboard)
    private static final Set<Session> activeSessions = Collections.synchronizedSet(new HashSet<>()); 
    // Scheduler để gửi cập nhật trạng thái WebSocket
    private static final ScheduledExecutorService broadcaster = Executors.newSingleThreadScheduledExecutor();

    // ⭐ Cấu trúc Dữ liệu Nhân viên Giả lập (Database)
    private static final Map<String, EmployeeInfo> employeeDatabase = new HashMap<>();

    private static class EmployeeInfo {
        String id;
        String name;
        String department;
        
        public EmployeeInfo(String id, String name, String department) {
            this.id = id;
            this.name = name;
            this.department = department;
        }
    }

    private static void initializeDatabase() {
        // Thêm các nhân viên vào database (để luôn hiển thị trên Dashboard)
        employeeDatabase.put("TuanAnh", new EmployeeInfo("TuanAnh", "Trần Tuấn Anh", "Dev"));
        employeeDatabase.put("MaiHoa", new EmployeeInfo("MaiHoa", "Lê Mai Hoa", "Design"));
        employeeDatabase.put("QuocViet", new EmployeeInfo("QuocViet", "Nguyễn Quốc Việt", "Marketing"));
        employeeDatabase.put("XuanTruong", new EmployeeInfo("XuanTruong", "Phạm Xuân Trường", "Dev"));
        logger.info("Đã khởi tạo Database giả lập với {} nhân viên.", employeeDatabase.size());
    }

    // ⭐ WATCHER: Lắng nghe sự kiện thay đổi ZNode (Check-in/out)
    private static final Watcher statusChangeWatcher = event -> {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            logger.info("📣 Zookeeper Event: NodeChildrenChanged tại {}", event.getPath());
            // Gửi cập nhật Dashboard ngay lập tức
            broadcaster.submit(TimeTrackerAPIServer::broadcastDashboardUpdate); 
            
            try {
                // Lần gọi này tái gắn Watcher cho ZNode gốc
                zkClient.getActiveMembers(); 
            } catch (Exception e) {
                logger.error("Lỗi khi tái thiết lập Watcher: {}", e.getMessage());
            }
        }
    };

    private static void broadcastDashboardUpdate() {
        String statusJson = getDetailedStatusJson();
        if (statusJson == null || statusJson.contains("\"status\":\"error\"")) {
            return; 
        }

        synchronized (activeSessions) {
            activeSessions.removeIf(session -> {
                try {
                    if (session.isOpen()) {
                        session.getRemote().sendString(statusJson);
                        return false; 
                    }
                } catch (Exception e) {
                    logger.warn("Lỗi khi gửi cập nhật qua WS: {}", e.getMessage());
                }
                return true; 
            });
        }
        logger.info("✅ Đã gửi cập nhật trạng thái tới {} Dashboard.", activeSessions.size());
    }


    // ⭐ PHƯƠNG THỨC MỚI: Tạo JSON trạng thái chi tiết của TẤT CẢ nhân viên
    private static String getDetailedStatusJson() {
        List<Map<String, Object>> statusList = new ArrayList<>();
        
        try {
            // 1. Lấy danh sách ID đang ONLINE (có ZNode) từ ZooKeeper
            // Lệnh này cũng tái gắn Watcher
            List<String> activeMemberIds = zkClient.getActiveMembers();
            
            // 2. Duyệt qua TẤT CẢ nhân viên trong Database Giả lập
            for (EmployeeInfo info : employeeDatabase.values()) {
                Map<String, Object> memberStatus = new HashMap<>();
                
                // Kiểm tra chéo trạng thái
                boolean isOnline = activeMemberIds.contains(info.id);
                
                memberStatus.put("id", info.id);
                memberStatus.put("name", info.name);
                memberStatus.put("department", info.department);
                memberStatus.put("status", isOnline ? "ONLINE" : "OFFLINE"); // GÁN TRẠNG THÁI

                // Xử lý dữ liệu chi tiết
                if (isOnline) {
                    byte[] dataBytes = zkClient.getMemberData(info.id);
                    String dataJson = new String(dataBytes, StandardCharsets.UTF_8);

                    Map<String, String> dataMap = gson.fromJson(dataJson, Map.class);
                    String startTimeStr = dataMap.get("startTime"); 

                    memberStatus.put("checkInTime", startTimeStr);
                    
                    LocalDateTime startTime = LocalDateTime.parse(startTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    Duration duration = Duration.between(startTime, LocalDateTime.now());
                    long minutes = duration.toMinutes();
                    
                    memberStatus.put("minutesWorked", minutes);
                    memberStatus.put("estimatedSalary", (minutes / 60.0) * DEFAULT_HOURLY_RATE);
                } else {
                    // Nếu OFFLINE, gán giá trị mặc định/trống
                    memberStatus.put("checkInTime", "-");
                    memberStatus.put("minutesWorked", 0L);
                    memberStatus.put("estimatedSalary", 0.0);
                }

                statusList.add(memberStatus);
            }
            
            return gson.toJson(statusList);
            
        } catch (Exception e) {
            logger.error("Lỗi khi tạo JSON trạng thái chi tiết: {}", e.getMessage());
            return gson.toJson(Map.of("status", "error", "message", "Lỗi Server khi xử lý dữ liệu Zookeeper."));
        }
    }
    
    // ⭐ WEBSOCKET HANDLER ĐỂ QUẢN LÝ KẾT NỐI
    @WebSocket
    public static class StatusWebSocketHandler {
        @OnWebSocketConnect
        public void onConnect(Session userSession) {
            activeSessions.add(userSession);
            logger.info("➕ WebSocket mở: {} (Tổng: {})", userSession.getRemoteAddress(), activeSessions.size());
            try {
                // Gửi trạng thái ban đầu ngay khi kết nối
                userSession.getRemote().sendString(getDetailedStatusJson());
            } catch (Exception e) {
                logger.error("Lỗi gửi trạng thái ban đầu qua WS: {}", e.getMessage());
            }
        }

        @OnWebSocketClose
        public void onClose(Session userSession, int statusCode, String reason) {
            activeSessions.remove(userSession);
            logger.info("➖ WebSocket đóng: {} (Tổng: {})", userSession.getRemoteAddress(), activeSessions.size());
        }

        @OnWebSocketError
        public void onError(Session userSession, Throwable error) {
            logger.error("Lỗi WebSocket cho phiên {}: {}", userSession.getRemoteAddress(), error.getMessage());
        }
        
        @OnWebSocketMessage
        public void onMessage(Session userSession, String message) {
            // Không cần xử lý message
        }
    }

    public static void main(String[] args) {
        
        initializeDatabase();
        
        // 1. Khởi tạo và kết nối Zookeeper Client
        try {
            zkClient = new TimeTrackerClient("API_SERVER_WATCHER");
            // ⭐ TRUYỀN statusChangeWatcher VÀO KẾT NỐI
            zkClient.connect(statusChangeWatcher); 
        } catch (Exception e) {
            logger.error("Không thể kết nối Zookeeper: {}", e.getMessage());
            return; 
        }

        // 2. Cấu hình Spark Server
        port(API_PORT);
        
        // Cấu hình CORS (quan trọng cho việc gọi API từ HTML)
        options("/*", (request, response) -> {
            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }
            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if (accessControlRequestMethod != null) {
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }
            return "OK";
        });
        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));

        // WebSocket endpoint
        webSocket("/ws/status", StatusWebSocketHandler.class);
        
        // API Check-in
        post("/api/checkin/:id", (request, response) -> {
            String userId = request.params(":id");
            if (!employeeDatabase.containsKey(userId)) {
                response.status(404);
                return gson.toJson(Map.of("message", "ID không tồn tại trong hệ thống."));
            }
            
            TimeTrackerClient client = new TimeTrackerClient(userId);
            try {
                client.connect(); // Client Check-in/out không cần Watcher
                if (client.checkIn()) {
                    broadcaster.submit(TimeTrackerAPIServer::broadcastDashboardUpdate); 
                    response.status(200);
                    return gson.toJson(Map.of("message", "Check-in thành công."));
                } else {
                    response.status(409);
                    return gson.toJson(Map.of("message", "Bạn đã Check-in rồi."));
                }
            } catch (Exception e) {
                logger.error("Lỗi Check-in cho {}: {}", userId, e.getMessage());
                response.status(500);
                return gson.toJson(Map.of("message", "Lỗi Server Zookeeper."));
            } finally {
                client.close();
            }
        });
        
        // API Check-out
        post("/api/checkout/:id", (request, response) -> {
            String userId = request.params(":id");
            if (!employeeDatabase.containsKey(userId)) {
                response.status(404);
                return gson.toJson(Map.of("message", "ID không tồn tại trong hệ thống."));
            }
            
            TimeTrackerClient client = new TimeTrackerClient(userId);
            try {
                client.connect();
                if (client.checkOut()) {
                    broadcaster.submit(TimeTrackerAPIServer::broadcastDashboardUpdate); 
                    response.status(200);
                    return gson.toJson(Map.of("message", "Check-out thành công."));
                } else {
                    response.status(404);
                    return gson.toJson(Map.of("message", "Bạn chưa Check-in."));
                }
            } catch (Exception e) {
                logger.error("Lỗi Check-out cho {}: {}", userId, e.getMessage());
                response.status(500);
                return gson.toJson(Map.of("message", "Lỗi Server Zookeeper."));
            } finally {
                client.close();
            }
        });
        
        // API Lấy trạng thái thủ công (cho lần tải đầu tiên)
        get("/api/status", (request, response) -> {
            response.type("application/json");
            return getDetailedStatusJson();
        });


        // Thêm Shutdown Hook 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (zkClient != null) zkClient.close();
            broadcaster.shutdownNow();
            Spark.stop();
        }));
    }
}
