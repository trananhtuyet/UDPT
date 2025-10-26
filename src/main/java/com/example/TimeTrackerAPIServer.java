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

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import static spark.Spark.*;

public class TimeTrackerAPIServer {
    private static final Logger logger = LoggerFactory.getLogger(TimeTrackerAPIServer.class);
    private static final int API_PORT = 4567;
    private static final Gson gson = new Gson();

    // Default hourly rate for guests (VND per hour)
    private static final double DEFAULT_HOURLY_RATE = 50000.0;

    private static TimeTrackerClient zkClient;
    // SET để lưu trữ các WebSocket Session đang hoạt động
    private static final Set<Session> activeSessions = Collections.synchronizedSet(new HashSet<>()); 
    
    // Cấu trúc Dữ liệu Nhân viên Giả lập
    private static class EmployeeInfo {
        String id;
        String name;
        double hourlyRate;

        public EmployeeInfo(String id, String name, double hourlyRate) {
            this.id = id;
            this.name = name;
            this.hourlyRate = hourlyRate;
        }
    }
    private static final Map<String, EmployeeInfo> employeeDatabase = new HashMap<>();

    // Khởi tạo Database
    private static void initializeEmployeeDatabase() {
        employeeDatabase.put("TuanAnh", new EmployeeInfo("TuanAnh", "Nguyễn Tuấn Anh", 50000.0));
        employeeDatabase.put("MaiHoa", new EmployeeInfo("MaiHoa", "Trần Mai Hoa", 65000.0));
        employeeDatabase.put("VanNam", new EmployeeInfo("VanNam", "Lê Văn Nam", 45000.0));
        logger.info("Đã tải {} thông tin nhân viên.", employeeDatabase.size());
    }
    
    // WATCHER ĐỂ LẮNG NGHE SỰ KIỆN ZK
    private static final Watcher statusChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                logger.info(" ZNode thay đổi (Có người Check-in/out). Bắt đầu cập nhật Real-time...");
                try {
                    // Cố gắng kích hoạt lại Watcher
                    zkClient.getActiveMembers(this); 
                    //  GỌI PHƯƠNG THỨC GỬI DỮ LIỆU QUA WEBSOCKET
                    sendRealTimeStatusUpdate(); 
                } catch (Exception e) {
                    logger.error("Lỗi khi thiết lập lại Watcher: {}", e.getMessage());
                }
            } else if (event.getState() == Event.KeeperState.Expired) {
                // Xử lý khi session ZK hết hạn
                logger.warn("Kết nối Zookeeper hết hạn. Cố gắng kết nối lại...");
            }
        }
    };

    // PHƯƠNG THỨC TÍNH TOÁN VÀ TRẢ VỀ TRẠNG THÁI CHI TIẾT
    private static String getDetailedStatusJson() throws Exception {
        // Sử dụng Watcher để thiết lập lắng nghe khi đọc ZNode con
        List<String> memberIds = zkClient.getActiveMembers(statusChangeWatcher); 
        List<Map<String, Object>> detailedStatus = new ArrayList<>();

        for (String memberId : memberIds) {
            byte[] dataBytes = zkClient.getMemberData(memberId);
            String dataJson = new String(dataBytes, StandardCharsets.UTF_8);
            java.lang.reflect.Type mapType = new com.google.gson.reflect.TypeToken<java.util.Map<String, String>>(){}.getType();
            Map<String, String> dataMap = gson.fromJson(dataJson, mapType);
            String startTimeStr = dataMap.get("startTime");

            EmployeeInfo info = employeeDatabase.getOrDefault(memberId, new EmployeeInfo(memberId, "Khách (Guest)", DEFAULT_HOURLY_RATE));
            
            double hoursWorked = 0.0;
            double estimatedSalary = 0.0;
            double minutesWorked = 0.0;
            String checkInTime = "N/A";

            try {
                LocalDateTime startTime = LocalDateTime.parse(startTimeStr);
                LocalDateTime currentTime = LocalDateTime.now();
                
                Duration duration = Duration.between(startTime, currentTime);
                long totalSeconds = duration.getSeconds();
                minutesWorked = totalSeconds / 60.0; // fractional minutes

                // hours with two decimals
                hoursWorked = Math.round((minutesWorked / 60.0) * 100.0) / 100.0;

                // compute salary per minute to be more responsive
                double perMinuteRate = info.hourlyRate / 60.0;
                // round to whole VND
                estimatedSalary = Math.round(minutesWorked * perMinuteRate);

                checkInTime = startTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                
            } catch (Exception ignored) {}
            
            Map<String, Object> statusMap = new HashMap<>();
            statusMap.put("id", memberId);
            statusMap.put("name", info.name);
            statusMap.put("status", "ONLINE");
            statusMap.put("checkInTime", checkInTime); 
            statusMap.put("hoursWorked", hoursWorked);
            statusMap.put("minutesWorked", Math.round(minutesWorked * 100.0) / 100.0);
            statusMap.put("estimatedSalary", estimatedSalary);
            
            detailedStatus.add(statusMap);
        }
        return gson.toJson(detailedStatus);
    }

    //  PHƯƠNG THỨC GỬI DỮ LIỆU ĐẾN TẤT CẢ CLIENT QUA WEBSOCKET
    private static void sendRealTimeStatusUpdate() throws Exception {
        String statusJson = getDetailedStatusJson();
        // Log the payload so we can verify salary values being sent to clients
        logger.info("Broadcast payload: {}", statusJson);
        synchronized (activeSessions) {
            for (Session session : activeSessions) {
                if (session.isOpen()) {
                    session.getRemote().sendString(statusJson);
                }
            }
        }
    }

    // Scheduler to periodically broadcast status (helps update minutes/salary even if no ZK event)
    private static final java.util.concurrent.ScheduledExecutorService broadcaster =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "status-broadcaster");
                t.setDaemon(true);
                return t;
            });

    public static void main(String[] args) throws Exception {
        // 1. Khởi tạo Database và Zookeeper
        initializeEmployeeDatabase();
        zkClient = new TimeTrackerClient("API_SERVER_WATCHER");
        zkClient.connect(statusChangeWatcher); // TRUYỀN WATCHER VÀO KẾT NỐI

        // 2. Cấu hình Spark Server
        // 2. Cấu hình Spark Server
        port(API_PORT);
        staticFiles.location("/public"); // Nếu bạn có các file tĩnh
        
        // 3. WEBSOCKET ENDPOINT (phải được cấu hình trước các route)
        webSocket("/ws/status", StatusWebSocketHandler.class);

        // Start periodic broadcaster to update clients regularly (every 30 seconds)
        broadcaster.scheduleAtFixedRate(() -> {
            try {
                sendRealTimeStatusUpdate();
            } catch (Exception e) {
                logger.warn("Lỗi khi gửi broadcast định kỳ: {}", e.getMessage());
            }
        }, 5, 10, java.util.concurrent.TimeUnit.SECONDS);

        // Kích hoạt CORS và xử lý chung
        Spark.before((request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
            response.header("Access-Control-Allow-Headers", "Content-Type");
            response.type("application/json");
        });

        // Handle OPTIONS request
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
        

        // POST /api/checkin/:userId
        post("/api/checkin/:userId", "application/json", (request, response) -> {
            response.type("application/json");
            String userId = request.params(":userId");
            if (userId == null || userId.trim().isEmpty()) {
                response.status(400);
                return gson.toJson(Map.of("status", "error", "message", "Thiếu userId"));
            }

            try {
                TimeTrackerClient userClient = new TimeTrackerClient(userId);
                userClient.connect(statusChangeWatcher);
                if (userClient.checkIn()) {
                    sendRealTimeStatusUpdate(); // Push update sau khi Check-in thành công
                    return gson.toJson(Map.of("status", "success", "message", "Check-in thành công."));
                } else {
                    response.status(409); // Conflict
                    return gson.toJson(Map.of("status", "error", "message", "Bạn đã Check-in rồi."));
                }
            } catch (Exception e) {
                logger.error("Lỗi khi Check-in: {}", e.getMessage());
                response.status(500);
                return gson.toJson(Map.of("status", "error", "message", "Lỗi Server khi Check-in: " + e.getMessage()));
            }
        });

        // POST /api/checkout/:userId
        post("/api/checkout/:userId", "application/json", (request, response) -> {
            response.type("application/json");
            String userId = request.params(":userId");
            if (userId == null || userId.trim().isEmpty()) {
                response.status(400);
                return gson.toJson(Map.of("status", "error", "message", "Thiếu userId"));
            }

            try {
                zkClient = new TimeTrackerClient(userId);
                zkClient.connect(statusChangeWatcher);
                if (zkClient.checkOut()) {
                    sendRealTimeStatusUpdate(); // Push update sau khi Check-out thành công
                    return gson.toJson(Map.of("status", "success", "message", "Check-out thành công."));
                } else {
                    response.status(404); // Not Found
                    return gson.toJson(Map.of("status", "error", "message", "Bạn chưa Check-in hoặc đã Check-out trước đó."));
                }
            } catch (Exception e) {
                logger.error("Lỗi khi Check-out: {}", e.getMessage());
                response.status(500);
                return gson.toJson(Map.of("status", "error", "message", "Lỗi Server khi Check-out: " + e.getMessage()));
            }
        });

        // GET /api/admin/status (Dùng để lấy dữ liệu ban đầu/tính toán lương)
        get("/api/admin/status", "application/json", (request, response) -> {
            try {
                return getDetailedStatusJson();
            } catch (Exception e) {
                logger.error("Lỗi khi đọc trạng thái Admin: {}", e.getMessage());
                response.status(500);
                return gson.toJson(Map.of("status", "error", "message", "Lỗi server khi tính toán."));
            }
        });

        // Error handling
        exception(Exception.class, (e, request, response) -> {
            logger.error("Internal server error: {}", e.getMessage());
            response.status(500);
            response.body(gson.toJson(Map.of(
                "status", "error",
                "message", "Internal server error: " + e.getMessage()
            )));
        });

        // Handle 404 - Not Found
        notFound((request, response) -> {
            response.type("application/json");
            return gson.toJson(Map.of(
                "status", "error",
                "message", "Route not found"
            ));
        });
        
        // ENDPOINT CẤU HÌNH CONFIG (GET/POST /api/config/:key)
        post("/api/config/:key", "application/json", (request, response) -> {
            String key = request.params(":key");
            String value = request.body();
            try {
                zkClient.setConfigData(key, value);
                logger.info(" Cấu hình {} đã được cập nhật thành: {}", key, value);
                return gson.toJson(Map.of("status", "success", "message", "Cấu hình đã được cập nhật thành công."));
            } catch (Exception e) {
                response.status(500);
                return gson.toJson(Map.of("status", "error", "message", "Lỗi cập nhật cấu hình: " + e.getMessage()));
            }
        });
        
        get("/api/config/:key", "application/json", (request, response) -> {
            String key = request.params(":key");
            try {
                byte[] data = zkClient.getConfigData(key);
                String value = new String(data, StandardCharsets.UTF_8);
                return gson.toJson(Map.of("key", key, "value", value));
            } catch (KeeperException.NoNodeException e) {
                 response.status(404);
                 return gson.toJson(Map.of("status", "error", "message", "Key cấu hình không tồn tại."));
            } catch (Exception e) {
                response.status(500);
                return gson.toJson(Map.of("status", "error", "message", "Lỗi đọc cấu hình."));
            }
        });


        // 5. Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            zkClient.close();
            try {
                broadcaster.shutdownNow();
            } catch (Exception ignored) {}
        }));
    }
    
    // WEBSOCKET HANDLER ĐỂ QUẢN LÝ KẾT NỐI
    @WebSocket
    public static class StatusWebSocketHandler {
        @OnWebSocketConnect
        public void onConnect(Session userSession) {
            activeSessions.add(userSession);
            logger.info(" WebSocket mở: {} (Tổng: {})", userSession.getRemoteAddress(), activeSessions.size());
            try {
                // Gửi trạng thái hiện tại ngay khi kết nối
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
            // Không cần xử lý message từ Client trong ứng dụng này
        }
    }
}
