package org.example;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpHeaders;
import java.net.HttpURLConnection;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AssetMigrationBatch {
    // SharePoint credentials
    private static final String SHAREPOINT_SITE_NAME = "sanitarium_aprimo_asset";
    private static final String SP_TENANT_ID = "396f5882-803a-4ab2-a15c-cfaf37a12212";
    private static final String SP_CLIENT_ID = "a279d93b-83b5-4650-8df0-88ba3156a8c6";
    private static final String SP_CLIENT_SECRET = "FRo8Q~uvUIuqSN2A4kUd3dHUf.v3r.gP2e3CdcPT";
    private static final String SP_DRIVE_ID = "b!qb6egh9IQEqB1GG_7sQS3f9PMgEPlwFDphOxpzmIhmeEqwt8r7yaRr998YZOizIi";
    private static final String SHAREPOINT_SITE_ID = "dluxtechcorp.sharepoint.com,sites," + SHAREPOINT_SITE_NAME;
    // Aprimo credentials
    private static final String APRIMO_TOKEN_URL = "https://partnerdemo103.aprimo.com/login/connect/token";
    private static final String APRIMO_CLIENT_ID = "3FR7U1CD-3FR7";
    private static final String APRIMO_CLIENT_SECRET = "Vennilak@03";
    private static final String UPLOAD_BASE_URL = "https://partnerdemo103.aprimo.com/uploads";
    private static final String API_BASE_URL = "https://partnerdemo103.dam.aprimo.com";
    private static final String CLASSIFICATION_ID = "8515da41-a5f5-44bb-8c83-abc600c6a400";
    private static final long CHUNK_SIZE = 20 * 1024 * 1024; // 20 MB
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000; // 2 seconds between retries
    public static void main(String[] args) {
        // Clean leftover .azcopy logs from previous run
        cleanAzCopyLogs();

        final AtomicReference<File> csvRef = new AtomicReference<>();
        final AtomicReference<File> logFileRef = new AtomicReference<>();
        final AtomicReference<String> spTokenRef = new AtomicReference<>();
        final List<File> azCopyTempDirs = Collections.synchronizedList(new ArrayList<>());
        final List<Process> azCopyProcesses = Collections.synchronizedList(new ArrayList<>());
        final List<File> singleRowCsvFiles = Collections.synchronizedList(new ArrayList<>());

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            File csv = csvRef.get();
            File log = logFileRef.get();
            String spToken = spTokenRef.get();

            // Upload log to SharePoint
            if (log != null && log.exists() && spToken != null) {
                try {
                    uploadFileToSharePoint(spToken, log);
                    log.delete();
                    System.out.println("✅ Log file uploaded in shutdown hook.");
                } catch (Exception e) {
                    System.err.println("⚠️ Shutdown hook failed to upload log: " + e.getMessage());
                }
            }

            // Delete metadata CSV
            if (csv != null && csv.exists()) {
                if (csv.delete()) {
                    System.out.println("🧹 Deleted CSV file in shutdown hook.");
                } else {
                    System.err.println("⚠️ Failed to delete CSV file in shutdown hook.");
                }
            }

            // Kill any AzCopy processes
            // Kill any AzCopy processes
            for (Process proc : azCopyProcesses) {
                try {
                    proc.destroyForcibly();
                    proc.waitFor(5, TimeUnit.SECONDS);
                    System.out.println("🛑 Force killed AzCopy process.");
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to kill AzCopy process.");
                }
            }

// 🔄 Add sleep after killing processes
            try {
                Thread.sleep(1000); // Let OS unlock file handles
            } catch (InterruptedException ignored) {}

// Delete AzCopy temp folders
            for (File tempDir : azCopyTempDirs) {
                try {
                    deleteDirectoryRecursivelyWithRetry(tempDir, 3, 1000);
                    System.out.println("🧹 Deleted AzCopy temp dir: " + tempDir.getAbsolutePath());
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to delete AzCopy temp dir: " + tempDir.getAbsolutePath());
                }
            }

            // Delete single-row CSVs
// Delete single-row CSVs with retry and deleteOnExit fallback
            for (File file : singleRowCsvFiles) {
                boolean deleted = false;
                for (int i = 1; i <= 3 && !deleted; i++) {
                    try {
                        if (file.exists()) {
                            deleted = file.delete();
                            if (!deleted) Thread.sleep(1000); // small delay before retry
                        }
                    } catch (Exception e) {
                        System.err.println("⚠️ Error deleting single-row CSV file: " + e.getMessage());
                    }
                }

                if (deleted) {
                    System.out.println("🧹 Deleted single-row CSV file: " + file.getAbsolutePath());
                } else {
                    file.deleteOnExit(); // fallback for next JVM exit
                    System.err.println("⚠️ Failed to delete single-row CSV file (marked for deletion): " + file.getAbsolutePath());
                }
            }

            // Clean .azcopy folder
            try {
                cleanAzCopyLogs();
            } catch (Exception e) {
                System.err.println("⚠️ Failed to delete .azcopy folder: " + e.getMessage());
            }
        }));

        // Main workflow
        try {
            String spToken = executeWithRetry(AssetMigrationBatch::getSharePointToken, "getSharePointToken");
            spTokenRef.set(spToken);

            final String finalSpToken = spToken;
            String resolvedSiteId = executeWithRetry(() -> resolveSiteId(finalSpToken), "resolveSiteId");
            String aprimoToken = executeWithRetry(AssetMigrationBatch::getAprimoToken, "getAprimoToken");

            final String finalResolvedSiteId = resolvedSiteId;
            List<JsonNode> files = executeWithRetry(() -> listFilesInFolder(finalResolvedSiteId, finalSpToken), "listFilesInFolder");

            JsonNode csvFile = null;
            for (JsonNode file : files) {
                if (file.get("name").asText().endsWith(".csv")) {
                    csvFile = file;
                    break;
                }
            }

            if (csvFile != null) {
                String csvName = csvFile.get("name").asText();
                String downloadUrl = csvFile.get("@microsoft.graph.downloadUrl").asText();
                System.out.println("\n📄 CSV File Found: " + csvName);

                File localCsv = executeWithRetry(() -> downloadAsset(downloadUrl, finalSpToken), "downloadAsset");
                csvRef.set(localCsv);

                String logFileName = SHAREPOINT_SITE_NAME + "_log_file.xlsx";
                File logFile = new File(System.getProperty("java.io.tmpdir"), logFileName);
                logFileRef.set(logFile);

                processCsv(localCsv, finalSpToken, aprimoToken, azCopyTempDirs, azCopyProcesses, singleRowCsvFiles);
                return;
            }

            System.out.println("🔍 No CSV found in SharePoint, checking for local files...");

        } catch (Exception e) {
            System.err.println("❌ General Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static void processCsv(File localCsv, String spToken, String aprimoToken, List<File> azCopyTempDirs, List<Process> azCopyProcesses, List<File> singleRowCsvFiles) {
        boolean allUploadsSuccessful = true;
        String logFileName = SHAREPOINT_SITE_NAME + "_log_file.xlsx";
        File logFile = new File(System.getProperty("java.io.tmpdir"), logFileName);
        ExcelLogger logger = null;
        ExecutorService executor = Executors.newFixedThreadPool(3);
        List<Future<?>> futures = new ArrayList<>();

        AtomicInteger uploaded = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger totalFiles = new AtomicInteger(0);
        AtomicReference<String> currentUploadingFile = new AtomicReference<>("-");
        AtomicBoolean uploadStarted = new AtomicBoolean(false);

        ScheduledExecutorService uploadProgressExecutor = Executors.newSingleThreadScheduledExecutor();
        uploadProgressExecutor.scheduleAtFixedRate(() -> {
            if (!uploadStarted.get()) return;

            int doneCount = uploaded.get();
            int failCount = failed.get();
            int skipCount = skipped.get();
            int pendingCount = totalFiles.get() - (doneCount + failCount + skipCount);

            System.out.print(String.format(
                    "\r⬆️ Uploading [%s] %.2f%% | %d Done, %d Failed, %d Pending, %d Skipped, %d Total | %.4f Mb/s",
                    currentUploadingFile.get(),
                    ((doneCount + failCount + skipCount) * 100.0) / (totalFiles.get() == 0 ? 1 : totalFiles.get()),
                    doneCount, failCount, pendingCount, skipCount, totalFiles.get(), 0.0000
            ));
        }, 0, 2, TimeUnit.SECONDS);

        try {
            logger = new ExcelLogger(logFile.getAbsolutePath());

            CSVFormat format = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreSurroundingSpaces()
                    .withTrim()
                    .withDelimiter(',')
                    .withIgnoreEmptyLines();

            Map<String, Integer> filteredHeaders = new LinkedHashMap<>();
            try (Reader headerReader = new FileReader(localCsv);
                 CSVParser headerParser = new CSVParser(headerReader, format)) {
                for (Map.Entry<String, Integer> entry : headerParser.getHeaderMap().entrySet()) {
                    String key = entry.getKey();
                    if (key != null && !key.trim().isEmpty()) {
                        filteredHeaders.put(key.trim(), entry.getValue());
                    }
                }
            }

            try (Reader mainReader = new FileReader(localCsv);
                 CSVParser parser = new CSVParser(mainReader, format.withHeader(filteredHeaders.keySet().toArray(new String[0])))) {

                if (!filteredHeaders.containsKey("Folder_path")) {
                    throw new IllegalArgumentException("❌ Missing 'Folder_path' column in CSV.");
                }

                final List<String> originalHeaders = new ArrayList<>(filteredHeaders.keySet());

                for (CSVRecord record : parser) {
                    totalFiles.incrementAndGet();
                    final CSVRecord finalRecord = record;
                    final ExcelLogger logRef = logger;

                    futures.add(executor.submit(() -> {
                        String folderPath = finalRecord.get("Folder_path").trim();
                        final String fileNameFromCsv = finalRecord.isMapped("FileName") ? finalRecord.get("FileName").trim() : "-";

                        if (folderPath.isEmpty()) {
                            skipped.incrementAndGet();
                            return null;
                        }

                        File assetFile = null;
                        File singleCsv = null;
                        String downloadStatus = "Success", uploadStatus = "Failure", metadataStatus = "Not Applied", errorMessage = "-";
                        String downloadTime = "-", uploadTime = "-", recordId = "-";
                        String fileSizeStr = "0 MB";

                        try {
                            long startDownload = System.currentTimeMillis();
                            assetFile = executeWithRetry(() -> downloadAsset(folderPath, spToken), "downloadAsset");
                            long endDownload = System.currentTimeMillis();
                            downloadTime = ((endDownload - startDownload) / 1000) + "s";

                            if (assetFile != null) assetFile.deleteOnExit();
                            final File finalAssetFile = assetFile;
                            final String fileName = finalAssetFile != null ? finalAssetFile.getName() : fileNameFromCsv;

                            if (assetFile != null && assetFile.exists()) {
                                long sizeInBytes = assetFile.length();
                                fileSizeStr = String.format("%.2f MB", sizeInBytes / 1024.0 / 1024.0);
                            }

                            singleCsv = createSingleRowMetadataCsv(finalRecord, originalHeaders);
                            if (singleCsv != null) singleCsv.deleteOnExit();
                            if (singleCsv != null && singleCsv.exists()) singleRowCsvFiles.add(singleCsv);

                            final File finalCsv = singleCsv;
                            String metadataToken = executeWithRetry(() -> uploadCsvAndGetToken(finalCsv, aprimoToken), "uploadCsvAndGetToken");

                            currentUploadingFile.set(fileName);
                            uploadStarted.set(true);

                            long startUpload = System.currentTimeMillis();
                            boolean uploadSuccess = false;
                            int attempts = 0;

                            while (!uploadSuccess && attempts < 3) {
                                attempts++;
                                try {
                                    UploadInfo info = executeWithRetry(() -> startUploadSession(aprimoToken, finalAssetFile), "startUploadSession");

                                    if (finalAssetFile.length() > 20 * 1024 * 1024) {
                                        uploadSuccess = executeWithRetry(() -> uploadViaAzCopy(finalAssetFile, info.sasUrl, azCopyTempDirs, azCopyProcesses), "uploadViaAzCopy");
                                    } else {
                                        uploadSuccess = executeWithRetry(() -> uploadToAprimo(info.sasUrl, finalAssetFile), "uploadToAprimo");
                                    }

                                    if (!uploadSuccess) {
                                        System.out.println("⚠️ Upload failed on attempt " + attempts + ", retrying...");
                                        Thread.sleep(2000);
                                    }

                                    if (uploadSuccess) {
                                        uploaded.incrementAndGet();

                                        // ✅ Immediately delete file after successful upload
                                        if (finalAssetFile != null && finalAssetFile.exists()) {
                                            try {
                                                boolean deleted = finalAssetFile.delete();
                                                if (deleted) {
                                                    System.out.println("🧹 Deleted uploaded file: " + finalAssetFile.getName());
                                                } else {
                                                    System.err.println("⚠️ Failed to delete uploaded file: " + finalAssetFile.getAbsolutePath());
                                                }
                                            } catch (Exception ex) {
                                                System.err.println("❌ Error deleting uploaded file: " + ex.getMessage());
                                            }
                                        }
                                        recordId = executeWithRetry(() ->
                                                createAssetRecordAndReturnId(HttpClients.createDefault(), aprimoToken, info.token, metadataToken, finalAssetFile.getName()), "createAssetRecord");

                                        if (recordId != null && !recordId.equals("-")) {
                                            uploadStatus = "Success";
                                            metadataStatus = "Applied";
                                            System.out.println("\n✅ Uploaded & Created Asset: " + finalAssetFile.getName());
                                        }
                                    }

                                } catch (Exception e) {
                                    errorMessage = e.getMessage();
                                    Thread.sleep(2000);
                                }
                            }

                            long endUpload = System.currentTimeMillis();
                            uploadTime = ((endUpload - startUpload) / 1000) + "s";

                            if (!uploadSuccess) {
                                failed.incrementAndGet();
                            }

                        } catch (Exception e) {
                            failed.incrementAndGet();
                            errorMessage = e.getMessage();
                        } finally {
                            if (recordId != null && !recordId.equals("-")) {
                                try {
                                    if (singleCsv != null && singleCsv.exists() && !singleCsv.delete()) {
                                        System.err.println("⚠️ Failed to delete single CSV: " + singleCsv.getAbsolutePath());
                                    }

                                    if (assetFile != null && assetFile.exists() && !assetFile.delete()) {
                                        System.err.println("⚠️ Failed to delete file: " + assetFile.getAbsolutePath());
                                    }

                                    File parentDir = (assetFile != null) ? assetFile.getParentFile() : null;
                                    if (parentDir != null && parentDir.exists() && parentDir.isDirectory()) {
                                        File[] remaining = parentDir.listFiles();
                                        if (remaining != null && remaining.length == 0 && !parentDir.delete()) {
                                            System.err.println("⚠️ Failed to delete temp dir: " + parentDir.getAbsolutePath());
                                        }
                                    }
                                } catch (Exception ex) {
                                    System.err.println("⚠️ Cleanup exception: " + ex.getMessage());
                                }
                            }

                            synchronized (logRef) {
                                logRef.log(new LogEntry(
                                        (assetFile != null && assetFile.exists()) ? assetFile.getName() : fileNameFromCsv,
                                        folderPath,
                                        downloadStatus,
                                        uploadStatus,
                                        downloadTime,
                                        uploadTime,
                                        errorMessage,
                                        metadataStatus,
                                        recordId,
                                        fileSizeStr
                                ));
                            }
                        }
                        return null;
                    }));
                }
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    allUploadsSuccessful = false;
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            allUploadsSuccessful = false;
            e.printStackTrace();
        } finally {
            executor.shutdown();
            uploadProgressExecutor.shutdown();
            System.out.println();

            if (logger != null) {
                try {
                    logger.close();
                    uploadFileToSharePoint(spToken, logFile);
                    if (logFile.exists()) logFile.delete();
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to upload log: " + e.getMessage());
                }
            }

            if (localCsv != null && localCsv.exists()) {
                if (localCsv.delete()) {
                    System.out.println("🧹 Deleted metadata CSV: " + localCsv.getName());
                } else {
                    System.err.println("⚠️ Failed to delete metadata CSV: " + localCsv.getAbsolutePath());
                }
            }

            cleanAzCopyLogs();
        }
    }    private static void uploadFileToSharePoint(String spToken, File file) {
        try {
            if (file == null || !file.exists()) {
                System.err.println("❌ File not found for upload: " + (file != null ? file.getAbsolutePath() : "null"));
                return;
            }

            // URL-encode only the file name
            String encodedFileName = URLEncoder.encode(file.getName(), StandardCharsets.UTF_8);

            // This URL uploads directly into the library root (no extra “Shared Documents” segment)
            String uploadUrl = String.format(
                    "https://graph.microsoft.com/v1.0/drives/%s/root:/%s:/content",
                    SP_DRIVE_ID,
                    encodedFileName
            );

            HttpPut put = new HttpPut(uploadUrl);
            put.setHeader("Authorization", "Bearer " + spToken);
            put.setEntity(new FileEntity(file));

            try (CloseableHttpClient client = HttpClients.createDefault();
                 CloseableHttpResponse response = client.execute(put)) {

                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    System.out.println("✅ Log file uploaded to SharePoint: " + file.getName());
                } else {
                    System.err.println("❌ Failed to upload log (HTTP " + status + ")");
                    System.err.println("🔍 URL: " + uploadUrl);
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Unexpected error during SharePoint upload.");
            e.printStackTrace();
        }
    }
    private static class UploadInfo {
        String token, sasUrl;
        UploadInfo(String token, String sasUrl) {
            this.token = token;
            this.sasUrl = sasUrl;
        }
    }
    private static <T> T executeWithRetry(RetryableOperation<T> operation, String operationName) throws Exception {
        int attempt = 0;
        Exception lastException = null;
        long baseDelayMs = RETRY_DELAY_MS;

        while (attempt < MAX_RETRIES) {
            attempt++;
            try {
                System.out.printf("🔄 Attempt %d/%d for %s...%n", attempt, MAX_RETRIES, operationName);
                T result = operation.execute();
                if (attempt > 1) {
                    System.out.printf("✅ Attempt %d/%d succeeded for %s%n", attempt, MAX_RETRIES, operationName);
                }
                return result;

            } catch (Exception e) {
                lastException = e;
                System.err.printf("⚠️ Attempt %d/%d failed for %s: %s%n",
                        attempt, MAX_RETRIES, operationName, e.getMessage());

                // Check if this is a 429 error
                if (e instanceof IOException && e.getMessage().contains("429")) {

                    // Calculate exponential backoff (2^attempt * base delay)
                    long backoffTime = (long) (Math.pow(2, attempt) * baseDelayMs);
                    System.err.printf("⏳ 429 Too Many Requests detected. Waiting %d ms before retry...%n", backoffTime);

                    try {
                        Thread.sleep(backoffTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }

                } else  if (attempt < MAX_RETRIES) {
                    try {
                        System.out.printf("⏳ Waiting before next attempt (%d/%d)...%n", attempt+1, MAX_RETRIES);
                        Thread.sleep(baseDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        System.err.printf("❌ Operation %s failed after %d attempts. Skipping...%n", operationName, MAX_RETRIES);
        throw lastException != null ? lastException : new RuntimeException("Operation failed");
    }
    @FunctionalInterface
    private interface RetryableOperation<T> {
        T execute() throws Exception;
    }
    private static UploadInfo startUploadSession(String token, File file) throws IOException {
        String url = UPLOAD_BASE_URL;

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Authorization", "Bearer " + token);
            post.setHeader("API-VERSION", "1");
            post.setHeader("Content-Type", "application/json");
            final ObjectMapper mapper = new ObjectMapper();
            ObjectNode payload = mapper.createObjectNode();
            payload.put("filename", file.getName());
            payload.put("size", file.length());

            post.setEntity(new StringEntity(payload.toString(), ContentType.APPLICATION_JSON));

            try (CloseableHttpResponse resp = client.execute(post)) {
                JsonNode json = mapper.readTree(resp.getEntity().getContent());
                if (json.has("token") && json.has("sasUrl")) {
                    return new UploadInfo(json.get("token").asText(), json.get("sasUrl").asText());
                }
                return null;
            }
        }
    }
    private static String getSharePointToken() throws IOException {
        return spTokenManager.getValidToken();
    }
    private static boolean uploadToAprimo(String sasUrl, File file) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("azcopy", "copy", file.getAbsolutePath(), sasUrl, "--overwrite=true");
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process process = pb.start();
        int exitCode = process.waitFor();
        return exitCode == 0;
    }
    public static File createSingleRowMetadataCsv(CSVRecord record, List<String> headers) throws IOException {
        File tempCsv = File.createTempFile("single_asset_metadata_", ".csv");

        // Define known multi-value option list fields
        Set<String> multiValueFields = new HashSet<>(Arrays.asList(
                "Language", "AssetType", "Keywords", "Country", "Region"
        ));

        // ✅ Use encoding-safe writer with auto-close
        try (BufferedWriter writer = Files.newBufferedWriter(tempCsv.toPath(), StandardCharsets.UTF_8)) {
            if (!headers.contains("Folder_path")) {
                throw new IllegalArgumentException("CSV must have 'Folder_path' column.");
            }

            // Header row
            List<String> quotedHeaders = headers.stream()
                    .map(h -> "\"" + h + "\"")
                    .collect(Collectors.toList());
            writer.write(String.join(";", quotedHeaders));
            writer.newLine();

            // Data row
            List<String> row = new ArrayList<>();
            for (String header : headers) {
                String value = record.isMapped(header) ? record.get(header).trim() : "";

                if (multiValueFields.contains(header)) {
                    value = value.replace(";", ",").replace("|", ",");
                }

                value = "\"" + value.replace("\"", "\"\"") + "\"";
                row.add(value);
            }

            writer.write(String.join(";", row));
            writer.newLine();
        }
        try {
            Thread.sleep(2000); // Give OS time to release lock
        } catch (InterruptedException ignored) {}
        return tempCsv;
    }
    public static String createAssetRecordAndReturnId(CloseableHttpClient client, String aprimoToken, String uploadToken, String metadataToken, String fileName) throws IOException {
        String url = "https://partnerdemo103.dam.aprimo.com/api/core/records";
        ObjectMapper mapper = new ObjectMapper();

        HttpPost post = new HttpPost(url);
        post.setHeader("Authorization", "Bearer " + aprimoToken);
        post.setHeader("API-VERSION", "1");
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");

        ObjectNode root = mapper.createObjectNode();
        root.put("status", "draft");

        ObjectNode filesNode = root.putObject("files");
        filesNode.put("master", uploadToken);

        ObjectNode version = mapper.createObjectNode();
        version.put("id", uploadToken);
        version.put("fileName", fileName);
        version.put("versionLabel", "v1");
        version.put("comment", "Uploaded from SharePoint");

        ObjectNode versionWrap = mapper.createObjectNode();
        versionWrap.set("addOrUpdate", mapper.createArrayNode().add(version));

        ObjectNode fileWrap = mapper.createObjectNode();
        fileWrap.set("versions", versionWrap);

        filesNode.set("addOrUpdate", mapper.createArrayNode().add(fileWrap));

        if (metadataToken != null && !metadataToken.isEmpty()) {
            ObjectNode metadataNode = root.putObject("metadata");
            metadataNode.put("name", "UpdateAssetMetadata");
            metadataNode.put("token", metadataToken);
            metadataNode.put("metadataMatchField", "FileName");
        }

        post.setEntity(new StringEntity(mapper.writeValueAsString(root), ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = client.execute(post)) {
            int status = response.getStatusLine().getStatusCode();
            String responseBody = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                    .lines().reduce("", (a, b) -> a + b);

            if (status >= 200 && status < 300) {
                JsonNode responseJson = mapper.readTree(responseBody);
                String recordId = responseJson.has("id") ? responseJson.get("id").asText() : "-";
                System.out.println("✅ Asset created for: " + fileName + " | Record ID: " + recordId);
                return recordId;
            } else {
                throw new IOException("❌ Failed to create asset: " + status + " - " + responseBody);
            }
        }
    }
    public static String uploadCsvAndGetToken(File csvFile, String aprimoToken) throws IOException {
        String url = "https://partnerdemo103.aprimo.com/uploads";
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + aprimoToken);
            post.setHeader("API-VERSION", "1");

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addBinaryBody("file", csvFile, ContentType.DEFAULT_BINARY, csvFile.getName());
            post.setEntity(builder.build());

            try (CloseableHttpResponse response = client.execute(post)) {
                int status = response.getStatusLine().getStatusCode();
                String body = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                        .lines().reduce("", (a, b) -> a + b);

                System.out.println("🔍 Metadata Upload Response (" + status + "):");
                System.out.println(body);

                if (status >= 200 && status < 300) {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode json = mapper.readTree(body);
                    if (json.has("token")) {
                        return json.get("token").asText();
                    } else {
                        throw new IOException("❌ 'token' not found in response: " + json.toPrettyString());
                    }
                } else {
                    throw new IOException("❌ Metadata upload failed: " + status + " - " + body);
                }
            }
        }
    }
    private static File downloadAsset(String shareLink, String spToken) throws IOException, InterruptedException {
        if (shareLink == null || shareLink.isEmpty()) {
            throw new IOException("❌ Empty share link (Folder_path is blank)");
        }

        String encodedUrl = Base64.getUrlEncoder().encodeToString(shareLink.getBytes(StandardCharsets.UTF_8));
        String graphUrl = "https://graph.microsoft.com/v1.0/shares/u!" + encodedUrl + "/driveItem";

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(graphUrl);
            get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + spToken);
            get.setHeader(HttpHeaders.ACCEPT, "application/json");

            try (CloseableHttpResponse response = client.execute(get)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(response.getEntity().getContent());

                if (root.has("@microsoft.graph.downloadUrl") && root.has("size") && root.has("id") && root.has("name")) {
                    String fileId = root.get("id").asText();
                    String fileName = root.get("name").asText();
                    return downloadFileParallel(spToken, fileId, fileName);
                } else {
                    throw new IOException("❌ No download URL or size or ID found.");
                }
            }
        }
    }
    private static File downloadFileParallel(String accessToken, String fileId, String fileName) throws IOException, InterruptedException {
        // Get file metadata (size)
        String metaUrl = "https://graph.microsoft.com/v1.0/drives/" + SP_DRIVE_ID + "/items/" + fileId;
        HttpURLConnection metaConn = (HttpURLConnection) new URL(metaUrl).openConnection();
        metaConn.setRequestProperty("Authorization", "Bearer " + accessToken);
        metaConn.setRequestMethod("GET");
        JsonNode fileMeta = new ObjectMapper().readTree(metaConn.getInputStream());
        long fileSize = fileMeta.get("size").asLong();
        metaConn.disconnect();

        // Create unique temp directory
        File tempDir = Files.createTempDirectory("az_temp_").toFile();
        File file = new File(tempDir, fileName);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(fileSize);
        }

        int numThreads = 8;
        long chunkSize = fileSize / numThreads;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Boolean>> futures = new ArrayList<>();
        AtomicLong downloadedBytes = new AtomicLong(0);
        AtomicInteger done = new AtomicInteger(0), failed = new AtomicInteger(0), skipped = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        // Progress logger
        ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();
        progressExecutor.scheduleAtFixedRate(() -> {
            long downloaded = downloadedBytes.get();
            double percent = (downloaded * 100.0) / fileSize;
            double elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0;
            double speedMbps = (downloaded / 1024.0 / 1024.0) / (elapsedSec == 0 ? 1 : elapsedSec);
            int pending = numThreads - (done.get() + failed.get() + skipped.get());

            System.out.print(String.format(
                    "\r⬇️ Downloading [%s] %.2f%% | %d Done, %d Failed, %d Pending, %d Skipped, %d Total | %.4f Mb/s",
                    fileName, percent, done.get(), failed.get(), pending, skipped.get(), numThreads, speedMbps));
        }, 0, 2, TimeUnit.SECONDS);

        // Parallel chunk downloads
        for (int i = 0; i < numThreads; i++) {
            long start = i * chunkSize;
            long end = (i == numThreads - 1) ? fileSize - 1 : (start + chunkSize - 1);
            final int part = i;

            futures.add(executor.submit(() -> {
                int retry = 0;
                while (retry < 3) {
                    try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
                        HttpURLConnection conn = (HttpURLConnection) new URL("https://graph.microsoft.com/v1.0/drives/" + SP_DRIVE_ID + "/items/" + fileId + "/content").openConnection();
                        conn.setRequestProperty("Authorization", "Bearer " + accessToken);
                        conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
                        conn.connect();

                        try (InputStream in = conn.getInputStream()) {
                            out.seek(start);
                            byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
                            int bytesRead;
                            while ((bytesRead = in.read(buffer)) != -1) {
                                out.write(buffer, 0, bytesRead);
                                downloadedBytes.addAndGet(bytesRead);
                            }
                        }

                        done.incrementAndGet();
                        return true;

                    } catch (IOException e) {
                        retry++;
                        System.err.printf("\n❌ Chunk %d failed (try %d): %s\n", part, retry, e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                }

                failed.incrementAndGet();
                return false;
            }));
        }

        // Await completion
        try {
            for (Future<Boolean> future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            throw new IOException("❌ Parallel download failed", e);
        } finally {
            executor.shutdown();
            progressExecutor.shutdownNow();
        }

        System.out.println();
        System.out.println("✅ Parallel download completed: " + fileName);

        // Return file WITH reference to its tempDir for cleanup after upload
        file.deleteOnExit(); // in case something fails
        file.getParentFile().deleteOnExit(); // mark folder for deletion

        return file;
    }
    private static String resolveSiteId(String spToken) throws IOException {
        String hostname = "dluxtechcorp.sharepoint.com";
        String sitePath = "/sites/sanitarium_aprimo_asset";
        String url = String.format("https://graph.microsoft.com/v1.0/sites/%s:%s", hostname, sitePath);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(url);
            get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + spToken);
            get.setHeader(HttpHeaders.ACCEPT, "application/json");

            try (CloseableHttpResponse response = client.execute(get)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(response.getEntity().getContent());

                if (root.has("id")) {
                    System.out.println("✅ SharePoint Site ID: " + root.get("id").asText());
                    return root.get("id").asText();
                } else {
                    throw new RuntimeException("❌ Could not resolve SharePoint site ID: " + root.toPrettyString());
                }
            }
        }
    }
    private static List<JsonNode> listFilesInFolder(String siteId, String token) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        String url = String.format("https://graph.microsoft.com/v1.0/sites/%s/drives/%s/root/children",
                siteId,
                SP_DRIVE_ID);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(url);
            get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
            get.setHeader(HttpHeaders.ACCEPT, "application/json");

            try (CloseableHttpResponse response = client.execute(get)) {
                ObjectMapper mapper = new ObjectMapper();
                String rawJson = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                        .lines().reduce("", (acc, line) -> acc + line);

//                System.out.println("📄 SharePoint list response: " + rawJson);

                JsonNode root = mapper.readTree(rawJson);
                if (root.has("value")) {
                    for (JsonNode file : root.get("value")) {
                        result.add(file);
                    }
                } else {
                    throw new RuntimeException("❌ 'value' not found in SharePoint response. Full response: " + rawJson);
                }
            }
        }

        return result;
    }
    public static boolean uploadViaAzCopy(File assetFile, String sasUrl, List<File> tempDirs, List<Process> processes) throws IOException, InterruptedException {
        // Prepare temp folder for AzCopy
        File tempDir = Files.createTempDirectory("az_temp_").toFile();
        tempDirs.add(tempDir);

        File tempAsset = new File(tempDir, assetFile.getName());
        Files.copy(assetFile.toPath(), tempAsset.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // ✅ Check file size
        if (!tempAsset.exists() || tempAsset.length() == 0) {
            throw new IOException("❌ Temp asset is missing or empty: " + tempAsset.getAbsolutePath());
        }

        // Build AzCopy command
        String azCopyCommand = String.format("azcopy copy \"%s\" \"%s\" --overwrite=true", tempAsset.getAbsolutePath(), sasUrl);
        System.out.println("🛠️ Executing AzCopy command:\n" + azCopyCommand);

        // Detect OS
        String os = System.getProperty("os.name").toLowerCase();
        ProcessBuilder pb;
        if (os.contains("win")) {
            pb = new ProcessBuilder("cmd.exe", "/c", azCopyCommand);
        } else {
            pb = new ProcessBuilder("/bin/sh", "-c", azCopyCommand);
        }

        pb.redirectErrorStream(true);
        Process process = pb.start();
        processes.add(process);

        // Read output from AzCopy
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line); // Print AzCopy logs for debugging
            }
        }

        int exitCode = process.waitFor();
        System.out.println("🔚 AzCopy exited with code: " + exitCode);

        // Optionally: print AzCopy log file path
        String userHome = System.getProperty("user.home");
        File logDir = new File(userHome, ".azcopy");
        if (logDir.exists() && logDir.isDirectory()) {
            File[] logs = logDir.listFiles((dir, name) -> name.endsWith(".log"));
            if (logs != null && logs.length > 0) {
                Arrays.sort(logs, Comparator.comparingLong(File::lastModified).reversed());
                System.out.println("📄 Latest AzCopy log: " + logs[0].getAbsolutePath());
            }
        }

        return exitCode == 0;
    }
    private static void cleanAzCopyLogs() {
        File azCopyLogDir = new File(System.getProperty("user.home"), ".azcopy");
        if (azCopyLogDir.exists()) {
            boolean deleted = deleteDirectoryRecursivelyWithRetry(azCopyLogDir, 3, 1000);
            if (deleted) {
                System.out.println("🧼 Deleted .azcopy logs in shutdown hook.");
            } else {
                System.err.println("⚠️ Failed to delete some files in .azcopy folder.");
            }
        }
    }
    private static boolean deleteDirectoryRecursivelyWithRetry(File dir, int maxTries, long waitMillis) {
        for (int attempt = 1; attempt <= maxTries; attempt++) {
            if (tryDeleteDirectory(dir)) return true;

            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException ignored) {}
        }
        return false;
    }
    private static boolean tryDeleteDirectory(File dir) {
        if (!dir.exists()) return true;

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    tryDeleteDirectory(file);
                } else {
                    if (file.exists() && !file.delete()) {
                        System.err.println("⚠️ Failed to delete file (may be locked): " + file.getAbsolutePath());
                        file.deleteOnExit(); // retry on JVM exit
                    }
                }
            }
        }

        if (!dir.delete()) {
            if (dir.getName().equals(".azcopy")) {
                System.err.println("⚠️ .azcopy may be in use. Ignored.");
                dir.deleteOnExit(); // try later
                return true; // fake success
            } else {
                System.err.println("⚠️ Failed to delete directory: " + dir.getAbsolutePath());
                return false;
            }
        }

        return true;
    }
    private static class AprimoTokenManager {
        private String accessToken;
        private long expiryTimeMillis;

        private static final ObjectMapper mapper = new ObjectMapper();

        public synchronized String getValidToken() throws IOException {
            if (accessToken == null || System.currentTimeMillis() > expiryTimeMillis - 60_000) {
                refreshToken();
            }
            return accessToken;
        }

        private void refreshToken() throws IOException {
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(APRIMO_TOKEN_URL); // uses outer constant
                post.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");

                String body = "grant_type=client_credentials"
                        + "&client_id=" + URLEncoder.encode(APRIMO_CLIENT_ID, StandardCharsets.UTF_8.name())
                        + "&client_secret=" + URLEncoder.encode(APRIMO_CLIENT_SECRET, StandardCharsets.UTF_8.name())
                        + "&scope=api";

                post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

                try (CloseableHttpResponse response = client.execute(post)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != 200) {
                        throw new IOException("Failed to get Aprimo token: " + statusCode);
                    }

                    JsonNode json = mapper.readTree(response.getEntity().getContent());
                    accessToken = json.get("access_token").asText();
                    int expiresIn = json.get("expires_in").asInt();
                    expiryTimeMillis = System.currentTimeMillis() + (expiresIn * 1000L);

                    System.out.println("🔄 New Aprimo token acquired. Valid for " + expiresIn + " seconds.");
                }
            }
        }
    }
    private static final AprimoTokenManager tokenManager = new AprimoTokenManager();
    private static final SharePointTokenManager spTokenManager = new SharePointTokenManager();
    private static String getAprimoToken() throws IOException {
        return tokenManager.getValidToken();
    }
    private static class SharePointTokenManager {
        private String accessToken;
        private long expiryTimeMillis;
        private static final ObjectMapper mapper = new ObjectMapper();

        public synchronized String getValidToken() throws IOException {
            if (accessToken == null || System.currentTimeMillis() > expiryTimeMillis - 60_000) {
                refreshToken();
            }
            return accessToken;
        }

        private void refreshToken() throws IOException {
            String url = "https://login.microsoftonline.com/" + SP_TENANT_ID + "/oauth2/v2.0/token";

            String body = "grant_type=client_credentials"
                    + "&client_id=" + URLEncoder.encode(SP_CLIENT_ID, StandardCharsets.UTF_8.name())
                    + "&client_secret=" + URLEncoder.encode(SP_CLIENT_SECRET, StandardCharsets.UTF_8.name())
                    + "&scope=" + URLEncoder.encode("https://graph.microsoft.com/.default", StandardCharsets.UTF_8.name());

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
                post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

                try (CloseableHttpResponse response = client.execute(post)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    String jsonString = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                            .lines().reduce("", (acc, line) -> acc + line);

                    System.out.println("🔍 SharePoint Token Response: " + jsonString);

                    if (statusCode != 200) {
                        throw new IOException("❌ Failed to get SharePoint token. Status: " + statusCode + ", Body: " + jsonString);
                    }

                    JsonNode json = mapper.readTree(jsonString);
                    if (json.has("access_token")) {
                        accessToken = json.get("access_token").asText();
                        int expiresIn = json.get("expires_in").asInt();
                        expiryTimeMillis = System.currentTimeMillis() + (expiresIn * 1000L);
                        System.out.println("🔄 New SharePoint token acquired. Valid for " + expiresIn + " seconds.");
                    } else {
                        throw new IOException("❌ No access_token found in response: " + jsonString);
                    }
                }
            }
        }
    }
}
