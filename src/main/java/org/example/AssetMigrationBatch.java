package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.impl.classic.*;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.*;


import javax.swing.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AssetMigrationBatch {
    // Configuration properties
    private static Properties config;

    // Initialize these from config instead of hardcoding
    private static String SHAREPOINT_SITE_NAME;
    private static String SP_TENANT_ID;
    private static String SP_CLIENT_ID;
    private static String SP_CLIENT_SECRET;
    private static String SP_DRIVE_ID;
    private static String SHAREPOINT_SITE_ID;
    private static String APRIMO_TOKEN_URL;
    private static String APRIMO_CLIENT_ID;
    private static String APRIMO_CLIENT_SECRET;
    private static String UPLOAD_BASE_URL;
    private static String API_BASE_URL;
    private static String CLASSIFICATION_ID;
    private static long CHUNK_SIZE;
    private static int MAX_RETRIES;
    private static long RETRY_DELAY_MS;

    private static final AtomicInteger activeUploads = new AtomicInteger(0);
    private static final CountDownLatch completionLatch = new CountDownLatch(1);
    private static volatile long lastActivityTime = System.currentTimeMillis();
    private static final List<Future<?>> futures = Collections.synchronizedList(new ArrayList<>());
    private static ExecutorService executor;

    static {
        // Load configuration
        config = new Properties();
        try (InputStream input = AssetMigrationBatch.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new FileNotFoundException("config.properties file not found in classpath");
            }
            config.load(input);

            // Initialize all configuration values
            SHAREPOINT_SITE_NAME = config.getProperty("sharepoint.site.name");
            SP_TENANT_ID = config.getProperty("sharepoint.tenant.id");
            SP_CLIENT_ID = config.getProperty("sharepoint.client.id");
            SP_CLIENT_SECRET = config.getProperty("sharepoint.client.secret");
            SP_DRIVE_ID = config.getProperty("sharepoint.drive.id");
            SHAREPOINT_SITE_ID = "dluxtechcorp.sharepoint.com,sites," + SHAREPOINT_SITE_NAME;

            APRIMO_TOKEN_URL = config.getProperty("aprimo.token.url");
            APRIMO_CLIENT_ID = config.getProperty("aprimo.client.id");
            APRIMO_CLIENT_SECRET = config.getProperty("aprimo.client.secret");
            UPLOAD_BASE_URL = config.getProperty("aprimo.upload.url");
            API_BASE_URL = config.getProperty("aprimo.api.url");
            CLASSIFICATION_ID = config.getProperty("aprimo.classification.id");

            CHUNK_SIZE = Long.parseLong(config.getProperty("chunk.size.mb")) * 1024 * 1024;
            MAX_RETRIES = Integer.parseInt(config.getProperty("max.retries"));
            RETRY_DELAY_MS = Long.parseLong(config.getProperty("retry.delay.ms"));

        } catch (Exception e) {
            System.err.println("❌ Failed to load configuration: " + e.getMessage());
            throw new RuntimeException("Configuration loading failed", e);
        }
    }

    public static void main(String[] args) {
        final AtomicReference<File> csvRef = new AtomicReference<>();
        final AtomicReference<File> logFileRef = new AtomicReference<>();
        final List<File> azCopyTempDirs = Collections.synchronizedList(new ArrayList<>());
        final List<Process> azCopyProcesses = Collections.synchronizedList(new ArrayList<>());
        final List<File> singleRowCsvFiles = Collections.synchronizedList(new ArrayList<>());
        SharePointTokenManager spTokenManager = new SharePointTokenManager();
        AprimoTokenManager aprimoTokenManager = new AprimoTokenManager();
        String dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Process proc : azCopyProcesses) {
                try {
                    proc.destroyForcibly();
                    proc.waitFor(5, TimeUnit.SECONDS);
                    System.out.println("🛑 Force killed AzCopy process.");
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to kill AzCopy process.");
                }
            }

            for (File tempDir : azCopyTempDirs) {
                try {
                    deleteDirectoryRecursivelyWithRetry(tempDir, 3, 1000);
                    System.out.println("🧹 Deleted AzCopy temp dir: " + tempDir.getAbsolutePath());
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to delete AzCopy temp dir: " + tempDir.getAbsolutePath());
                }
            }

            for (File file : singleRowCsvFiles) {
                boolean deleted = false;
                for (int i = 1; i <= 3 && !deleted; i++) {
                    try {
                        if (file.exists()) {
                            deleted = file.delete();
                            if (!deleted) Thread.sleep(1000);
                        }
                    } catch (Exception e) {
                        System.err.println("⚠️ Error deleting single-row CSV file: " + e.getMessage());
                    }
                }

                if (!deleted) {
                    file.deleteOnExit();
                    System.err.println("⚠️ Failed to delete single-row CSV file (marked for deletion): " + file.getAbsolutePath());
                }
            }

            try {
                cleanAzCopyLogs();
            } catch (Exception e) {
                System.err.println("⚠️ Failed to clean AzCopy logs: " + e.getMessage());
            }
        }));

        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("📄 Enter the full path to the metadata CSV file, or press Enter to choose a file:");
            String input = scanner.nextLine().trim();
            File localCsv;
            if (input.isEmpty()) {
                JFileChooser chooser = new JFileChooser();
                chooser.setDialogTitle("Select Metadata CSV File");
                chooser.setMultiSelectionEnabled(false);
                int result = chooser.showOpenDialog(null);
                if (result == JFileChooser.APPROVE_OPTION) {
                    localCsv = chooser.getSelectedFile();
                } else {
                    System.out.println("❌ No file selected. Exiting.");
                    return;
                }
            } else {
                String cleanedPath = input.replaceAll("^['\"]|['\"]$", "").replace("\\", "/");
                localCsv = new File(cleanedPath);
                if (!localCsv.exists() || !localCsv.isFile()) {
                    System.err.println("❌ Metadata CSV not found or invalid: " + cleanedPath);
                    return;
                }
            }

            csvRef.set(localCsv);
            System.out.println("📄 Using CSV File: " + localCsv.getName());

            String todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String logFileName = SHAREPOINT_SITE_NAME + "_log_file_" + todayDate + ".xlsx";
            File logFile = new File(localCsv.getParentFile(), logFileName);
            logFileRef.set(logFile);

            processCsv(localCsv, azCopyTempDirs, azCopyProcesses, singleRowCsvFiles, spTokenManager, aprimoTokenManager);

        } catch (Exception e) {
            System.err.println("❌ General Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processCsv(File localCsv, List<File> azCopyTempDirs, List<Process> azCopyProcesses,
                                   List<File> singleRowCsvFiles, SharePointTokenManager spTokenManager,
                                   AprimoTokenManager aprimoTokenManager) {
        boolean allUploadsSuccessful = true;
        String todayDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String logFileName = SHAREPOINT_SITE_NAME + "_log_file_" + todayDate + ".xlsx";
        File logFile = new File(localCsv.getParentFile(), logFileName);
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
            int done = uploaded.get();
            int fail = failed.get();
            int skip = skipped.get();
            int pending = totalFiles.get() - (done + fail + skip);
            System.out.print(String.format(
                    "\r⬆️ Uploading [%s] %.2f%% | %d Done, %d Failed, %d Pending (%d Skipped), %d Total | %.4f Mb/s",
                    currentUploadingFile.get(),
                    ((done + fail) * 100.0) / (totalFiles.get() == 0 ? 1 : totalFiles.get()),
                    done, fail, pending, skip, totalFiles.get(), 0.0000
            ));
        }, 0, 2, TimeUnit.SECONDS);

        File convertedCsv = null;

        try {
            if (localCsv.getName().toLowerCase().endsWith(".xlsx")) {
                convertedCsv = convertExcelToCsv(localCsv);
                localCsv = convertedCsv;
            }

            logger = new ExcelLogger(logFile.getAbsolutePath());
            List<CSVRecord> records = new ArrayList<>();
            Set<String> headers = new HashSet<>();

            try (Reader reader = new FileReader(localCsv);
                 CSVParser parser = CSVFormat.DEFAULT
                         .withFirstRecordAsHeader()
                         .withIgnoreSurroundingSpaces()
                         .withTrim()
                         .withDelimiter(',')
                         .withIgnoreEmptyLines()
                         .parse(reader)) {

                headers.addAll(parser.getHeaderMap().keySet());
                for (CSVRecord record : parser) {
                    records.add(record);
                }
            }

            if (!(headers.contains("Folder_path") && headers.contains("FileName"))) {
                throw new IllegalArgumentException("❌ Missing mandatory 'Folder_path' or 'FileName' column.");
            }

            final ExcelLogger finalLogger = logger;
            final SharePointTokenManager finalSpTokenManager = spTokenManager;
            final AprimoTokenManager finalAprimoTokenManager = aprimoTokenManager;
            final List<File> finalSingleRowCsvFiles = singleRowCsvFiles;
            final List<Process> finalAzCopyProcesses = azCopyProcesses;

            for (CSVRecord record : records) {
                totalFiles.incrementAndGet();
                final CSVRecord currentRecord = record;

                futures.add(executor.submit(() -> {
                    String folderPath = currentRecord.get("Folder_path").trim();
                    String fileNameFromCsv = currentRecord.get("FileName").trim();

                    if (folderPath.isEmpty() && fileNameFromCsv.isEmpty()) {
                        skipped.incrementAndGet();
                        synchronized (finalLogger) {
                            finalLogger.log(new LogEntry(
                                    fileNameFromCsv,     // 1
                                    folderPath,          // 2
                                    "-",                 // swatchPdf
                                    "-",                 // printSpecPdf
                                    "Skipped",           // downloadStatus
                                    "Not Attempted",     // uploadStatus
                                    "-",                 // downloadTime
                                    "-",                 // uploadTime
                                    "Empty Folder_path", // errorMessage
                                    "Not Applied",       // metadataStatus
                                    "-",                 // recordId
                                    "0 MB"               // fileSize
                            ));
                        }
                        return null;
                    }

                    File assetFile = null;
                    File singleCsv = null;
                    String downloadStatus = "Success", uploadStatus = "Failure", metadataStatus = "Not Applied", errorMessage = "-";
                    String downloadTime = "-", uploadTime = "-";
                    AtomicReference<String> recordId = new AtomicReference<>("-");
                    String fileSizeStr = "0 MB";

                    try {
                        long startDownload = System.currentTimeMillis();
                        assetFile = executeWithRetry(() -> downloadAsset(folderPath, finalSpTokenManager.getValidToken()), "downloadAsset");
                        long endDownload = System.currentTimeMillis();
                        downloadTime = ((endDownload - startDownload) / 1000) + "s";

                        if (assetFile != null) assetFile.deleteOnExit();
                        if (assetFile != null && assetFile.exists()) {
                            long sizeInBytes = assetFile.length();
                            fileSizeStr = String.format("%.2f MB", sizeInBytes / 1024.0 / 1024.0);
                        }

                        final File finalAssetFile = assetFile;
                        final String fileName = finalAssetFile != null ? finalAssetFile.getName() : fileNameFromCsv;

                        singleCsv = createSingleRowMetadataCsv(currentRecord);
                        if (singleCsv != null) {
                            singleCsv.deleteOnExit();
                            if (singleCsv.exists()) finalSingleRowCsvFiles.add(singleCsv);
                        }

                        final File finalCsv = singleCsv;
                        final String aprimoTokenFinal = finalAprimoTokenManager.getValidToken();
                        String metadataToken = executeWithRetry(() -> uploadCsvAndGetToken(finalCsv, aprimoTokenFinal), "uploadCsvAndGetToken");

                        currentUploadingFile.set(fileName);
                        uploadStarted.set(true);
                        long startUpload = System.currentTimeMillis();
                        boolean uploadSuccess = false;
                        int attempts = 0;

                        while (!uploadSuccess && attempts < 3) {
                            attempts++;
                            try {
                                UploadInfo uploadInfo = executeWithRetry(() -> startUploadSession(aprimoTokenFinal, finalAssetFile), "startUploadSession");

                                if (finalAssetFile.length() > 20 * 1024 * 1024) {
                                    uploadSuccess = executeWithRetry(() -> uploadViaAzCopy(finalAssetFile, uploadInfo.sasUrl, finalAzCopyProcesses), "uploadViaAzCopy");
                                } else {
                                    uploadSuccess = executeWithRetry(() -> uploadToAprimo(uploadInfo.sasUrl, finalAssetFile), "uploadToAprimo");
                                }

                                if (!uploadSuccess) {
                                    System.out.println("⚠️ Upload failed on attempt " + attempts + ", retrying...");
                                    Thread.sleep(5000);
                                }

                                if (uploadSuccess) {
                                    uploaded.incrementAndGet();
                                    if (finalAssetFile != null && finalAssetFile.exists()) finalAssetFile.delete();

                                    String id = executeWithRetry(() -> createAssetRecordAndReturnId(
                                            HttpClients.createDefault(), aprimoTokenFinal, uploadInfo.token, metadataToken, finalAssetFile.getName()
                                    ), "createAssetRecord");

                                    recordId.set(id);
                                    if (id != null && !id.equals("-")) {
                                        uploadStatus = "Success";
                                        metadataStatus = "Applied";
                                        System.out.println("\n✅ Uploaded & Created Asset: " + finalAssetFile.getName());
                                    }
                                }
                            } catch (Exception e) {
                                errorMessage = e.getMessage();
                                Thread.sleep(5000);
                            }
                        }

                        long endUpload = System.currentTimeMillis();
                        uploadTime = ((endUpload - startUpload) / 1000) + "s";
                        if (!uploadSuccess) failed.incrementAndGet();

                    } catch (Exception e) {
                        failed.incrementAndGet();
                        errorMessage = e.getMessage();
                    } finally {
                        synchronized (finalLogger) {
                            finalLogger.log(new LogEntry(
                                    (assetFile != null && assetFile.exists()) ? assetFile.getName() : fileNameFromCsv,
                                    folderPath,
                                    "-",                 // swatchPdf
                                    "-",                 // printSpecPdf
                                    downloadStatus,
                                    uploadStatus,
                                    downloadTime,
                                    uploadTime,
                                    errorMessage,
                                    metadataStatus,
                                    recordId.get(),
                                    fileSizeStr
                            ));
                        }
                    }
                    return null;
                }));
            }

            executor.shutdown();
            if (!executor.awaitTermination(2, TimeUnit.HOURS)) {
                executor.shutdownNow();
                System.err.println("⚠️ Forced shutdown after timeout");
            }

            System.out.println(uploaded.get() + " uploaded, " + failed.get() + " failed, " + skipped.get() + " skipped");

        } catch (Exception e) {
            allUploadsSuccessful = false;
            e.printStackTrace();
        } finally {
            executor.shutdown();
            uploadProgressExecutor.shutdown();
            if (logger != null) {
                try {
                    logger.close();
                    System.out.println("📁 Log file saved locally at: " + logFile.getAbsolutePath());
                } catch (Exception e) {
                    System.err.println("⚠️ Failed to upload log: " + e.getMessage());
                }
            }
            if (convertedCsv != null && convertedCsv.exists()) {
                convertedCsv.delete();
            }
            cleanAzCopyLogs();
        }
    }

    public static File convertExcelToCsv(File xlsxFile) throws IOException {
        File csvFile = File.createTempFile("converted_", ".csv");

        try (FileInputStream fis = new FileInputStream(xlsxFile);
             Workbook workbook = new XSSFWorkbook(fis);
             FileWriter fw = new FileWriter(csvFile);
             CSVPrinter printer = new CSVPrinter(fw, CSVFormat.DEFAULT)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (Row row : sheet) {
                List<String> cells = new ArrayList<>();
                for (Cell cell : row) {
                    switch (cell.getCellType()) {
                        case STRING:
                            cells.add(cell.getStringCellValue().trim());
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                LocalDate date = cell.getLocalDateTimeCellValue().toLocalDate();
                                cells.add(date.format(DateTimeFormatter.ofPattern("M/d/yyyy")));
                            } else {
                                cells.add(String.valueOf(cell.getNumericCellValue()));
                            }
                            break;
                        case BOOLEAN:
                            cells.add(String.valueOf(cell.getBooleanCellValue()));
                            break;
                        case FORMULA:
                            try {
                                if (DateUtil.isCellDateFormatted(cell)) {
                                    LocalDate date = cell.getLocalDateTimeCellValue().toLocalDate();
                                    cells.add(date.format(DateTimeFormatter.ofPattern("M/d/yyyy")));
                                } else {
                                    cells.add(cell.getStringCellValue().trim());
                                }
                            } catch (Exception e) {
                                cells.add(cell.toString().trim());
                            }
                            break;
                        case BLANK:
                            cells.add("");
                            break;
                        default:
                            cells.add(cell.toString().trim());
                    }
                }
                printer.printRecord(cells);
            }
        }

        return csvFile;
    }

    public static String formatToMdyyyy(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) return dateStr;

        List<DateTimeFormatter> formats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy"),
                DateTimeFormatter.ofPattern("M/d/yyyy")
        );

        for (DateTimeFormatter fmt : formats) {
            try {
                LocalDate date = LocalDate.parse(dateStr, fmt);
                return date.format(DateTimeFormatter.ofPattern("M/d/yyyy"));
            } catch (DateTimeParseException ignored) {}
        }

        return dateStr; // fallback if unrecognized
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
                } else if (attempt < MAX_RETRIES) {
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

    private static boolean uploadToAprimo(String sasUrl, File file) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("azcopy", "copy", file.getAbsolutePath(), sasUrl, "--overwrite=true");
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process process = pb.start();
        int exitCode = process.waitFor();
        return exitCode == 0;
    }

    public static File createSingleRowMetadataCsv(CSVRecord record) throws IOException {
        File tempCsv = File.createTempFile("single_asset_metadata_", ".csv");

        // Define known multi-value option list fields
        Set<String> multiValueFields = new HashSet<>(Arrays.asList(
                "Language", "AssetType", "Keywords", "Country", "Region"
        ));

        // Dynamically extract headers from the record
        List<String> headers = new ArrayList<>(record.toMap().keySet());
        if (!headers.contains("Folder_path")) {
            throw new IllegalArgumentException("CSV must have 'Folder_path' column.");
        }

        try (BufferedWriter writer = Files.newBufferedWriter(tempCsv.toPath(), StandardCharsets.UTF_8)) {
            // Write header row
            List<String> quotedHeaders = headers.stream()
                    .map(h -> "\"" + h + "\"")
                    .collect(Collectors.toList());
            writer.write(String.join(";", quotedHeaders));
            writer.newLine();

            // Write data row
            List<String> row = new ArrayList<>();
            for (String header : headers) {
                String value = record.isMapped(header) ? record.get(header).trim() : "";

                // Handle multi-value fields (convert ; and | to comma)
                if (multiValueFields.contains(header)) {
                    value = value.replace(";", ",").replace("|", ",");
                }

                // Handle Expiration Date format
                if ("Expiration Date".equalsIgnoreCase(header)) {
                    value = formatToMdyyyy(value); // apply format conversion
                }

                value = "\"" + value.replace("\"", "\"\"") + "\"";  // escape quotes
                row.add(value);
            }

            writer.write(String.join(";", row));
            writer.newLine();
        }

        try {
            Thread.sleep(5000); // allow OS to release file handle
        } catch (InterruptedException ignored) {}

        return tempCsv;
    }

    public static String createAssetRecordAndReturnId(CloseableHttpClient client, String aprimoToken, String uploadToken, String metadataToken, String fileName) throws IOException {
        String url = API_BASE_URL + "/api/core/records";
        ObjectMapper mapper = new ObjectMapper();

        // Prepare request body
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

        String requestBody = mapper.writeValueAsString(root);

        for (int attempt = 1; attempt <= 2; attempt++) {
            String tokenToUse = (attempt == 1) ? aprimoToken : tokenManager.getValidToken(); // refresh token on 2nd attempt
            HttpPost post = new HttpPost(url);
            post.setHeader("Authorization", "Bearer " + tokenToUse);
            post.setHeader("API-VERSION", "1");
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept", "application/json");
            post.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

            try (CloseableHttpResponse response = client.execute(post)) {
                int status = response.getStatusLine().getStatusCode();
                String responseBody = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                        .lines().collect(Collectors.joining());

                if (status == 401 && attempt == 1) {
                    System.err.println("⚠️ 401 Unauthorized. Retrying after token refresh...");
                    continue;
                }

                if (status >= 200 && status < 300) {
                    JsonNode json = mapper.readTree(responseBody);
                    String recordId = json.has("id") ? json.get("id").asText() : "-";
                    System.out.println("✅ Asset created for: " + fileName + " | Record ID: " + recordId);
                    return recordId;
                } else {
                    throw new IOException("❌ Failed to create asset: " + status + " - " + responseBody);
                }
            }
        }

        throw new IOException("❌ Failed to create asset record after retry for: " + fileName);
    }

    public static String uploadCsvAndGetToken(File csvFile, String aprimoToken) throws IOException {
        String url = API_BASE_URL + "/uploads";
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
        // 1. Get file metadata
        String metaUrl = "https://graph.microsoft.com/v1.0/drives/" + SP_DRIVE_ID + "/items/" + fileId + "?select=@microsoft.graph.downloadUrl,size";
        HttpURLConnection metaConn = (HttpURLConnection) new URL(metaUrl).openConnection();
        metaConn.setRequestProperty("Authorization", "Bearer " + accessToken);
        metaConn.setRequestMethod("GET");
        JsonNode fileMeta = new ObjectMapper().readTree(metaConn.getInputStream());
        String downloadUrl = fileMeta.get("@microsoft.graph.downloadUrl").asText();
        long fileSize = fileMeta.get("size").asLong();
        metaConn.disconnect();

        // 2. Handle small files
        if (fileSize < 5 * 1024 * 1024) {
            System.out.println("ℹ️ File is small, using single-threaded download");

            File smallFile = new File(Files.createTempDirectory("az_temp_").toFile(), fileName);
            HttpURLConnection conn = (HttpURLConnection) new URL(downloadUrl).openConnection();

            try (InputStream in = conn.getInputStream();
                 FileOutputStream out = new FileOutputStream(smallFile)) {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
            smallFile.deleteOnExit();
            smallFile.getParentFile().deleteOnExit();
            return smallFile;
        }

        // 3. Handle large files with parallel download
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
                activeUploads.incrementAndGet();
                lastActivityTime = System.currentTimeMillis();
                try {
                    int retry = 0;
                    while (retry < 3) {
                        try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
                            HttpURLConnection conn = (HttpURLConnection) new URL(downloadUrl).openConnection();
                            conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
                            conn.connect();
                            try (InputStream in = conn.getInputStream()) {
                                out.seek(start);
                                byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
                                int bytesRead;
                                while ((bytesRead = in.read(buffer)) != -1) {
                                    out.write(buffer, 0, bytesRead);
                                    downloadedBytes.addAndGet(bytesRead);
                                    lastActivityTime = System.currentTimeMillis(); // Update activity on each write
                                }
                            }

                            done.incrementAndGet();
                            return true;

                        } catch (IOException e) {
                            retry++;
                            System.err.printf("\n❌ Chunk %d failed (try %d): %s\n", part, retry, e.getMessage());
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return false;
                            }
                        }
                    }

                    failed.incrementAndGet();
                    return false;
                } finally {
                    lastActivityTime = System.currentTimeMillis();
                    if (activeUploads.decrementAndGet() == 0) {
                        completionLatch.countDown();
                    }
                }
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

    public static boolean uploadViaAzCopy(File assetFile, String sasUrl, List<Process> processes) throws IOException, InterruptedException {
        if (!assetFile.exists() || assetFile.length() == 0) {
            throw new IOException("❌ Asset is missing or empty: " + assetFile.getAbsolutePath());
        }
        // Build AzCopy command
        String azCopyCommand = String.format("azcopy copy \"%s\" \"%s\" --overwrite=true", assetFile.getAbsolutePath(), sasUrl);
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

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        int exitCode = process.waitFor();
        System.out.println("🔚 AzCopy exited with code: " + exitCode);
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
                HttpPost post = new HttpPost(APRIMO_TOKEN_URL);
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

    private static boolean hasActiveUploads() {
        for (Future<?> future : futures) {
            if (!future.isDone()) {
                return true;
            }
        }
        return false;
    }

    // Supporting classes
    private static class LogEntry {
        private final String fileName;
        private final String folderPath;
        private final String swatchPdf;
        private final String printSpecPdf;
        private final String downloadStatus;
        private final String uploadStatus;
        private final String downloadTime;
        private final String uploadTime;
        private final String errorMessage;
        private final String metadataStatus;
        private final String recordId;
        private final String fileSize;

        public LogEntry(String fileName, String folderPath, String swatchPdf, String printSpecPdf,
                        String downloadStatus, String uploadStatus, String downloadTime, String uploadTime,
                        String errorMessage, String metadataStatus, String recordId, String fileSize) {
            this.fileName = fileName;
            this.folderPath = folderPath;
            this.swatchPdf = swatchPdf;
            this.printSpecPdf = printSpecPdf;
            this.downloadStatus = downloadStatus;
            this.uploadStatus = uploadStatus;
            this.downloadTime = downloadTime;
            this.uploadTime = uploadTime;
            this.errorMessage = errorMessage;
            this.metadataStatus = metadataStatus;
            this.recordId = recordId;
            this.fileSize = fileSize;
        }

        // Getters
        public String getFileName() { return fileName; }
        public String getFolderPath() { return folderPath; }
        public String getSwatchPdf() { return swatchPdf; }
        public String getPrintSpecPdf() { return printSpecPdf; }
        public String getDownloadStatus() { return downloadStatus; }
        public String getUploadStatus() { return uploadStatus; }
        public String getDownloadTime() { return downloadTime; }
        public String getUploadTime() { return uploadTime; }
        public String getErrorMessage() { return errorMessage; }
        public String getMetadataStatus() { return metadataStatus; }
        public String getRecordId() { return recordId; }
        public String getFileSize() { return fileSize; }
    }

    private static class ExcelLogger implements AutoCloseable {
        private final Workbook workbook;
        private final Sheet sheet;
        private final File file;
        private int rowNum = 0;

        public ExcelLogger(String filePath) {
            this.workbook = new XSSFWorkbook();
            this.sheet = workbook.createSheet("Migration Log");
            this.file = new File(filePath);
            createHeaderRow();
        }

        private void createHeaderRow() {
            Row headerRow = sheet.createRow(rowNum++);
            String[] headers = {
                    "File Name", "Folder Path", "Swatch PDF", "Print Spec PDF",
                    "Download Status", "Upload Status", "Download Time", "Upload Time",
                    "Error Message", "Metadata Status", "Record ID", "File Size"
            };

            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
            }
        }

        public synchronized void log(LogEntry entry) {
            Row row = sheet.createRow(rowNum++);
            int col = 0;

            row.createCell(col++).setCellValue(entry.getFileName());
            row.createCell(col++).setCellValue(entry.getFolderPath());
            row.createCell(col++).setCellValue(entry.getSwatchPdf());
            row.createCell(col++).setCellValue(entry.getPrintSpecPdf());
            row.createCell(col++).setCellValue(entry.getDownloadStatus());
            row.createCell(col++).setCellValue(entry.getUploadStatus());
            row.createCell(col++).setCellValue(entry.getDownloadTime());
            row.createCell(col++).setCellValue(entry.getUploadTime());
            row.createCell(col++).setCellValue(entry.getErrorMessage());
            row.createCell(col++).setCellValue(entry.getMetadataStatus());
            row.createCell(col++).setCellValue(entry.getRecordId());
            row.createCell(col).setCellValue(entry.getFileSize());

            // Auto-size columns
            for (int i = 0; i < 12; i++) {
                sheet.autoSizeColumn(i);
            }
        }

        @Override
        public void close() throws Exception {
            try (FileOutputStream out = new FileOutputStream(file)) {
                workbook.write(out);
            }
            workbook.close();
        }
    }
}