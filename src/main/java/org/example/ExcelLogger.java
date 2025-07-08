package org.example;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ExcelLogger {
    private final String filePath;
    private final Workbook workbook;
    private final Sheet sheet;
    private final AtomicInteger rowIndex = new AtomicInteger(1); // Start after header

    private final String[] headers = {
            "File Name", "Folder Path", "Download Status", "Upload Status",
            "Download Time", "Upload Time", "Error Message", "Metadata Status", "Record ID", "File Size"
    };

    public ExcelLogger(String filePath) throws IOException {
        this.filePath = filePath;
        File file = new File(filePath);

        if (file.exists() && file.length() > 0) {
            try (FileInputStream fis = new FileInputStream(file)) {
                workbook = new XSSFWorkbook(fis);
                sheet = workbook.getSheetAt(0);
                rowIndex.set(sheet.getLastRowNum() + 1); // continue appending
            }
        } else {
            workbook = new XSSFWorkbook();
            sheet = workbook.createSheet("Migration Log details");
            createHeader();
            save(); // write header row
        }
    }

    private void createHeader() {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            headerRow.createCell(i).setCellValue(headers[i]);
        }
    }

    public synchronized void log(LogEntry entry) {
        Row row = sheet.createRow(rowIndex.getAndIncrement());

        row.createCell(0).setCellValue(entry.fileName());
        row.createCell(1).setCellValue(entry.folderPath());
        row.createCell(2).setCellValue(entry.downloadStatus());
        row.createCell(3).setCellValue(entry.uploadStatus());
        row.createCell(4).setCellValue(entry.downloadTime());
        row.createCell(5).setCellValue(entry.uploadTime());
        row.createCell(6).setCellValue(entry.errorMessage() != null ? entry.errorMessage() : "");
        row.createCell(7).setCellValue(entry.metadataStatus());
        row.createCell(8).setCellValue(entry.recordId() != null ? entry.recordId() : "-");
        row.createCell(9).setCellValue(entry.fileSize() != null ? entry.fileSize() : "0 MB");

        try {
            save();
            System.out.println("📝 Logged: " + entry.fileName());
        } catch (IOException e) {
            System.err.println("❌ Failed to write log for " + entry.fileName() + ": " + e.getMessage());
        }
    }

    private void save() throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            workbook.write(fos);
        }
    }

    public void close() throws IOException {
        addTotalFileSizeRow(); // Add this before closing
        save();
        workbook.close();
    }

    public File getFile() {
        return new File(filePath);
    }

    // ✅ Add a total file size row at the end
    private void addTotalFileSizeRow() {
        double totalSizeMB = 0.0;

        for (int i = 1; i < rowIndex.get(); i++) {
            Row row = sheet.getRow(i);
            if (row != null) {
                Cell cell = row.getCell(9); // File Size column
                if (cell != null && cell.getCellType() == CellType.STRING) {
                    String value = cell.getStringCellValue().replace(" MB", "").trim();
                    try {
                        totalSizeMB += Double.parseDouble(value);
                    } catch (NumberFormatException ignored) {}
                }
            }
        }

        Row totalRow = sheet.createRow(rowIndex.getAndIncrement());
        totalRow.createCell(8).setCellValue("Total File Size:");
        totalRow.createCell(9).setCellValue(String.format("%.2f MB", totalSizeMB));
    }
}
