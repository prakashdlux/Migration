package org.example;

/**
 * @param folderPath 📁 Folder path (from SharePoint)
 * @param recordId   🆔 Aprimo asset record ID
 * @param fileSize   📦 File size in MB (e.g., "12.45 MB")
 */
public record LogEntry(String fileName, String folderPath, String downloadStatus, String uploadStatus,
                       String downloadTime, String uploadTime, String errorMessage, String metadataStatus,
                       String recordId, String fileSize) {
}
