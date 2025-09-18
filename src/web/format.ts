// Utility formatting and content helpers

/**
 * Escapes HTML special characters to prevent XSS
 * @param s - String to escape
 * @returns HTML-safe string
 */
export function escapeHtml(s: string): string {
  return s.replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c] as string
  );
}

/**
 * Detects if content is binary by checking for non-text bytes
 * @param bytes - Content to check
 * @returns True if content appears to be binary
 * @note Checks first 8KB for null bytes or control characters
 */
export function detectBinary(bytes: Uint8Array): boolean {
  // Check first 8KB for null bytes or non-text characters
  const checkLength = Math.min(8192, bytes.length);
  for (let i = 0; i < checkLength; i++) {
    const byte = bytes[i];
    // Null byte or control characters (except tab, newline, carriage return)
    if (byte === 0 || (byte < 32 && byte !== 9 && byte !== 10 && byte !== 13)) {
      return true;
    }
  }
  return false;
}

/**
 * Formats byte size into human-readable string
 * @param bytes - Size in bytes
 * @returns Formatted string (e.g., "1.5 MB")
 */
export function formatSize(bytes: number): string {
  if (bytes < 1024) return bytes + " bytes";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  return (bytes / (1024 * 1024)).toFixed(1) + " MB";
}

/**
 * Converts byte array to text, handling various encodings
 * @param bytes - Raw bytes to decode
 * @returns Decoded text string
 * @note Handles UTF-8, UTF-16 LE/BE with BOM detection
 */
export function bytesToText(bytes: Uint8Array): string {
  if (!bytes || bytes.byteLength === 0) return "";
  // UTF-8 BOM
  if (bytes.length >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
    return new TextDecoder("utf-8").decode(bytes.subarray(3));
  }
  // UTF-16 LE BOM
  if (bytes.length >= 2 && bytes[0] === 0xff && bytes[1] === 0xfe) {
    try {
      return new TextDecoder("utf-16le").decode(bytes.subarray(2));
    } catch {}
  }
  // UTF-16 BE BOM
  if (bytes.length >= 2 && bytes[0] === 0xfe && bytes[1] === 0xff) {
    try {
      return new TextDecoder("utf-16be").decode(bytes.subarray(2));
    } catch {}
  }
  // Default to UTF-8
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
  } catch {
    return "(binary content)";
  }
}

export function formatWhen(epochSeconds: number, tz: string): string {
  // Display local time + tz offset string
  try {
    const d = new Date(epochSeconds * 1000);
    // Keep it simple: ISO date and preserve provided tz offset string
    return `${d.toISOString().replace("T", " ").replace("Z", " UTC")} (${tz})`;
  } catch {
    return String(epochSeconds);
  }
}
