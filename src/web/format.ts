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
    // Keep it simple: ISO date without milliseconds and preserve provided tz offset string
    const iso = d.toISOString(); // e.g., 2025-09-21T10:08:06.000Z
    const noMs = iso.replace(/\.\d{3}Z$/, "Z");
    return `${noMs.replace("T", " ").replace("Z", " UTC")} (${tz})`;
  } catch {
    return String(epochSeconds);
  }
}

/**
 * Determines the appropriate Bootstrap Icon class for a file based on its extension
 * @param filename - The name of the file
 * @returns Bootstrap Icon class name (e.g., "bi-filetype-js")
 */
export function getFileIconClass(filename: string): string {
  // Default icon for files
  const defaultIcon = "bi-file-earmark";

  // Get file extension (lowercase, without dot)
  const ext = filename.split(".").pop()?.toLowerCase() || "";

  // Map extensions to Bootstrap Icon classes
  // Using specific filetype-* icons where available, file-earmark-* for others
  const iconMap: Record<string, string> = {
    // JavaScript/TypeScript - specific filetype icons
    js: "bi-filetype-js",
    mjs: "bi-filetype-js",
    cjs: "bi-filetype-js",
    jsx: "bi-filetype-jsx",
    ts: "bi-filetype-tsx", // Using tsx icon for ts files
    tsx: "bi-filetype-tsx",

    // Web technologies - specific filetype icons
    html: "bi-filetype-html",
    htm: "bi-filetype-html",
    css: "bi-filetype-css",
    scss: "bi-filetype-scss",
    sass: "bi-filetype-sass",
    less: "bi-filetype-less",
    vue: "bi-file-earmark-code",
    svelte: "bi-file-earmark-code",

    // Programming languages - specific filetype icons where available
    py: "bi-filetype-py",
    rb: "bi-filetype-rb",
    go: "bi-file-earmark-code", // No specific go icon yet
    java: "bi-filetype-java",
    c: "bi-file-earmark-code",
    h: "bi-file-earmark-code",
    cc: "bi-filetype-cpp",
    cpp: "bi-filetype-cpp",
    cxx: "bi-filetype-cpp",
    hpp: "bi-filetype-cpp",
    cs: "bi-filetype-cs",
    php: "bi-filetype-php",
    swift: "bi-file-earmark-code", // No specific swift icon
    kt: "bi-file-earmark-code", // No specific kotlin icon
    rs: "bi-file-earmark-code", // No specific rust icon
    lua: "bi-file-earmark-code",
    dart: "bi-file-earmark-code",
    mm: "bi-file-earmark-code", // Objective-C
    pl: "bi-file-earmark-code", // Perl

    // Shell scripts - specific filetype icon
    sh: "bi-filetype-sh",
    bash: "bi-filetype-sh",
    zsh: "bi-filetype-sh",
    ps1: "bi-filetype-ps1",
    psm1: "bi-filetype-ps1",

    // Config/data files - specific filetype icons where available
    json: "bi-filetype-json",
    jsonc: "bi-filetype-json",
    xml: "bi-filetype-xml",
    yaml: "bi-filetype-yml",
    yml: "bi-filetype-yml",
    sql: "bi-filetype-sql",
    toml: "bi-file-earmark-code", // No specific toml icon
    ini: "bi-file-earmark-code",
    cfg: "bi-file-earmark-code",
    conf: "bi-file-earmark-code",
    diff: "bi-file-earmark-diff",
    gradle: "bi-file-earmark-code", // Groovy
    proto: "bi-file-earmark-code",

    // Text/Documentation files - specific filetype icons where available
    txt: "bi-filetype-txt",
    md: "bi-filetype-md",
    markdown: "bi-filetype-md",
    mdx: "bi-filetype-mdx",
    rst: "bi-file-earmark-text",
    tex: "bi-file-earmark-text",
    log: "bi-file-earmark-text",

    // Image files - specific filetype icons where available
    jpg: "bi-filetype-jpg",
    jpeg: "bi-filetype-jpg",
    png: "bi-filetype-png",
    gif: "bi-filetype-gif",
    svg: "bi-filetype-svg",
    webp: "bi-file-earmark-image",
    ico: "bi-file-earmark-image",
    bmp: "bi-filetype-bmp",
    tiff: "bi-filetype-tiff",
    tif: "bi-filetype-tiff",
    psd: "bi-filetype-psd",
    ai: "bi-filetype-ai",
    raw: "bi-filetype-raw",

    // Documents - specific filetype icons where available
    pdf: "bi-filetype-pdf",
    doc: "bi-filetype-doc",
    docx: "bi-filetype-docx",
    xls: "bi-filetype-xls",
    xlsx: "bi-filetype-xlsx",
    csv: "bi-filetype-csv",
    ppt: "bi-filetype-ppt",
    pptx: "bi-filetype-pptx",
    odt: "bi-file-earmark-text", // OpenDocument Text
    ods: "bi-file-earmark-spreadsheet", // OpenDocument Spreadsheet
    odp: "bi-file-earmark-slides",

    // Archives - file-earmark-zip for all
    zip: "bi-file-earmark-zip",
    rar: "bi-file-earmark-zip",
    tar: "bi-file-earmark-zip",
    gz: "bi-file-earmark-zip",
    "7z": "bi-file-earmark-zip",
    bz2: "bi-file-earmark-zip",
    xz: "bi-file-earmark-zip",

    // Media files - specific filetype icons where available
    mp3: "bi-filetype-mp3",
    mp4: "bi-filetype-mp4",
    wav: "bi-filetype-wav",
    aac: "bi-filetype-aac",
    m4p: "bi-filetype-m4p",
    m4a: "bi-file-earmark-music",
    ogg: "bi-file-earmark-music",
    flac: "bi-file-earmark-music",

    avi: "bi-file-earmark-play",
    mkv: "bi-file-earmark-play",
    mov: "bi-filetype-mov",
    wmv: "bi-file-earmark-play",
    webm: "bi-file-earmark-play",
    flv: "bi-file-earmark-play",

    // Font files - specific filetype icons where available
    ttf: "bi-filetype-ttf",
    otf: "bi-filetype-otf",
    woff: "bi-filetype-woff",
    woff2: "bi-file-earmark-font",
    eot: "bi-filetype-eot",

    // Binary/executable files
    exe: "bi-filetype-exe",
    dll: "bi-file-earmark-binary",
    so: "bi-file-earmark-binary",
    dylib: "bi-file-earmark-binary",
    wasm: "bi-file-earmark-binary",
    key: "bi-filetype-key", // Keynote
    heic: "bi-filetype-heic",
    bin: "bi-file-earmark-binary",
  };

  // Special cases for files without extensions or with special names
  const specialFiles: Record<string, string> = {
    readme: "bi-filetype-md",
    license: "bi-file-earmark-text",
    dockerfile: "bi-file-earmark-code",
    makefile: "bi-file-earmark-code",
    gemfile: "bi-filetype-rb", // Ruby Gemfile
    rakefile: "bi-filetype-rb", // Ruby Rakefile
    guardfile: "bi-filetype-rb", // Ruby Guardfile
    procfile: "bi-file-earmark-code",
    gitignore: "bi-file-earmark-code",
    gitattributes: "bi-file-earmark-code",
    editorconfig: "bi-file-earmark-code",
    "package-lock": "bi-file-earmark-lock",
    "yarn.lock": "bi-file-earmark-lock",
    "pnpm-lock": "bi-file-earmark-lock",
    tsconfig: "bi-filetype-json", // tsconfig.json
    webpack: "bi-filetype-js", // webpack.config.js
    babel: "bi-filetype-js", // babel.config.js
    eslint: "bi-filetype-json", // .eslintrc.json
    prettier: "bi-filetype-json", // .prettierrc
  };

  // Check special files first (case-insensitive)
  const filenameLower = filename.toLowerCase();
  for (const [pattern, icon] of Object.entries(specialFiles)) {
    if (filenameLower.includes(pattern)) {
      return icon;
    }
  }

  // Check extension mapping
  return iconMap[ext] || defaultIcon;
}
