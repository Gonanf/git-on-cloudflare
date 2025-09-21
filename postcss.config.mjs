import { resolve, join, basename } from "path";
import { existsSync, mkdirSync, copyFileSync } from "fs";
import tailwindcss from "@tailwindcss/postcss";
import cssImport from "postcss-import";
import postcssUrl from "postcss-url";
import cssnano from "cssnano";

export default (ctx) => {
  const minify = process.argv.includes("--minify");

  const plugins = [
    // Make @import "bootstrap-icons/..." resolve/inlined first
    cssImport({ path: ["node_modules"] }),
    // Tailwind v4 PostCSS plugin
    tailwindcss(),
    // Copy the BI font files and rewrite url() to fonts/...
    // Using a custom transform function for better control
    postcssUrl({
      url: (asset, dir) => {
        // Only process bootstrap-icons font files
        if (!/bootstrap-icons\.(woff2?|woff)(\?.*)?$/i.test(asset.pathname)) {
          return asset.url;
        }

        // Source file path
        const sourcePath = asset.absolutePath;

        // Target directory - relative to dist folder
        const targetDir = resolve("src/assets/dist/fonts");
        const targetFile = join(targetDir, basename(asset.pathname));

        // Create target directory if it doesn't exist
        if (!existsSync(targetDir)) {
          mkdirSync(targetDir, { recursive: true });
        }

        // Check if source file exists
        if (!existsSync(sourcePath)) {
          console.error("Source file does not exist:", sourcePath);
          return asset.url;
        }

        // Copy the file
        copyFileSync(sourcePath, targetFile);

        // Return the new URL path relative to the CSS file location
        // Since CSS is at src/assets/dist/app.css and fonts at src/assets/dist/fonts/
        return "fonts/" + basename(asset.pathname);
      },
      basePath: resolve("node_modules/bootstrap-icons/font"),
    }),
  ];

  // Add cssnano for minification in production
  if (minify) {
    plugins.push(
      cssnano({
        preset: "default",
      })
    );
  }

  return { plugins };
};
