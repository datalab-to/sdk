/**
 * Datalab API - Node.js Example
 *
 * This example demonstrates how to use the Datalab API to convert documents
 * to markdown, HTML, or JSON using Node.js (native fetch).
 *
 * Get your API key from: https://www.datalab.to/app/keys
 *
 * Usage:
 *   node datalab-example.js
 *
 * Or with a file URL:
 *   node datalab-example.js https://example.com/document.pdf
 */

// Configuration
const API_KEY = "dlab_abc123xyz789_your_api_key_here"; // Replace with your actual API key
const BASE_URL = "https://www.datalab.to";

/**
 * Convert a document using the Datalab Marker API
 * @param {string} fileUrl - URL of the document to convert
 * @param {Object} options - Conversion options
 * @returns {Promise<Object>} - Conversion result with markdown, HTML, or JSON
 */
async function convertDocument(fileUrl, options = {}) {
  // Step 1: Submit the conversion request
  console.log("Submitting document for conversion...");

  const formData = new FormData();
  formData.append("file_url", fileUrl);

  // Add optional parameters
  if (options.outputFormat) {
    formData.append("output_format", options.outputFormat); // "markdown", "html", or "json"
  }
  if (options.maxPages) {
    formData.append("max_pages", options.maxPages.toString());
  }
  if (options.languages) {
    formData.append("languages", options.languages); // comma-separated, e.g., "English,Spanish"
  }

  const submitResponse = await fetch(`${BASE_URL}/api/v1/marker`, {
    method: "POST",
    headers: {
      "X-Api-Key": API_KEY,
    },
    body: formData,
  });

  if (!submitResponse.ok) {
    const error = await submitResponse.json();
    throw new Error(`Submission failed: ${error.detail || error.error || submitResponse.statusText}`);
  }

  const submitData = await submitResponse.json();

  if (!submitData.success) {
    throw new Error(`Submission failed: ${submitData.error || "Unknown error"}`);
  }

  console.log(`Request submitted. Polling for results...`);

  // Step 2: Poll for completion
  const result = await pollForResult(submitData.request_check_url);

  return result;
}

/**
 * Poll the API until processing is complete
 * @param {string} checkUrl - URL to poll for results
 * @param {number} maxPolls - Maximum number of polling attempts
 * @param {number} pollInterval - Seconds between polls
 * @returns {Promise<Object>} - Final result
 */
async function pollForResult(checkUrl, maxPolls = 300, pollInterval = 1) {
  const fullUrl = checkUrl.startsWith("http") ? checkUrl : `${BASE_URL}${checkUrl}`;

  for (let i = 0; i < maxPolls; i++) {
    const response = await fetch(fullUrl, {
      headers: {
        "X-Api-Key": API_KEY,
      },
    });

    if (!response.ok) {
      throw new Error(`Polling failed: ${response.statusText}`);
    }

    const data = await response.json();

    if (data.status === "complete") {
      console.log("Processing complete!");
      return data;
    }

    if (!data.success && data.status !== "processing") {
      throw new Error(`Processing failed: ${data.error || "Unknown error"}`);
    }

    // Wait before next poll
    await new Promise((resolve) => setTimeout(resolve, pollInterval * 1000));

    // Show progress every 5 polls
    if ((i + 1) % 5 === 0) {
      console.log(`Still processing... (${i + 1} checks)`);
    }
  }

  throw new Error(`Polling timed out after ${maxPolls * pollInterval} seconds`);
}

/**
 * Perform OCR on a document
 * @param {string} fileUrl - URL of the document
 * @returns {Promise<Object>} - OCR result with pages
 */
async function performOCR(fileUrl) {
  console.log("Submitting document for OCR...");

  const formData = new FormData();
  formData.append("file_url", fileUrl);

  const submitResponse = await fetch(`${BASE_URL}/api/v1/ocr`, {
    method: "POST",
    headers: {
      "X-Api-Key": API_KEY,
    },
    body: formData,
  });

  if (!submitResponse.ok) {
    const error = await submitResponse.json();
    throw new Error(`OCR submission failed: ${error.detail || error.error || submitResponse.statusText}`);
  }

  const submitData = await submitResponse.json();

  if (!submitData.success) {
    throw new Error(`OCR submission failed: ${submitData.error || "Unknown error"}`);
  }

  console.log("OCR request submitted. Polling for results...");

  return await pollForResult(submitData.request_check_url);
}

// Example usage
async function main() {
  // Example document URL (replace with your own)
  const documentUrl =
    process.argv[2] || "https://arxiv.org/pdf/2310.08535";

  try {
    // Convert document to markdown
    const result = await convertDocument(documentUrl, {
      outputFormat: "markdown",
    });

    console.log("\n--- Conversion Result ---");
    console.log(`Status: ${result.status}`);
    console.log(`Page count: ${result.page_count || "N/A"}`);

    if (result.markdown) {
      // Show first 500 characters of markdown
      console.log("\nMarkdown preview (first 500 chars):");
      console.log(result.markdown.substring(0, 500) + "...");
    }

    if (result.metadata) {
      console.log("\nMetadata:", JSON.stringify(result.metadata, null, 2));
    }
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
}

// Run the example
main();
