// This script uses Playwright to scrape data from the Active and Closed Investigations
// tables on the FDA Foodborne Illness Outbreaks page.

// Loading dependencies
//      1. playwright enables the information of targeted webpage elements to be collected
//      2. file system enables the data caching
//      3. path enables directory navigation
const playwright = require('playwright');
const fs = require('fs');
const path = require('path');

// Defining global variables
//      1. The url to the FDA page for outbreaks of foodborne illnesses
//      2. The output directory: 'data'
//      3. The name of the JSON file to be cached
const url = 'https://www.fda.gov/food/outbreaks-foodborne-illness/investigations-foodborne-illness-outbreaks';
const output_dir = 'data';
const output_file = 'fda_investigations_data.json';

/**
 * Scrapes a single table identified by a direct CSS selector.
 * @param {import('playwright').Page} page - The Playwright Page object.
 * @param {string} tableSelector - The direct CSS selector for the table
 * @param {string} tableName - A name for the table
 * @returns {Promise<{tableName: string, data: Array<Object>}>}
 */
async function scrapeTable(page, tableSelector, tableName) {
    console.log(`\n--- Attempting to scrape: ${tableName} ---`);

    // Checking if the table exists before attempting to scrape
    const tableElement = await page.$(tableSelector);

    if (!tableElement) {
        // Logging the full selector that failed for better debugging
        console.warn(`Table element not found using selector: ${tableSelector}. Skipping.`);
        return { tableName, data: [] };
    }

    try {
        const data = await page.evaluate((selector) => {
            const table = document.querySelector(selector);
            if (!table) return [];

            const rows = Array.from(table.querySelectorAll('tr'));
            if (rows.length === 0) return [];

            // Extracting the headers from the first row
            const headerRow = rows[0];
            const headers = Array.from(headerRow.querySelectorAll('th, td'))
                .map(cell => cell.textContent.trim())
                // Replacing newline/multi-space characters with a single space
                .map(text => text.replace(/\s+/g, ' '));

            // Extracting the table data, skipping the header row
            const dataRows = rows.slice(1);

            const tableData = [];
            dataRows.forEach(row => {
                const cells = Array.from(row.querySelectorAll('td'));
                const rowObject = {};

                cells.forEach((cell, index) => {
                    const header = headers[index];
                    let text = cell.textContent.trim().replace(/\s+/g, ' ');

                    // Checking for links within the cell
                    const link = cell.querySelector('a');
                    if (link) {
                        text += ` (Link: ${link.href})`;
                    }

                    if (header) {
                        rowObject[header] = text;
                    }
                });

                // Checking the extracted row for data before adding it 
                // the rest of the table data
                if (Object.keys(rowObject).length > 0) {
                    tableData.push(rowObject);
                }
            });

            return tableData;

        }, tableSelector); // Passing the selector into the page.evaluate context

        console.log(`Successfully extracted ${data.length} rows.`);
        return { tableName, data };

    } catch (error) {
        console.error(`Error scraping ${tableName}:`, error.message);
        return { tableName, data: [] };
    }
}

/**
 * Main function to run the scrape.
 */
async function main() {
    let browser;
    try {
        // Launching a headless Chromium browser
        browser = await playwright.chromium.launch();
        const page = await browser.newPage();

        console.log(`Navigating to: ${url}`);

        // Navigating and waiting for the target page to load completely
        await page.goto(url, { waitUntil: 'domcontentloaded' });

        const allResults = [];

        // Using the most recent stable selector to scrape the data from the Active
        // Investigations Table
        const activeTableSelector = '#main-content > div > div.table-responsive > table';
        const activeResults = await scrapeTable(page, activeTableSelector, 'Active Investigations');
        allResults.push(activeResults);

        // Using the stable selectors for the Closed Investigations tables (2025 through 2020)
        // to scrape their data
        const yearlyClosedSelectors = [
            { year: 2025, selector: '#main-content > div > div:nth-child(12) table' },
            { year: 2024, selector: '#main-content > div > div:nth-child(14) table' },
            { year: 2023, selector: '#main-content > div > div:nth-child(16) table' },
            { year: 2022, selector: '#main-content > div > div:nth-child(18) table' },
            { year: 2021, selector: '#main-content > div > div:nth-child(20) table' },
            { year: 2020, selector: '#main-content > div > div:nth-child(22) table' },
        ];

        // Scraping the Closed Investigations tables by Year
        for (const { year, selector } of yearlyClosedSelectors) {
            const closedResult = await scrapeTable(page, selector, `Closed Investigations ${year}`);
            allResults.push(closedResult);
        }

        console.log('\n======================================================');
        console.log('                 SCRAPING COMPLETE');
        console.log('======================================================');

        // Filtering out any empty results before saving the data
        const finalData = allResults.filter(result => result.data.length > 0);

        // Restructuring the data to JSON format and specifying the location
        // where the data is to be saved
        const jsonContent = JSON.stringify(finalData, null, 2);
        const fullOutputPath = path.join(output_dir, output_file);

        // Checking the existence of the target directory for the data 
        if (!fs.existsSync(output_dir)) {
            fs.mkdirSync(output_dir, { recursive: true });
        }

        // Writing the JSON data to the file
        fs.writeFileSync(fullOutputPath, jsonContent);

        console.log(`\n✅ Data successfully saved to: ${fullOutputPath}`);
        console.log(`   Total tables saved: ${finalData.length}`);

    } catch (error) {
        console.error('\nAn error occurred during the main execution:', error);
    } finally {
        // Closing the browser if it was successfully opened
        if (browser) {
            await browser.close();
        }
    }
}

main();