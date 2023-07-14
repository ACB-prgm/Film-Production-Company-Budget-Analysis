# Production Budget Analyzer

This repository holds the code for a sophisticated ETL (Extract, Transform, Load) and data analytics pipeline I developed for a film production company to optimize their budget data handling and analysis.

The pipeline begins with a Flask application, hosted on AWS Elastic Beanstalk, that integrates with Dropbox and Google Sheets APIs. Users authorize these integrations through the app, enabling the system to access a designated Dropbox folder that houses all relevant budget data.

Upon selection of this folder, the Flask application springs into action, downloading and cacheing the budget data, processing it into a unified Excel file, and uploading this file to Google Sheets via the Google Sheets API. This dynamic Excel document in Google Sheets serves as a central repository for the budget data, streamlining data management and analysis.

Further enhancing the data's usability, the Google Sheets document is linked to a Looker dashboard. This connection allows the end-user, and anyone with whom they share the dashboard, to interact with and scrutinize the data, facilitating insightful and data-driven decision-making.

Finally, the system has a continuous data monitoring mechanism. A webhook from Dropbox has been integrated into the Flask application, enabling the system to track any modifications in the Dropbox folder. If any changes are detected, the app cross-references the updated files with its cache. If differences are identified, it triggers a fresh ETL process, ensuring the Looker dashboard always reflects the most recent and accurate data. This automated loop guarantees that the data, its analysis, and consequent decisions remain timely and effective.

Here is a diagram of the process:

![Diagram](https://github.com/ACB-prgm/Film-Production-Company-Budget-Analysis/assets/63984796/6780efdd-221d-495a-a8d8-3257c1ab6db2)
