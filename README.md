# capstone
This is in partial fulfillment of my data engineering certificate with TDI

## LA Parking Lot Data Analysis Dashboard
This document presents a project that aims to provide insightful analytics for city planners based on data collected from parking meters in specific blocks of Los Angeles City. The data is collected and provided by (Los Angeles City Directory)[https://www.laexpresspark.org/la-city-open-data/]. The project is completed as part of my data engineering certificate with TDI.


### Data Description: 
The project utilizes two main datasets: Live Parking Meter feeds and Inventory & Policies dataset. The Live Parking Meter Feeds offers a five-minute periodic update on the occupancy status of parking slots at various times. On the other hand, the Inventory & Policies dataset provides valuable information such as the hourly rate and coordinates for each parking slot. The combined dataset contains 7 columns.

### Data Processing: 
To process the data effectively, an ETL (Extract, Transform, Load) pipeline was developed. The pipeline leverages APIs to access the data and swiftly extract the required variables. These variables are then consolidated into a JSON file and stored in an s3 bucket. Notably, the processing-to-storage pipeline is carefully designed to update existing parking slot data with changes obtained from the APIs since this dataset is live and dynamic. Therefore, all the data processing and updates occur within a single JSON file object.

### Data Interactivity: 
The processed data is then seamlessly pushed to a PostgreSQL database using an s3 sensor for storage and further analysis. The PostgreSQL database is utilized to stream the data into an interactive dashboard. This dashboard enables end users to access and download the data and gain insights into parking peaks, the occupancy status of each parking slot, parking prices, and parking volumes per slot.


### Conclusion: 
The visualization and interactivity are powered by the (Streamlit app)[https://streamlit.io/], offering an intuitive and user-friendly experience. To host the app, (Python Anywhere)[https://www.pythonanywhere.com/] is employed. The interactive visualizations provided by the dashboard empower end users to identify areas that may require new operations or improvements to enhance parking in Los Angeles City. This project serves as a valuable tool for city planners to make data-driven decisions and enhance the overall parking experience in LA.


End


