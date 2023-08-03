#import files
import psycopg2
import plotly.express as px
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
import plotly.express as px
import base64
from datetime import datetime
import pandas as pd

#read data
def read_data_from_postgres(dbname, user, password, host, port, query):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
        dbname='nejjfqyu',
        user= 'nejjfqyu',
        password= 'Xd27RYX7w3VkVAkbxUN9Q94A9lmVdmEV',
        port='5432',
        host= 'stampy.db.elephantsql.com'
    )
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all the data into a Pandas DataFrame
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return df

    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

#set download button
def get_csv_download_link(df):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="parking_data.csv">Download CSV File</a>'
    return href


dbname='nejjfqyu',
user= 'nejjfqyu',
password= 'Xd27RYX7w3VkVAkbxUN9Q94A9lmVdmEV',
port='5432',
host= 'stampy.db.elephantsql.com'


# Example SQL query to fetch data from the "parking_clean" table
query = "SELECT * FROM parking_clean;"

# Call the function to read data from the PostgreSQL database
df = read_data_from_postgres(dbname, user, password, host, port, query)

# Display the DataFrame
#df.head(3)



#Data transformation
# Convert "eventtime" column to pandas DateTime object
df['eventtime'] = pd.to_datetime(df['eventtime'])

# Set "eventtime" as the DataFrame index
df.set_index('eventtime', inplace=True)

# Geospatial Analysis - Latlng
df['latitude'] = df['latlng'].apply(lambda x: float(x['latitude']))
df['longitude'] = df['latlng'].apply(lambda x: float(x['longitude']))

# Combine the "occupied" and "vacant" spaces into one DataFrame
geospatial_df = df[df['occupancystate'].isin(['OCCUPIED', 'VACANT'])]

# Geospatial Analysis - Occupied Spaces (red) and Vacant Spaces (green)
geospatial_fig = px.scatter_mapbox(
    geospatial_df, lat="latitude", lon="longitude", color="occupancystate",
    color_discrete_map={"OCCUPIED": "red", "VACANT": "green"},
    hover_name="blockface", hover_data=["metertype", "spaceid", "occupancystate", "raterange"],
    mapbox_style="open-street-map", zoom=11, title="Geospatial Analysis - Occupied/Vacant Spaces"
)



# Streamlit App
st.set_page_config(
    page_title="Parking Data Analysis Dashboard",
    page_icon="🅿️",
    layout="wide"
)

# Pad to center the page content
st.markdown(
    """
    <style>
    .stApp {
        max-width: 1000px;
        margin: auto;
        padding-left: 10px;
        padding-right: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Header
st.title("🅿️ Parking Data Analysis Dashboard")
st.markdown("Welcome to the Parking Data Analysis Dashboard. This interactive dashboard provides insights "
            "into the parking data, including occupancy state and other important insights")

# Geospatial Analysis
st.header("🗺️ Map View")

# Base Map Selection
base_map = st.sidebar.selectbox("Select Base Map", ['open-street-map', 'satellite', 'carto-darkmatter'])
geospatial_fig.update_layout(mapbox_style=base_map)

# Display the Geospatial Analysis with the selected base map
st.plotly_chart(geospatial_fig)

# Download Button
st.sidebar.markdown("---")
st.sidebar.markdown("## 📥 Download Data")
st.sidebar.write("Click the button below to download the data as a CSV file.")
if st.sidebar.button("Download Data"):
    st.sidebar.markdown(get_csv_download_link(df), unsafe_allow_html=True)


# Non-Geospatial Analyses Buttons in the Sidebar
st.sidebar.header("📈 Analysis Options")

if st.sidebar.button('Parking Peaks'):
    # Time Series Analysis - Resample by 15 seconds and count occurrences
    peak_counts = df.resample('15S').size()

    # Time Series Analysis - Event Time by 15-second intervals (Line Graph)
    peak_time_series_fig = px.line(peak_counts, x=peak_counts.index, y=peak_counts.values, title="Parking Peaks")
    peak_time_series_fig.update_layout(xaxis_title="Date", yaxis_title="Count")
    st.plotly_chart(peak_time_series_fig)

if st.sidebar.button('Price per Slot'):
    # Price Range Analysis - SpaceID vs. Price
    price_range_fig = px.scatter(df, x="spaceid", y="raterange", color="occupancystate",
                                 hover_name="blockface", hover_data=["metertype", "occupancystate", "raterange"],
                                 title="Price per Slot")
    price_range_fig.update_layout(xaxis_title="SpaceID", yaxis_title="Price Range")
    st.plotly_chart(price_range_fig)

if st.sidebar.button('Parking Volume'):
    # Correlation Analysis - Occupancy State and Event Time
    correlation_df = df.groupby('occupancystate').size().reset_index()
    correlation_fig = px.bar(correlation_df, x='occupancystate', y=0, title="Parking Volume")
    correlation_fig.update_layout(xaxis_title="Occupancy State", yaxis_title="Count")
    st.plotly_chart(correlation_fig)


# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("Built with ❤️ by [Stellamaris]")
st.sidebar.markdown("[GitHub Repository](https://github.com/StellaWava/capstone)")
