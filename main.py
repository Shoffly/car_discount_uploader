import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import io

# Set page config
st.set_page_config(
    page_title="Showroom Discount Data Uploader",
    page_icon="💰",
    layout="wide"
)


# Function to validate data structure
def validate_data(df):
    """Validate the uploaded data structure and content"""
    required_columns = ['c_code', 'flash_price', 'consignment_price', 'speed_discount_price']

    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"

    # Check for empty c_code (required field)
    if df['c_code'].isnull().any() or (df['c_code'] == '').any():
        return False, "c_code column cannot have empty values"

    # Check for duplicate c_codes
    if df['c_code'].duplicated().any():
        duplicates = df[df['c_code'].duplicated()]['c_code'].tolist()
        return False, f"Duplicate c_code values found: {', '.join(duplicates)}"

    # Check if price columns are numeric (allow NaN for nullable fields)
    price_columns = ['flash_price', 'consignment_price', 'speed_discount_price']
    for col in price_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                return False, f"Column {col} contains non-numeric values that cannot be converted"

    return True, "Data validation successful"


# Function to upload data to BigQuery
def upload_to_bigquery(df):
    """Upload the dataframe to BigQuery by appending to existing data"""
    try:
        # Get credentials for BigQuery
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                return False, "Error: No credentials found for BigQuery access"

        # Create BigQuery client
        client = bigquery.Client(credentials=credentials)

        # Define table reference
        table_id = "pricing-338819.wholesale_test.showroom_discount"

        # Configure the load job for appending
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # This will append data to the table
            schema=[
                bigquery.SchemaField("c_code", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("flash_price", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("consignment_price", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("speed_discount_price", "FLOAT64", mode="NULLABLE"),
            ]
        )

        # Load data to BigQuery
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        return True, f"Successfully uploaded {len(df)} rows to showroom_discount table"

    except Exception as e:
        return False, f"Error appending to BigQuery: {str(e)}"


# Function to create template file
def create_template():
    """Create a template DataFrame with sample data"""
    template_data = {
        'c_code': ['c-001', 'c-002', 'c-003'],
        'flash_price': [25000.00, 30000.00, 22000.00],
        'consignment_price': [27000.00, 32000.00, 24000.00],
        'speed_discount_price': [24000.00, 29000.00, 21000.00]
    }
    return pd.DataFrame(template_data)


# Main app
def main():
    st.title("💰 Showroom Discount Data Uploader")
    st.markdown("Upload Excel or CSV files with showroom discount data to the BigQuery database.")

    # Template download section
    st.header("📥 Download Template")
    st.markdown("Download a template file to see the required format and column structure.")

    col1, col2 = st.columns(2)

    with col1:
        # Create template data
        template_df = create_template()

        # Convert to CSV for download
        csv_template = template_df.to_csv(index=False)
        st.download_button(
            label="📄 Download CSV Template",
            data=csv_template,
            file_name="showroom_discount_template.csv",
            mime="text/csv",
            help="Download a CSV template with sample data"
        )

    with col2:
        # Convert to Excel for download
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            template_df.to_excel(writer, index=False, sheet_name='showroom_discount')
        excel_data = excel_buffer.getvalue()

        st.download_button(
            label="📊 Download Excel Template",
            data=excel_data,
            file_name="showroom_discount_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Download an Excel template with sample data"
        )

    # Show template preview
    st.subheader("Template Preview")
    st.dataframe(template_df)
    st.info("💡 Replace the sample data above with your actual car codes and prices")

    st.divider()

    # File upload section
    st.header("📁 Upload File")
    uploaded_file = st.file_uploader(
        "Choose an Excel or CSV file",
        type=['xlsx', 'xls', 'csv'],
        help="File should contain columns: c_code, flash_price, consignment_price, speed_discount_price"
    )

    if uploaded_file is not None:
        try:
            # Read the file based on its type
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.success(f"File loaded successfully! Found {len(df)} rows and {len(df.columns)} columns.")

            # Display file preview
            st.header("📊 Data Preview")
            st.dataframe(df.head(10))


            # Validate data

            is_valid, validation_message = validate_data(df)

            if is_valid:
                st.success(validation_message)






                if st.button("🚀 Upload Data", type="primary"):
                    with st.spinner("Uploading data to BigQuery..."):
                        success, message = upload_to_bigquery(df)

                        if success:
                            st.success(message)
                            st.balloons()
                        else:
                            st.error(message)
            else:
                st.error(validation_message)
                st.info("Please fix the data issues and upload again.")

        except Exception as e:
            st.error(f"Error reading file: {str(e)}")

    # Instructions section
    st.header("📋 Instructions")
    st.markdown("""
    ### Required File Format:
    Your Excel/CSV file must contain the following columns:

    | Column Name | Type | Description |
    |-------------|------|-------------|
    | `c_code` | TEXT | Car code identifier (cannot be empty) |
    | `flash_price` | NUMBER | Flash sale price |
    | `consignment_price` | NUMBER | Consignment price |
    | `speed_discount_price` | NUMBER | Speed discount price |

    ### Important Notes:
    - **c_code** is required and cannot be empty
    - **c_code** values must be unique (no duplicates)
    - Price columns can be empty (will be stored as NULL)
    - Price columns must contain numeric values only
    - Supported file formats: Excel (.xlsx, .xls) and CSV (.csv)

    
    """)


if __name__ == "__main__":
    main()
