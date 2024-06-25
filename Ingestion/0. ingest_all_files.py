# Databricks notebook source
# List of notebooks to be executed
notebooks = [
    '1. Ingest_Circuits_File',
    '2. Ingest_Races_File',
    '3. Ingest_Constructor_File',
    '4. Ingest_Drivers_File',
    '5. Ingest_Results_File',
    '6. Ingestion_Pitstop_File',
    '7. Ingestion_LapTimes_File',
    '8. Ingestion_Qualifying_File'
]

# Parameters to be passed to the notebooks
param = {
    'p_data_source': 'Ergast API',
    'p_file_date': '2021-03-21'
}

# Loop through the list of notebooks
for notebook in notebooks:
    print(f"Running notebook: {notebook}")
    result = dbutils.notebook.run(notebook, 0, param)
    
    # Check if the notebook execution was successful
    if result == 'Success':
        print(f"Notebook {notebook} executed successfully.")
    else:
        # Handle the failure case
        print(f"Notebook {notebook} failed to execute. Exiting the loop.")
        break

print("Notebook execution process completed.")
