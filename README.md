# Fingrid power visualizer

Viisualization of current power production and consumption in the main power grid in Finland. Power production and consumption are furthermore divided into distinct factors as provided by the Fingrid API. Data is based on the real time data (updated once per three minutes) but the database and notebook is updated once per hour.

In this mini project a Python script on a virtual machine extracts data from Fingrid API, performs minor transformations and loads it to a database held on the same VM. Power data held on the VM database is then retrieved and visualized by a Python script in a Jupyter notebook. ETL and data visualization processes are updated hourly by a cron job scheduler. 

**Main components/technologies**:
- All scripts are running on an Ubuntu VM in Google Cloud (Compute Engine)
- Pandas is used in the data processing
- Data is stored in a PostgreSQL database
- Visualizations are drawn with Seaborn/Matplotlib
- Cron schedules the execution of database and notebook update scripts 

[Example of the visualization notebook can be seen here](https://github.com/Halmari/power-visualization/blob/main/power_info_visualizer.ipynb).

Data source: Fingrid / data.fingrid.fi, license CC 4.0 BY
