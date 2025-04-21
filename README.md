# Snowboarding Statistics and Biometric Data
This project uses my heart rate data collected from a Pixel Watch 2 (FitBit app) and GPS data from the [Slopes app](https://getslopes.com/) to create a dashboard that allows me to explore my snowboarding metrics, such as speed, heart rate, places I am fastest, and more. This project is an exercise in learning how to create a repeatable pipeline that extracts and merges disparate data formats using multiple programming languages, then visualize the data in interactive dashboard to explore my metrics. I'll be updating this each season as I go on more trips.

[Check out my blog about this project this to learn more](https://blogs.sas.com/content/sgf/2025/04/18/from-slopes-to-stats/).

# Dashboard
**You must disable your ad blockers for the dashboard to render, otherwise you will get a blank screen**

Mobile is not supported at this time and is best viewed on a desktop.

[View my snowboarding dashboard on github.io](https://stu-code.github.io/snowboarding-dashboard)

![image](https://github.com/user-attachments/assets/6234e24a-7de9-4121-a63b-b3e585908505)
![image](https://github.com/user-attachments/assets/33b77bbc-7f90-458a-9232-6a2c873f587c)

# Software Required
1. Python 3.11
2. SAS Viya Workbench or SAS Viya

# Data
Data is located in the **data** folder
1. `final` - Final transformed datasets in parquet and sas7bdat format (UTF-8)
2. `gps` - GPS data in [gpx format](https://wiki.openstreetmap.org/wiki/GPX#:~:text=GPX%2C%20or%20GPS%20exchange%20format,and%20be%20used%20during%20editing.) and GPS metadata .slopes format (Zip file)
3. `hr` - FitBit app heart rate data in .csv format downloaded from Google Takeout

# Programs
1. `extract_snowboard_data.ipynb` - Extracts data with Python to stage it for final transformation
2. `transform_snowboard_data.sas` - Transforms data with SAS into a format for dashboarding in SAS Visual Analytics

# Expected Folder Structure
```
[root]
   |
   |-----[stage]
   |-----[snowboarding]
               |--------- extract_snowboard_data.ipynb
               |--------- transform_snowboard_data.sas
               |--------- [data]
                            |----- [gps]
                            |----- [hr]
```
