# Creating a Sensemaking Data Pipeline

Project completed while studying at MIT

In this project, I used the Python urllib library to pull data from MIT’s course catalog. The pulled data from the web was unstructured, so I had to clean it first to extract the course names. Once the course names were extracted and the data became structured, I performed some data analysis to learn how many times each word occurs throughout all of the course names. This analysis was later saved as a JSON file, which then was referenced by D3 web application. Lastly, I created the web application that generates a visual analysis of the JSON data that I collected.

To streamline the data analysis process, I used software tools such as Docker and Airflow. I installed Airflow web server within a Docker container to handle each Python task that I defined. These tasks divided the project into smaller, more manageable pieces. In the end, I experimented with different D3 libraries to customize my project.
