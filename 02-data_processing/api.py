from report_generator import ReportGenerator
from config import Config
from flask import Flask, request, jsonify
from spark_session import SparkManager
import threading
import os


app = Flask(__name__)

def create_report(start_date, end_date):
    reports_dir = os.path.join(Config.OUTPUT_DIR, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    spark_manager = SparkManager(
        mysql_host="employees_db",
        mysql_port="3306",
        mysql_db="employees",
        mysql_user="root",
        mysql_password="password"
    )

    dataframes = spark_manager.load_tables(Config.TABLES)

    report_generator = ReportGenerator(spark_manager.spark, reports_dir)

    report_generator.generate_changes_report(
        dataframes['employees'],
        dataframes['dept_emp'],
        dataframes['titles'],
        dataframes['salaries'],
        dataframes['dept_manager'],
        dataframes['departments'],
        start_date,
        end_date
    )

    spark_manager.stop_session()

@app.route('/process', methods=['GET'])
def trigger_processing():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    thread = threading.Thread(target=create_report, args=(start_date, end_date))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "status": "processing_started",
        "start_date": start_date,
        "end_date": end_date,
        "message": "Processing is running in the background"
    }), 202

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)