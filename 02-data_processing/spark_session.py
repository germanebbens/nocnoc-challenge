from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, mysql_host, mysql_port, mysql_db, mysql_user, mysql_password):
        self.jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
        self.connector_path = "connector/mysql-connector-java-8.0.28.jar"
        self.connection_properties = {
            "user": mysql_user,
            "password": mysql_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        self.spark = None
        
    def create_session(self):
        self.spark = SparkSession.builder \
            .appName("EmployeesDataProcessing") \
            .config("spark.driver.extraClassPath", self.connector_path) \
            .getOrCreate()
        return self.spark
    
    def load_tables(self, tables):
        if not self.spark:
            self.create_session()
            
        dataframes = {}
        for table in tables:
            dataframes[table] = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table,
                properties=self.connection_properties
            )
            print(f"Loaded {table}: {dataframes[table].count()} rows")
        
        return dataframes
    
    def stop_session(self):
        if self.spark:
            self.spark.stop()
