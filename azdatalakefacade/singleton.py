from pandas import DataFrame, read_csv
from threading import Lock
from azure.storage.blob import BlobServiceClient, ContainerClient
from os import getenv
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueServiceClient
from io import BytesIO

class SingletonMeta(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

class AzDataLakeSingleton(metaclass=SingletonMeta):
    str_connection: str
    blob_service_client: BlobServiceClient = None
    container_client: ContainerClient = None
    table_service_client: TableServiceClient = None
    

    def __init__(self, blob_cli: str = getenv("AZ_DATA_LAKE_STRING_CONNECTION"), container_name: str = getenv("AZ_CONTAINNER_CLI")) -> None:
        self.str_connection: str = blob_cli
        self.blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(blob_cli)
        self.container_client: ContainerClient = self.blob_service_client.get_container_client(container_name)
        self.table_service_client: TableServiceClient = TableServiceClient.from_connection_string(conn_str=self.str_connection)
        self.queue_service_client: QueueServiceClient = QueueServiceClient.from_connection_string(conn_str=self.str_connection)
        

    def upload_file(self, blob_name: str, data: bytes) -> bool:
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data, overwrite=True)
            return True
        except Exception as e:
            print("Error subir el archivo:", e)
            return False
        
    def upsert_data_to_table(self, table_name: str, data: DataFrame):
        """
        Inserta un DataFrame en una tabla de Azure Table Storage.
        Si la tabla no existe, la crea. Si ya existe, solo inserta los datos.
        
        Args:
            dataframe (pd.DataFrame): El DataFrame con los datos a insertar. 
                                    Debe incluir columnas 'PartitionKey' y 'RowKey'.
            table_name (str): Nombre de la tabla en Azure Table Storage.
        """
        # Crear conexión con Azure Table Storage
        table_client = self.table_service_client.get_table_client(table_name)

        
        # Verificar si la tabla existe y crearla si no existe
        try:
            self.table_service_client.create_table(table_name)
            print(f"Tabla '{table_name}' creada exitosamente.")
        except ResourceExistsError:
            print(f"Tabla '{table_name}' ya existe.")
        
        # Validar que el DataFrame tenga las columnas necesarias
        if 'PartitionKey' not in data.columns or 'RowKey' not in data.columns:
            raise ValueError("El DataFrame debe contener las columnas 'PartitionKey' y 'RowKey'.")
        
        # convirtiendo valores enteros a str por error de limite maximo int32
        for col in data.select_dtypes(include=['int']).columns:
            data[col] = data[col].astype(str)
            
        # Insertar o actualizar las entidades del DataFrame
        for _, row in data.iterrows():
            entity = row.to_dict()  # Convertir la fila del DataFrame a un diccionario
            table_client.upsert_entity(entity)
        
        print(f"Se insertaron {len(data)} registros en la tabla '{table_name}'.")
                
    def send_data_to_queue(self, queue_name: str, data: DataFrame):
        """
        Envía un DataFrame a una cola de Azure Storage.
        Si la cola no existe, la crea.

        Args:
            dataframe (pd.DataFrame): El DataFrame con los datos a enviar. 
                                    Cada fila será un mensaje en la cola.
            queue_name (str): Nombre de la cola en Azure Queue Storage.
        """
        # Crear conexión con Azure Queue Storage
        queue_client = self.queue_service_client.get_queue_client(queue_name)
        
        # Verificar si la cola existe y crearla si no existe
        try:
            queue_client.create_queue()
            print(f"Cola '{queue_name}' creada exitosamente.")
        except ResourceExistsError:
            print(f"Cola '{queue_name}' ya existe.")
        
        # Enviar mensajes desde el DataFrame a la cola
        for _, row in data.iterrows():
            message = row.to_json()  # Convertir la fila a JSON
            queue_client.send_message(message)
        
        print(f"Se enviaron {len(data)} mensajes a la cola '{queue_name}'.")
        
    def get_messages_from_queue(self, queue_name: str, max_messages: int = 10) -> DataFrame:
        """
        Recupera mensajes de una cola de Azure Storage.
        
        Args:
            queue_name (str): Nombre de la cola de Azure Queue Storage.
            max_messages (int): Número máximo de mensajes a recuperar (por defecto es 10).
            
        Returns:
            list: Lista de mensajes recuperados desde la cola.
        """
        try:
            queue_client = self.queue_service_client.get_queue_client(queue_name)
            messages = queue_client.receive_messages(messages_per_page=max_messages)
            result = []
            
            for message in messages.by_page():
                for msg in message:
                    result.append(msg.content)  # Guardar el contenido del mensaje
                    queue_client.delete_message(msg)  # Eliminar mensaje después de procesarlo
                    
            return DataFrame(result)
        except Exception as e:
            print(f"Error al obtener mensajes de la cola '{queue_name}': {e}")
            return []

    def get_table_data(self, table_name: str, partition_key: str = None, row_key: str = None) -> DataFrame:
        """
        Recupera datos de una tabla de Azure Table Storage.
        
        Args:
            table_name (str): Nombre de la tabla.
            partition_key (str, optional): Clave de partición para filtrar los datos.
            row_key (str, optional): Clave de fila para filtrar los datos.
            
        Returns:
            list: Lista de entidades (diccionarios) recuperadas desde la tabla.
        """
        try:
            table_client = self.table_service_client.get_table_client(table_name)
            query = None

            if partition_key and row_key:
                query = f"PartitionKey eq '{partition_key}' and RowKey eq '{row_key}'"
            elif partition_key:
                query = f"PartitionKey eq '{partition_key}'"
                
            entities = table_client.query_entities(query) if query else table_client.list_entities()
            
            result = DataFrame([entity for entity in entities])
            return result
        except Exception as e:
            print(f"Error al obtener datos de la tabla '{table_name}': {e}")
            return []
                
    def get_all_table_data(self, table_name: str) -> DataFrame:
        """
        Recupera todos los datos de una tabla de Azure Table Storage.
        
        Args:
            table_name (str): Nombre de la tabla en Azure Table Storage.
            
        Returns:
            DataFrame: DataFrame con todas las entidades de la tabla.
        """
        try:
            table_client = self.table_service_client.get_table_client(table_name)

            # Obtener todas las entidades sin filtro
            entities = table_client.list_entities()

            # Convertir a DataFrame si hay datos
            result = DataFrame([entity for entity in entities])

            return result
        except Exception as e:
            print(f"Error al obtener datos de la tabla '{table_name}': {e}")
            return DataFrame()  # Retornar un DataFrame vacío en caso de error

    def get_data_by_partition(self, table_name: str, partition_key: str) -> DataFrame:
        """
        Recupera todos los datos de una tabla de Azure Table Storage filtrados por PartitionKey.
        
        Args:
            table_name (str): Nombre de la tabla en Azure Table Storage.
            partition_key (str): Clave de partición para filtrar los datos.
            
        Returns:
            DataFrame: DataFrame con las entidades filtradas.
        """
        try:
            table_client = self.table_service_client.get_table_client(table_name)

            # Filtro para la clave de partición
            query = f"PartitionKey eq '{partition_key}'"

            # Ejecutar la consulta
            entities = table_client.query_entities(query)

            # Convertir a DataFrame si hay datos
            result = DataFrame([entity for entity in entities])

            return result
        except Exception as e:
            print(f"Error al obtener datos con PartitionKey '{partition_key}' en la tabla '{table_name}': {e}")
            return DataFrame()  # Retornar un DataFrame vacío en caso de error

    def get_csv(self, blob_name: str) -> DataFrame:
        """
        Descarga un archivo desde Azure Blob Storage y retorna un DataFrame si el archivo es un CSV.
        
        Args:
            blob_name (str): Nombre del archivo en el contenedor de Azure Blob Storage.
            file_path (str, optional): Ruta local donde se guardará el archivo descargado.
                                    Si no se especifica, procesa el contenido directamente.
        
        Returns:
            DataFrame: Un DataFrame generado a partir del archivo CSV descargado.
        """
        try:
            # Crear cliente del blob
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Descargar el archivo
            stream = blob_client.download_blob()
            file_content = stream.readall()
            
            return read_csv(BytesIO(file_content))
        
        except Exception as e:
            print(f"Error al descargar o procesar el archivo '{blob_name}': {e}")
            return None
            
    def read_file_as_bytes(self, file_path: str) -> bytes:
        """
        Lee un archivo local como bytes.
        
        Args:
            file_path (str): Ruta del archivo que se quiere leer.
        
        Returns:
            bytes: Contenido del archivo como bytes.
        """
        try:
            with open(file_path, 'rb') as file:
                file_content = file.read()
                print(f"Archivo '{file_path}' leído exitosamente.")
                return file_content
        except Exception as e:
            print(f"Error al leer el archivo '{file_path}': {e}")
            return None
