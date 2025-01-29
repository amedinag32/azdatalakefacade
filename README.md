
---

# AZ Data Lake

## Instalación

```bash
pip3 install azdatalakefacade
```

## Manual de uso
1. **Variables de Entorno**

| Variable | Tipo de dato |
|:-|-:|
|AZ_DATA_LAKE_STRING_CONNECTION|string|
|AZ_CONTAINNER_CLI|string|

2. **Funciones**:
   - **`upload_file(blob_name, data)`**: Este método permite subir un archivo a azure data storage gen2.
   - **`upsert_data_to_table(table_name, data)`**: Este método permite subir datos a una tabla de azure data storage gen2.
   - **`send_data_to_queue(queue_name, data)`**: Este método permite subir datos a una cola de azure data storage gen2.
   - **`get_messages_from_queue(queue_name, max_messages)`**: Este método permite obtener datos de una cola de azure data storage gen2.
   - **`get_table_data(table_name, partition_key, row_key)`**: Este método permite obtener datos de una tabla de azure data storage gen2.
   - **`get_all_table_data(table_name, partition_key, row_key)`**: Este método permite obtener una tabla de azure data storage gen2.
   - **`get_data_by_partition(table_name, partition_key, row_key)`**: Este método permite obtener los datos de una particion de una tabla de azure data storage gen2.
   - **`get_csv(blob_name)`**: Este método permite obtener datos de un csv de azure data storage gen2 y retorna un dataframe.

3. **Ejemplos de Uso**
```py
from azdatalakefacade.singleton import AzDataLakeSingleton

# Crear instancia del singleton
az = AzDataLakeSingleton()

# Consultar datos de un csv
content = az.get_csv(blob_name="mis_datos.csv")
print(content)

# Subir un archivo
az.upload_file("local.csv", az.read_file_as_bytes("local.csv"))

# Consulta datos de una tabla
data = az.get_table_data("unitest3", "480217418839", 23)
print(data)

# actualizar datos de una tabla
az.upsert_data_to_table(table_name="unitest3", data=data)

# obtener tabla completa
data = az.get_all_table_data("alarmassolis")
print("tabla obtenidos: ")
print(data)


# obtener particion de tabla
data = az.get_data_by_partition("alarmassolis", "1298491919449258694_2025_01")
print("tabla obtenidos: ")
print(data)



```

---

By: Alan Medina ⚙️
