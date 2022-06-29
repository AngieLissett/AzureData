# AzureData
# Ingesta de Datos
Proceso EL para extracción de datos a partir de una conexión Mysql a archivo formato parquet

![image](https://user-images.githubusercontent.com/108035811/176551418-0f587a8d-2595-4755-a24c-c0337001ebca.png)

# Procesamiento y carga de datos
´´´py
#Extrae archivo csv datos de dirección electonica del cliente
%%pyspark
from pyspark.sql import functions as F, Window
dfEmail = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/clientes_correos.csv', format='csv')

#lee archivo formato parquet con datos de la tabla Cliente
pathCliente='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/cliente_azambrano.parquet'
dfCliente = spark.read.load(pathCliente, format='parquet')

#lee archivo formato parquet con datos de la tabla Factura
pathFactura='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/factura_azambrano.parquet'
dfFactura = spark.read.load(pathFactura, format='parquet')

#lee archivo formato parquet con datos de la tabla Producto
pathProducto='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/producto_azambrano.parquet'
dfProducto = spark.read.load(pathProducto, format='parquet')

#lee archivo formato parquet con datos de la tabla Factura Producto
pathFacturaProducto='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/facturaproducto_azambrano.parquet'
dfFacturaProducto = spark.read.load(pathFacturaProducto, format='parquet')

#Crea tabla temporales
dfEmail.createOrReplaceTempView("tbl_email_AZ")
dfCliente.createOrReplaceTempView("tbl_cliente_AZ")
dfFactura.createOrReplaceTempView("tbl_factura_AZ")
dfProducto.createOrReplaceTempView("tbl_producto_AZ")
dfFacturaProducto.createOrReplaceTempView("tbl_facturaProducto_AZ")

#display(dfFacturaProducto.limit(10))

#Consulta para obtener el producto más vendido 
dfConsulta=spark.sql("SELECT rowidproducto, count(rowidproducto) from tbl_facturaProducto_AZ group by rowidproducto order by count(rowidproducto) desc limit 1")
dfConsulta.createOrReplaceTempView("tbl_producto_top_AZ")
#dfConsulta1=spark.sql("SELECT * FROM tbl_factura_AZ")
#display(dfConsulta1)

#Consulta para obtener en base al producto más solicitado por cada cliente la fecha de la ultima compra y su dirección de correo electronico.  
vSQL="""
SELECT d.rowidcliente as codigoCliente,  e._c1 as email,  c.producto producto, max(d.fecha) as fechaUltimaCompra from tbl_producto_top_AZ a
inner join tbl_facturaProducto_AZ b on a.rowidproducto = b.rowidproducto
inner join tbl_Producto_AZ c  on b.rowidproducto = c.rowidproducto
inner join tbl_factura_AZ d on b.rowidfactura = d.rowidfactura
inner join tbl_email_AZ e on d.rowidcliente = e._c0
group by d.rowidcliente,c.producto, e._c1
"""
dfResultadoAZ=spark.sql(vSQL)
#display(dfResultadoAZ)

#Carga de datos en Pool SQL 
dfResultadoAZ.write.mode("overwrite").saveAsTable("default.tbl_azambrano")
path='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/azambrano/tbl_azambrano.parquet'
dfResultadoAZ.repartition(1).write.mode("overwrite").parquet(path)
´´´

