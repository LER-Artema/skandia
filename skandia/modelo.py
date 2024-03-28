import pyarrow.parquet as pq

clientes = pq.read_table('0clientes.parquet').to_pandas()
saldos = pq.read_table('0saldos.parquet').to_pandas()
transferencias = pq.read_table('0transferencias.parquet').to_pandas()
# clientes
# transferencias
saldos = saldos.loc[saldos['TipoDocum'] == 'C ']
clientes = clientes.loc[clientes['TIPODOCUM'] == 'C']

conteo_contratos_por_cliente = saldos.groupby('NroDocum').agg(
    Numero_de_Contratos_Activos=('Contrato', 'count'),  # Conteo de contratos por cliente
    Contrato=('Contrato', list),
    NumeroProductos=('PlanProducto', 'count'),  # Conteo de contratos por cliente
    Productos=('PlanProducto', list)
)


# Renombra la columna de conteo para mayor claridad

print(conteo_contratos_por_cliente.to_string())

# conteo_contratos_por_cliente.merge(conteo_productos_por_contrato, how="left", left_on=['Contrato'], right_on=['Contrato'])

# # Muestra el nuevo DataFrame con la información de cuántos contratos tiene cada cliente
# clientes.nunique()
# # clientes
# saldos.nunique()
# missings = set(saldos.NroDocum.drop_duplicates().values) - set(clientes.NroDocum.values)
# print(missings)
# clientes_saldos_transf = clientes.copy()