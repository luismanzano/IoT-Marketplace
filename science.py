import streamlit as st
import connection as con
import psycopg2
import pandas as pd
import plotly.express as px

# st.write("""
# #HELLO WORLD
# """)
add_selectbox = st.sidebar.selectbox(
    "Selecciona Una Tienda",
    (1,2)
)
st.write(add_selectbox)
def query_categorias(tienda):
    st.write(tienda)
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        if tienda == 1:
            query = """select count(factura_producto.producto_id) as cantidad, categoria.nombre from factura_producto
            inner join producto_categoria on  factura_producto.producto_id = producto_categoria.producto_id
            inner join categoria on  categoria.categoria_id = producto_categoria.categoria_id
            inner join factura on factura.factura_id = factura_producto.factura_id
            where factura.sucursal_id = 1
            group by nombre
            order by cantidad desc
            limit 5"""
        if tienda == 2:
            query = """select count(factura_producto.producto_id) as cantidad, categoria.nombre from factura_producto
            inner join producto_categoria on  factura_producto.producto_id = producto_categoria.producto_id
            inner join categoria on  categoria.categoria_id = producto_categoria.categoria_id
            inner join factura on factura.factura_id = factura_producto.factura_id
            where factura.sucursal_id = 2
            group by nombre
            order by cantidad desc
            limit 5"""
        #cursor.execute(query, tienda)
        #response = cursor.fetchall()
        df = pd.read_sql(query, connection)
        return df

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

def query_mapa_de_calor(tienda):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        if tienda == 1:
            query = """
            select fecha, factura_id from factura
            where sucursal_id = 1
            order by fecha  
            """
        if tienda == 2:
            query = """
            select fecha, factura_id from factura
            where sucursal_id = 2
            order by fecha  
            """
        #cursor.execute(query)
        #response = cursor.fetchall()
        df = pd.read_sql(query, connection)
        return df

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

tabla_query_categorias = query_categorias(add_selectbox)
st.markdown("# **CATEGORIAS DE PRODUCTOS MAS VENDIDAS** ")
st.write("El top 5 de nuestras cateorias son las siguientes")
st.write(tabla_query_categorias)



##SEGUNDO APARTADO DE PLOTLY
def afiliadosBancos(tienda):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        if tienda == 1: 
            query = """
            select banco.nombre, count(nombre) from banco 
            inner join factura on factura.met_pago = banco.banco_id
            inner join afiliado on factura.cedula_cliente = afiliado.cliente_cedula
            where factura.sucursal_id = 1
            group by banco.nombre
            """
        if tienda == 2:
            query = """
            select banco.nombre, count(nombre) from banco 
            inner join factura on factura.met_pago = banco.banco_id
            inner join afiliado on factura.cedula_cliente = afiliado.cliente_cedula
            where factura.sucursal_id = 2
            group by banco.nombre
            """
            
        cursor.execute(query)
        #response = cursor.fetchall()
        df = pd.read_sql(query, connection)
        return df

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

afiliados_df = afiliadosBancos(add_selectbox)
st.markdown("# **BANCOS PREFERIDOS DE NUESTROS AFILIADOS** ")
figAfi = px.histogram(afiliados_df, x="nombre", y="count")
st.write(figAfi)
if add_selectbox == 1:
    st.markdown("Como se puede notar, el banco preferido de nuestros aliados es BANESCO")
elif add_selectbox == 2:
    st.markdown("Como se puede notar, nuestros clientes prefieren pagar con Banco Provincial")


def afiliadosCategorias(tienda):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        if tienda == 1:
            query = """
            select count(factura_producto.producto_id) as cantidad, categoria.nombre from factura_producto
            inner join producto_categoria on  factura_producto.producto_id = producto_categoria.producto_id
            inner join categoria on  categoria.categoria_id = producto_categoria.categoria_id
            inner join factura on factura.factura_id = factura_producto.factura_id
            inner join afiliado on afiliado.cliente_cedula = factura.cedula_cliente
            where factura.sucursal_id = 1
            group by nombre
            """

        if tienda == 2:
            query = """
            select count(factura_producto.producto_id) as cantidad, categoria.nombre from factura_producto
            inner join producto_categoria on  factura_producto.producto_id = producto_categoria.producto_id
            inner join categoria on  categoria.categoria_id = producto_categoria.categoria_id
            inner join factura on factura.factura_id = factura_producto.factura_id
            inner join afiliado on afiliado.cliente_cedula = factura.cedula_cliente
            where factura.sucursal_id = 2
            group by nombre
            """
        cursor.execute(query)
        #response = cursor.fetchall()
        df = pd.read_sql(query, connection)
        return df

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

afiliados_categorias = afiliadosCategorias(add_selectbox)
st.markdown("# **CATEGORIAS PREFERIDAS DE NUESTROS AFILIADOS** ")
figAfi = px.histogram(afiliados_categorias, x="nombre", y="cantidad")
st.write(figAfi)
if add_selectbox == 1:
    st.markdown("Como se puede notar, la categoria preferida de nuestros aliados es 'PINK'  ")
elif add_selectbox == 2:
    st.markdown("Como se puede notar, las categorias 'Red' 'Pink' y 'Purple' son igual de demandadas ")

def afiliadosSucursal():
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """
        select sucursal_id, count(sucursal_id) from factura
        inner join afiliado on factura.cedula_cliente = afiliado.cliente_cedula
        group by sucursal_id
        """
        cursor.execute(query)
        #response = cursor.fetchall()
        df = pd.read_sql(query, connection)
        return df

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

afiliados_Sucursal = afiliadosSucursal()
st.markdown("# **SUCURSAL FAVORIDA DE NUESTROS AFILIADOS** ")
figAfi = px.histogram(afiliados_Sucursal, x="sucursal_id", y="count")
st.write(figAfi)
st.markdown("Como se puede notar, la categoria preferida de nuestros aliados es 'La sucursal 1 que es nuestra principal'  ")
#TERCER APARTADO PLOTLY


mapa_de_calor = query_mapa_de_calor(add_selectbox)
mapa_de_calor['weekday'] = mapa_de_calor['fecha'].dt.dayofweek
mapa_de_calor['hora'] = mapa_de_calor['fecha'].dt.hour

testing = mapa_de_calor.groupby(['weekday','hora']).size().reset_index(name="sales")
#testing = testing.set_index('hora')
testing = testing.pivot(index='hora', columns='weekday', values='sales')


st.markdown("# **MAPA DE CALOR DIAS QUE PREFIEREN COMPRAR LOS USUARIOS** ")

days = []

for day in testing.columns.tolist():
    days.append(str(day))

data=[[1, 25, 30, 50, 1], [20, 1, 60, 80, 30], [30, 60, 1, 5, 20]]
fig = px.imshow(testing,
                labels=dict(x="Dia de la semana", y="Hora del Dia", color="Ventas")   ,
                x=days
               )
fig.update_xaxes(side="top")
st.write(fig)

st.write(testing)

if add_selectbox == 1:
    st.write("Nuestra tienda es bastante nueva, los clientes han comprado solo el dia martes y su hora favorita es al mediodia, quizas porque al ser una tienda de conveniencia, la visitan en el lunch break del trabajo.")

elif add_selectbox == 2:
    st.write("Nuestra tienda es bastante nueva, los clientes han comprado solo el dia Viernes y solo han comprado a las 18:00. La unica hora con ventas y a su vez, la que tiene mas ventas tambien.")