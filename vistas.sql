create or replace view tapabocas_diario as
	select SUM (CASE
               		WHEN tapabocas = true THEN 1
	       			ELSE 0
				END
				) AS "Con tapabocas",
			SUM (CASE
               		WHEN tapabocas = false THEN 1
	       			ELSE 0
				END
				) AS "Sin tapabocas",
			extract(day from hora_entrada) as dia,
			extract(month from hora_entrada) as mes,
			extract(year from hora_entrada) as ano
	from entrada_cliente
	group by dia,mes,ano
	order by ano,mes,dia asc;
	
	
create or replace view duracion_avg as
	select extract(minute from (avg(sc.hora_salida- ec.hora_entrada))),
			   	extract(day from ec.hora_entrada) as dia,
				extract(month from ec.hora_entrada) as mes,
				extract(year from ec.hora_entrada) as ano
			   from entrada_cliente ec
	inner join salida_cliente sc on ec.cliente_cedula = sc.cliente_cedula
	group by extract(day from ec.hora_entrada),
			extract(month from ec.hora_entrada),
			extract(year from ec.hora_entrada)
				

create or replace view temp_avg_diario as
	select avg(temperatura),
			extract(day from hora_entrada) as dia,
			extract(month from hora_entrada) as mes,
			extract(year from hora_entrada) as ano
	from entrada_cliente
	group by dia,mes,ano
	order by ano,mes,dia asc;
	
create or replace view cat_menosvendidas as
	select c.nombre as categoria , count(fp.cantidad) prod_vendidos, s.nombre from factura_producto fp
		inner join producto p on fp.producto_id = p.producto_id
		inner join producto_categoria pc on pc.producto_id = p.producto_id
		inner join categoria c on c.categoria_id = pc.categoria_id 
		inner join factura f on fp.factura_id = f.factura_id
		inner join sucursal s on f.sucursal_id = s.sucursal_id
	group by categoria, s.nombre
	order by sum(fp.cantidad) asc;
	
create or replace view pivottable as
	SELECT * FROM 
	crosstab('
		select categoria,nombre,nro_prod_vendidos from cat_masvendidas
			 order by 1,2') 
	AS cat_masvendidas("Categoria" varchar, "Lagos da Beira" bigint, "Liwu" bigint);
	
create or replace view cat_masvendidas as
	select s.nombre, c.nombre categoria, count(fp.producto_id) nro_prod_vendidos from factura f
	inner join sucursal s on f.sucursal_id = s.sucursal_id
	inner join factura_producto fp on f.factura_id = fp.factura_id
	inner join producto p on fp.producto_id = p.producto_id
	inner join producto_categoria pc on p.producto_id = pc.producto_id
	inner join categoria c on pc.categoria_id = c.categoria_id
	group by c.nombre,s.nombre
	order by count(fp.producto_id) desc


create or replace view personas_rechazadas as
	select SUM (CASE
               		WHEN tapabocas = false 
						OR temperatura >= 38 THEN 1
	       			ELSE 0
				END
				) AS "Personas rechazadas"
	from entrada_cliente

create or replace view prod_masvendidos as
	select sum(p.producto_id), p.nombre as prod_nombre, s.nombre from factura_producto fp
		inner join producto p on fp.producto_id = p.producto_id
		inner join factura f on fp.factura_id = f.factura_id
		inner join sucursal s on f.sucursal_id = s.sucursal_id
	group by prod_nombre, s.nombre
	order by sum(p.producto_id) desc;

create or replace view clientes_solosede1 as
	select count(cl.cedula), f.sucursal_id from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where not exists (select 1 from factura f 
						  	where sucursal_id = 2 
						 	and cedula_cliente = cl.cedula)
		and fecha between (now() - interval'7 days') and now()
		group by f.sucursal_id
											

	
create or replace view clientes_solosede2 as
	select count(cl.cedula), f.sucursal_id from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where not exists (select 1 from factura f 
						  	where sucursal_id = 1 
						 	and cedula_cliente = cl.cedula)
		and fecha between (now() - interval'7 days') and now()
		group by f.sucursal_id
	
create or replace view clientes_ambas_sedes as
	select count(cl.cedula) from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where f.sucursal_id = 1 and f.sucursal_id = 2
		and fecha between (now() - interval'7 days') and now()
											

	
create or replace view pago_bancos_distintos_afiliados as
	select cl.cedula, sum(f.costo) from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		inner join carrito ca on cl.cliente_id = ca.cliente_id
		where exists (select 1 from afiliado where afiliado_id = cl.cliente_id)
	group by cl.cedula
	having count(distinct f.met_pago) > 1;


		