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
	select avg(extract(minute from ec.hora_entrada)
			   - extract(minute from sc.hora_salida)) 
			   from entrada_cliente ec
	inner join salida_cliente sc on ec.cliente_id = sc.cliente_id
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
	
create or replace view cat_menosvendida as
	select c.nombre as categoria , sum(fp.cantidad), s.nombre from factura_producto fp
		inner join producto p on fp.producto_id = p.producto_id
		inner join producto_categoria pc on pc.producto_id = p.producto_id
		inner join categoria c on c.categoria_id = pc.categoria_id 
		inner join factura f on fp.factura_id = f.factura_id
		inner join sucursal s on f.sucursal_id = s.sucursal_id
	group by categoria, s.nombre
	order by sum(fp.cantidad) asc
	limit 3;


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
	order by sum(p.producto_id) desc
	limit 5;

create or replace view clientes_solosede1 as
	select cl.cliente_id, cl.cedula, f.sucursal_id as nro_sucursal from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where not exists (select 1 from factura f 
						  	where sucursal_id = 2 
						 	and fecha between (now() - interval'7 days') and 
						 					(now() + interval'7 days'))
		and fecha between (now() - interval'7 days') and 
						 					(now() + interval'7 days')
	group by cl.cliente_id,cl.cedula, f.sucursal_id;
	
create or replace view clientes_solosede2 as
	select cl.cedula, f.sucursal_id as nro_sucursal from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where not exists (select 1 from factura f 
						  	where sucursal_id = 1 
						 	and fecha between (now() - interval'7 days') and 
						 					(now() + interval'7 days'))
		and fecha between (now() - interval'7 days') and 
						 					(now() + interval'7 days')
	group by cl.cedula, f.sucursal_id;
	
create or replace view clientes_ambas_sedes as
	select cl.cedula from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		where f.sucursal_id = 1 or f.sucursal_id = 2
		and fecha between (now() - interval'7 days') and 
						 					(now() + interval'7 days')
											

	
create or replace view pago_bancos_distintos_afiliados as
	select cl.cedula, sum(f.costo) from factura f
		inner join cliente cl on f.cedula_cliente = cl.cedula
		inner join carrito ca on cl.cliente_id = ca.cliente_id
		where exists (select 1 from afiliado where afiliado_id = cl.cliente_id)
	group by cl.cedula
	having count(distinct f.met_pago) > 1;


		